"""
Comparative benchmark harness (paper Section 4.1/4.2): sweeps the same
(N, F, M) matrix across Airflow, Argo Workflows, and Kubeflow Pipelines,
using each system's adapter, and writes raw per-run records in the common
schema analysis/analyze_comparative.py consumes.

Architectural note surfaced by actually wiring this up (worth citing in
paper Section 2.4): Airflow requires a separate DAG-deployment step (files
must land in the scheduler's dag_folder before they can run) -- Argo and
Kubeflow do not, since both submit directly via their REST/SDK APIs with no
shared-filesystem step in between. This asymmetry shows up directly below:
only the Airflow arm has a deploy_dags()-style hook.

Usage:
    python run_comparative_matrix.py --config ../config/comparative_matrix.yaml --dry-run
"""
from __future__ import annotations
import argparse
import itertools
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from argo_adapter.argo_client import ArgoClient                    # noqa: E402
from kubeflow_adapter.kfp_client import KubeflowClient              # noqa: E402
from airflow_adapter.airflow_client import AirflowClient            # noqa: E402
from common.metrics_schema import (                                 # noqa: E402
    from_airflow_dag_run, from_argo_workflow_status, from_kubeflow_run,
)


@dataclass
class RunConfig:
    system: str          # "airflow" | "argo" | "kubeflow"
    n: int
    fan_out: int
    short_ratio: float
    repeat_index: int
    is_warmup: bool

    @property
    def config_id(self) -> str:
        return (f"{self.system}_N{self.n}_F{self.fan_out}_M{self.short_ratio}_"
                f"r{self.repeat_index}{'_warmup' if self.is_warmup else ''}")


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def iter_configs(cfg: dict, repeats: int, warmup_runs: int):
    sweep = cfg["sweep"]
    for system in cfg["systems"]:
        for n, f, m in itertools.product(sweep["dag_count"], sweep["fan_out"], sweep["short_ratio"]):
            if system == "kubeflow" and f < 1:
                continue  # unsupported -- see kubeflow_adapter/generate_pipeline.py
            for i in range(warmup_runs):
                yield RunConfig(system, n, f, m, repeat_index=i, is_warmup=True)
            for i in range(repeats):
                yield RunConfig(system, n, f, m, repeat_index=i, is_warmup=False)


# ----------------------------------------------------------------------
# Per-system generation, dispatched to each adapter's own generator.
# ----------------------------------------------------------------------

def generate_for_config(cfg: dict, rc: RunConfig) -> str:
    out_dir = os.path.join(cfg["output"]["generated_dir"], rc.config_id)
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    td = cfg["task_duration"]

    if rc.system == "airflow":
        generator = os.path.join(os.path.dirname(__file__), "..", "airflow_adapter", "generate_dags.py")
        cmd = [sys.executable, generator, "--n", str(rc.n), "--f", str(rc.fan_out),
               "--m", str(rc.short_ratio), "--short-dur", str(td["short_duration_sec"]),
               "--long-dur", str(td["long_duration_sec"]), "--out", out_dir,
               "--prefix", "bench", "--shard-id", "0", "--shard-count", "1"]
        subprocess.run(cmd, check=True)

    elif rc.system == "argo":
        generator = os.path.join(os.path.dirname(__file__), "..", "argo_adapter", "generate_workflows.py")
        cmd = [sys.executable, generator, "--n", str(rc.n), "--f", str(rc.fan_out),
               "--m", str(rc.short_ratio), "--short-dur", str(td["short_duration_sec"]),
               "--long-dur", str(td["long_duration_sec"]), "--out", out_dir, "--prefix", "bench"]
        subprocess.run(cmd, check=True)

    elif rc.system == "kubeflow":
        generator = os.path.join(os.path.dirname(__file__), "..", "kubeflow_adapter", "generate_pipeline.py")
        ir_out = out_dir + "_ir"
        cmd = [sys.executable, generator, "--n", str(rc.n), "--f", str(rc.fan_out),
               "--m", str(rc.short_ratio), "--short-dur", str(td["short_duration_sec"]),
               "--long-dur", str(td["long_duration_sec"]), "--out", out_dir,
               "--prefix", "bench", "--compile", "--ir-out", ir_out]
        subprocess.run(cmd, check=True)
        return ir_out  # Kubeflow submits the compiled IR, not the source

    else:
        raise ValueError(f"Unknown system: {rc.system}")

    return out_dir


# ----------------------------------------------------------------------
# Per-system deploy/submit/collect -- this is where the architectural
# asymmetry noted in the module docstring becomes concrete.
# ----------------------------------------------------------------------

def deploy_dags_airflow(out_dir: str, cfg: dict) -> None:
    """
    Copies generated DAG files into Airflow's dag_folder.

    Still environment-specific in ONE respect: the destination path.
    cfg['airflow']['dag_folder_path'] must point at a location the
    scheduler actually watches (a local path if the scheduler runs on this
    same host / a mounted shared volume; for a remote cluster without a
    shared filesystem, replace the shutil.copytree call below with
    `kubectl cp`, an rsync to a shared PVC mount, or a git-sync push,
    whichever matches your deployment -- see README).
    """
    dag_folder = cfg["airflow"].get("dag_folder_path")
    if not dag_folder:
        raise NotImplementedError(
            "Set airflow.dag_folder_path in the config to a path the "
            "scheduler's dag_folder watches (local path or shared-volume "
            "mount). If your cluster has no shared filesystem, replace "
            "this function's shutil.copytree call with your deployment "
            "mechanism (kubectl cp / rsync / git-sync)."
        )
    dest = os.path.join(dag_folder, os.path.basename(out_dir))
    if os.path.exists(dest):
        shutil.rmtree(dest)
    shutil.copytree(out_dir, dest)


def clear_dags_airflow(out_dir: str, cfg: dict) -> None:
    dag_folder = cfg["airflow"].get("dag_folder_path")
    if not dag_folder:
        return
    dest = os.path.join(dag_folder, os.path.basename(out_dir))
    if os.path.exists(dest):
        shutil.rmtree(dest)


def submit_and_collect_airflow(out_dir: str, cfg: dict, rc: RunConfig) -> list[dict]:
    deploy_dags_airflow(out_dir, cfg)

    client = AirflowClient(
        base_url=cfg["airflow"]["base_url"],
        auth=(cfg["airflow"]["auth_username"], cfg["airflow"]["auth_password"]),
    )

    dag_ids = [os.path.splitext(f)[0] for f in sorted(os.listdir(out_dir)) if f.endswith(".py")]

    # Airflow needs a moment to parse newly-deployed DAG files before they're
    # triggerable; poll list_dags rather than sleeping a fixed guess.
    deadline = time.monotonic() + cfg["run"]["run_timeout_sec"]
    pending = set(dag_ids)
    while pending and time.monotonic() < deadline:
        known = {d["dag_id"] for d in client.list_dags(limit=len(dag_ids) + 10).get("dags", [])}
        pending -= known
        if pending:
            time.sleep(cfg["run"]["poll_interval_sec"])
    if pending:
        raise TimeoutError(f"Airflow never parsed {len(pending)} DAG(s): {sorted(pending)[:5]}...")

    records = []
    for dag_id in dag_ids:
        client.unpause_dag(dag_id)
        run_id = f"bench_{uuid.uuid4().hex[:8]}"
        client.trigger_dag(dag_id, run_id=run_id)
        final_run = client.wait_for_dag_run(
            dag_id, run_id,
            poll_interval_sec=cfg["run"]["poll_interval_sec"],
            timeout_sec=cfg["run"]["run_timeout_sec"],
        )
        task_instances = client.list_task_instances(dag_id, run_id).get("task_instances", [])
        timing = from_airflow_dag_run(final_run, task_instances)
        records.append({
            "submission_to_running_ms": timing.submission_to_running_ms,
            "running_to_completion_ms": timing.running_to_completion_ms,
            "final_state": timing.final_state,
        })

    clear_dags_airflow(out_dir, cfg)
    return records


def submit_and_collect_argo(out_dir: str, cfg: dict, rc: RunConfig) -> list[dict]:
    """Unlike Airflow, no deploy step: Argo manifests are submitted directly
    via the Argo Server REST API, no shared dag_folder involved."""
    client = ArgoClient(
        base_url=cfg["argo"]["base_url"], namespace=cfg["argo"]["namespace"],
        bearer_token=cfg["argo"]["bearer_token"],
    )
    records = []
    for fname in sorted(os.listdir(out_dir)):
        if not fname.endswith(".yaml"):
            continue
        with open(os.path.join(out_dir, fname)) as f:
            manifest = yaml.safe_load(f)
        submitted = client.submit_workflow(manifest)
        name = submitted["metadata"]["name"]
        final = client.wait_for_completion(
            name, poll_interval_sec=cfg["run"]["poll_interval_sec"],
            timeout_sec=cfg["run"]["run_timeout_sec"],
        )
        timing = from_argo_workflow_status(final)
        records.append({
            "submission_to_running_ms": timing.submission_to_running_ms,
            "running_to_completion_ms": timing.running_to_completion_ms,
            "final_state": timing.final_state,
        })
    return records


def submit_and_collect_kubeflow(ir_dir: str, cfg: dict, rc: RunConfig) -> list[dict]:
    """Also no deploy step: KFP submits the compiled IR file directly via
    the kfp.Client SDK."""
    client = KubeflowClient(
        host=cfg["kubeflow"]["host"], namespace=cfg["kubeflow"]["namespace"],
        existing_token=cfg["kubeflow"]["existing_token"],
    )
    records = []
    for fname in sorted(os.listdir(ir_dir)):
        if not fname.endswith(".yaml"):
            continue
        run_name = os.path.splitext(fname)[0]
        run_id = client.submit_run(os.path.join(ir_dir, fname), run_name=run_name)
        final = client.wait_for_completion(
            run_id, timeout_sec=cfg["run"]["run_timeout_sec"],
            poll_interval_sec=int(cfg["run"]["poll_interval_sec"]),
        )
        timing = from_kubeflow_run(final.to_dict() if hasattr(final, "to_dict") else final)
        records.append({
            "submission_to_running_ms": timing.submission_to_running_ms,
            "running_to_completion_ms": timing.running_to_completion_ms,
            "final_state": timing.final_state,
        })
    return records


def run_one_config(cfg: dict, rc: RunConfig, dry_run: bool) -> list[dict] | None:
    print(f"=== {rc.config_id} ===")
    out_dir = generate_for_config(cfg, rc)

    if dry_run:
        ext = ".yaml" if rc.system in ("argo", "kubeflow") else ".py"
        n_files = len([f for f in os.listdir(out_dir) if f.endswith(ext)])
        print(f"[dry-run] generated {n_files} file(s) in {out_dir}; skipping submit/collect.")
        return None

    if rc.system == "airflow":
        results = submit_and_collect_airflow(out_dir, cfg, rc)
    elif rc.system == "argo":
        results = submit_and_collect_argo(out_dir, cfg, rc)
    elif rc.system == "kubeflow":
        results = submit_and_collect_kubeflow(out_dir, cfg, rc)
    else:
        raise ValueError(f"Unknown system: {rc.system}")

    return results


def write_records(cfg: dict, rc: RunConfig, results: list[dict]) -> None:
    raw_dir = cfg["output"]["raw_metrics_dir"]
    os.makedirs(raw_dir, exist_ok=True)
    for i, metrics in enumerate(results):
        record = {
            "config": asdict(rc),
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics,
        }
        fname = f"{rc.config_id}_{i}_{uuid.uuid4().hex[:8]}.json"
        with open(os.path.join(raw_dir, fname), "w") as f:
            json.dump(record, f, indent=2)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    cfg = load_config(args.config)
    repeats = cfg["run"]["repeats"]
    warmup_runs = cfg["run"]["warmup_runs"]

    n_configs = 0
    for rc in iter_configs(cfg, repeats=repeats, warmup_runs=warmup_runs):
        n_configs += 1
        results = run_one_config(cfg, rc, dry_run=args.dry_run)
        if results is not None and not rc.is_warmup:
            write_records(cfg, rc, results)

    print(f"Swept {n_configs} run(s) total across systems {cfg['systems']}.")


if __name__ == "__main__":
    main()
