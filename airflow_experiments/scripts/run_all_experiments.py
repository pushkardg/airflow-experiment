"""
Run All Experiments
===================
Orchestrates all 30-trial experimental runs via the Airflow REST API.

Usage:
    python scripts/run_all_experiments.py [--section {all,4,5,6}] [--dry-run]

Requires Airflow webserver running on localhost:8080.
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.experiment_config import N_TRIALS, CONCURRENT_DAGS, RESULTS_DIR

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_AUTH = (
    os.environ.get("AIRFLOW_USER", "admin"),
    os.environ.get("AIRFLOW_PASS", "admin"),
)
POLL_INTERVAL = 30   # seconds between status checks
RUN_TIMEOUT = 7200   # 2 hours per trial max


def trigger_dag(dag_id: str, run_id: str, conf: dict = None) -> bool:
    """Trigger a DAG run via REST API."""
    url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
    payload = {
        "dag_run_id": run_id,
        "conf": conf or {},
    }
    r = requests.post(url, json=payload, auth=AIRFLOW_AUTH, timeout=10)
    if r.status_code in (200, 409):
        return True
    print(f"  ERROR triggering {dag_id}/{run_id}: {r.status_code} {r.text[:200]}")
    return False


def wait_for_run(dag_id: str, run_id: str, timeout: int = RUN_TIMEOUT) -> str:
    """Poll until DAG run completes. Returns final state."""
    url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL)
        try:
            r = requests.get(url, auth=AIRFLOW_AUTH, timeout=10)
            if r.status_code == 200:
                state = r.json().get("state")
                if state in ("success", "failed"):
                    return state
                print(f"    {dag_id}/{run_id}: {state}")
        except requests.RequestException as e:
            print(f"    Poll error: {e}")

    return "timeout"


def run_experiment(dag_id: str, n_trials: int, section: str, dry_run: bool = False) -> list:
    """Run n_trials of a DAG and collect results."""
    results = []
    print(f"\n{'='*60}")
    print(f"Section {section}: {dag_id} ({n_trials} trials)")
    print(f"{'='*60}")

    for trial in range(1, n_trials + 1):
        run_id = f"exp_trial_{trial:03d}_{int(time.time())}"
        print(f"  Trial {trial:3d}/{n_trials}: {run_id}", end="")

        if dry_run:
            print(" [DRY RUN - skipped]")
            continue

        start = datetime.utcnow()
        ok = trigger_dag(dag_id, run_id)
        if not ok:
            print(" TRIGGER FAILED")
            results.append({"trial": trial, "run_id": run_id, "state": "trigger_failed"})
            continue

        state = wait_for_run(dag_id, run_id)
        elapsed = (datetime.utcnow() - start).total_seconds() / 60.0
        print(f" -> {state} ({elapsed:.1f}min)")
        results.append({
            "trial": trial,
            "run_id": run_id,
            "dag_id": dag_id,
            "state": state,
            "elapsed_min": elapsed,
            "section": section,
        })

    return results


def run_concurrent(dag_id: str, n_dags: int, n_trials: int, dry_run: bool = False) -> list:
    """
    Trigger n_dags simultaneously for n_trials concurrent experiment rounds.
    This tests SCI (scheduler contention) under simultaneous failures.
    """
    all_results = []
    print(f"\n{'='*60}")
    print(f"Concurrent experiment: {dag_id} x{n_dags} ({n_trials} rounds)")
    print(f"{'='*60}")

    for trial in range(1, n_trials + 1):
        print(f"  Round {trial:3d}/{n_trials}: triggering {n_dags} concurrent DAGs")
        run_ids = []

        if not dry_run:
            # Trigger all n_dags simultaneously
            for i in range(n_dags):
                run_id = f"conc_t{trial:03d}_d{i:02d}_{int(time.time())}"
                trigger_dag(dag_id, run_id)
                run_ids.append(run_id)

            # Wait for all to complete
            states = []
            for run_id in run_ids:
                state = wait_for_run(dag_id, run_id)
                states.append(state)

            success_rate = states.count("success") / len(states)
            print(f"    Round {trial}: {states.count('success')}/{n_dags} succeeded")
            all_results.append({
                "trial": trial,
                "n_dags": n_dags,
                "success_rate": success_rate,
                "run_ids": ",".join(run_ids),
            })
        else:
            print("    [DRY RUN]")

    return all_results


def save_results(results: list, filename: str):
    """Write results list to CSV."""
    if not results:
        return
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    print(f"  Saved: {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--section", default="all",
                        choices=["all", "4", "4.2", "4.3", "4.4", "5", "6"])
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would run without triggering DAGs")
    parser.add_argument("--trials", type=int, default=N_TRIALS,
                        help=f"Number of trials per condition (default: {N_TRIALS})")
    args = parser.parse_args()

    n = args.trials
    dry = args.dry_run

    if dry:
        print("DRY RUN MODE — no DAGs will be triggered\n")

    # ── Section 4: Retry Policy Comparison ─────────────────────────────────
    if args.section in ("all", "4", "4.2"):
        # Single DAG
        r = run_experiment("exp_fixed_interval_single", n, "4.2-single", dry)
        save_results(r, "retry_fixed_single.csv")

        r = run_experiment("exp_backoff_nojitter_single", n, "4.2-nojitter", dry)
        save_results(r, "retry_nojitter_single.csv")

        r = run_experiment("exp_jitter_single", n, "4.3-jitter-single", dry)
        save_results(r, "retry_jitter_single.csv")

        # Concurrent (20 DAGs simultaneously)
        r = run_concurrent("exp_fixed_interval_concurrent", CONCURRENT_DAGS, n, dry)
        save_results(r, "retry_fixed_concurrent.csv")

        r = run_concurrent("exp_jitter_concurrent", CONCURRENT_DAGS, n, dry)
        save_results(r, "retry_jitter_concurrent.csv")

    if args.section in ("all", "4", "4.4"):
        r = run_experiment("exp_exception_aware", n, "4.4", dry)
        save_results(r, "retry_exception_aware.csv")

    # ── Section 5: SLA Detection ───────────────────────────────────────────
    if args.section in ("all", "5"):
        r = run_experiment("exp_sla_detection", n, "5.1", dry)
        save_results(r, "sla_detection_runs.csv")

        for mult in ["1_0", "1_2", "1_5"]:
            r = run_experiment(f"exp_sla_fp_{mult}x", 500, "5.1-fp", dry)
            save_results(r, f"sla_fp_{mult}x.csv")

    # ── Section 6: Alerting MTTA ───────────────────────────────────────────
    if args.section in ("all", "6"):
        for channel in ["email", "slack", "pagerduty"]:
            r = run_experiment(f"exp_alerting_{channel}", n, "6.x", dry)
            save_results(r, f"alerting_{channel}.csv")

    print("\nAll experiments complete.")
    print(f"Results in: {RESULTS_DIR}")
    print("Run: python scripts/analyze_results.py")


if __name__ == "__main__":
    main()
