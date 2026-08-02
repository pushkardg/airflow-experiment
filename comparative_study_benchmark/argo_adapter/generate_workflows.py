"""
Argo Workflows generator for the comparative-study benchmark (paper Section 4.1,
"Argo adapter").

Generates N Argo Workflow YAML manifests, each expressing F sibling steps via
withItems, with a short/long duration mixture M -- the same (N, F, M) contract
as the Airflow adapter (dag_generator/generate_dags.py in the companion
Airflow-only harness), so the two systems execute structurally equivalent
workloads (paper Section 4.1).

Usage:
    python generate_workflows.py --n 100 --f 50 --m 0.9 --out ./generated_workflows

Each generated file is one Workflow manifest, named {prefix}_{index:06d}.yaml.
Unlike the Airflow adapter, there is no shard_id/shard_count concept here --
Argo has no analogous centralized-scheduler-partitioning question (paper
Section 2.2/2.4), so every generated workflow is deployed identically
regardless of how many Argo controllers are running.
"""
import argparse
import hashlib
import os
import random

import yaml


def stable_workflow_name(prefix: str, index: int) -> str:
    return f"{prefix}-{index:06d}"


def duration_for_item(workflow_name: str, item: int, short_ratio: float) -> str:
    """Deterministic short/long assignment, mirroring the Airflow adapter's
    _duration_for_index() so both systems see the same duration mixture for
    the "same" logical task (paper Section 4.1's equivalence requirement)."""
    rnd = random.Random(f"{workflow_name}-{item}")
    return "short" if rnd.random() < short_ratio else "long"


def build_workflow_manifest(name: str, fan_out: int, short_ratio: float,
                             short_dur: float, long_dur: float) -> dict:
    items = list(range(fan_out))
    # Encode each item's target sleep duration directly in the withItems list
    # so the container command can read it without a separate lookup step --
    # keeps the generated manifest self-contained, matching the Airflow
    # adapter's DAGs (which also embed duration logic inline).
    item_durations = [
        short_dur if duration_for_item(name, i, short_ratio) == "short" else long_dur
        for i in items
    ]
    with_items = [{"index": i, "duration": d} for i, d in zip(items, item_durations)]

    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {
            "generateName": f"{name}-",
            "labels": {
                "benchmark": "true",
                "worksatsc-study": "comparative-orchestrator-study",
            },
        },
        "spec": {
            "entrypoint": "fanout",
            "templates": [
                {
                    "name": "fanout",
                    "steps": [
                        [
                            {
                                "name": "sibling-task",
                                "template": "sibling",
                                "withItems": with_items,
                                "arguments": {
                                    "parameters": [
                                        {"name": "index", "value": "{{item.index}}"},
                                        {"name": "duration", "value": "{{item.duration}}"},
                                    ]
                                },
                            }
                        ]
                    ],
                },
                {
                    "name": "sibling",
                    "inputs": {
                        "parameters": [
                            {"name": "index"},
                            {"name": "duration"},
                        ]
                    },
                    "container": {
                        "image": "python:3.11-slim",
                        "command": ["python", "-c"],
                        "args": [
                            "import time,sys; "
                            "d=float(sys.argv[1]); "
                            "t0=time.monotonic(); "
                            "time.sleep(d); "
                            "print('BENCH_TASK_DONE index=%s target_duration=%.3f elapsed=%.3f' "
                            "% (sys.argv[2], d, time.monotonic()-t0))",
                            "{{inputs.parameters.duration}}",
                            "{{inputs.parameters.index}}",
                        ],
                    },
                },
            ],
        },
    }


def generate(n, fan_out, short_ratio, short_dur, long_dur, out_dir, prefix):
    os.makedirs(out_dir, exist_ok=True)
    written = 0
    for index in range(n):
        name = stable_workflow_name(prefix, index)
        manifest = build_workflow_manifest(name, fan_out, short_ratio, short_dur, long_dur)
        path = os.path.join(out_dir, f"{name}.yaml")
        with open(path, "w") as f:
            yaml.safe_dump(manifest, f, sort_keys=False, default_flow_style=False)
        written += 1
    return written


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, required=True, help="Workflow count (N)")
    ap.add_argument("--f", type=int, required=True, dest="fan_out",
                     help="Fan-out per workflow (F): number of withItems steps")
    ap.add_argument("--m", type=float, required=True, dest="short_ratio",
                     help="Duration mixture (M): fraction of steps that are 'short'")
    ap.add_argument("--short-dur", type=float, default=0.5)
    ap.add_argument("--long-dur", type=float, default=180.0)
    ap.add_argument("--out", required=True)
    ap.add_argument("--prefix", default="bench")
    args = ap.parse_args(argv)

    if not (0.0 <= args.short_ratio <= 1.0):
        ap.error("--m must be between 0.0 and 1.0")

    written = generate(
        n=args.n, fan_out=args.fan_out, short_ratio=args.short_ratio,
        short_dur=args.short_dur, long_dur=args.long_dur,
        out_dir=args.out, prefix=args.prefix,
    )
    print(f"Wrote {written} Argo Workflow manifest(s) to {args.out} "
          f"(N={args.n}, F={args.fan_out}, M={args.short_ratio})")


if __name__ == "__main__":
    main()
