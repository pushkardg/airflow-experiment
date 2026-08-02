# Comparative Orchestrator Benchmark (Airflow vs. Argo Workflows vs. Kubeflow Pipelines)

Code implementing paper Sections 4 (Methodology) and the underlying data source for
Section 6 (Results) of the comparative-study draft. This produces real (N, F, M)
workloads across all three systems using each system's native DAG-authoring model,
plus the cross-system statistical comparison (Section 4.4).

## Layout

```
airflow_adapter/generate_dags.py       Reused unmodified from the single-system harness
argo_adapter/generate_workflows.py     Generates N Argo Workflow YAML manifests, F-fan-out via withItems
argo_adapter/argo_client.py            Argo Server REST API wrapper
kubeflow_adapter/generate_pipeline.py  Generates N KFP v2 pipelines, F-fan-out via ParallelFor;
                                        compiles each via the REAL kfp SDK compiler
kubeflow_adapter/kfp_client.py         Wrapper around the real kfp.Client SDK (not hand-rolled REST)
common/metrics_schema.py               Section 4.3's common cross-system metrics schema
harness/run_comparative_matrix.py      Sweeps (N, F, M) across all three systems
analysis/analyze_comparative.py        Section 4.4's pairwise cross-system statistical comparison
config/comparative_matrix.yaml         The sweep, as config
```

## What's been verified, and how

Unlike the single-system harness, **this package could actually install and use two of
the three systems' real SDKs** (`kfp==2.17.0`), which allowed stronger verification than
"does it parse":

- **Kubeflow adapter: compiled through the real KFP SDK compiler**, not just syntax-checked.
  This caught two real bugs during development that a syntax check alone would have missed:
  1. `dsl.ParallelFor` rejects an *empty* item list at compile time (`ValueError: Got an
     empty item list for loop argument`) -- happens legitimately when F is small and the
     deterministic short/long split produces one empty group. Fixed by only emitting a
     `ParallelFor` block for non-empty groups.
  2. A pipeline with **zero tasks** is rejected outright (`ValueError: Task is missing
     from pipeline`) -- this makes F=0 a *structurally unsupported* benchmark point for
     Kubeflow specifically, unlike Airflow (an empty `.expand()` list is legal) and Argo
     (an empty `withItems` list is legal). This is a genuine, discovered cross-system
     difference worth citing directly in paper Section 5.4, not just an implementation
     detail -- the harness now skips F=0 for the Kubeflow arm only (see
     `run_comparative_matrix.py::iter_configs`).
  After the fix: re-verified against F=0 (now cleanly rejected with a clear error instead
  of a raw traceback), F=1 (highest chance of an empty group) across M=0.0/0.5/1.0
  boundary values, and a larger N=20/F=500 sweep -- all compiled successfully, and a
  sample compiled IR file was inspected directly to confirm it's a structurally valid
  KFP v2 pipeline spec (not just "didn't crash").
- **Argo adapter**: no `argo` CLI was available in this sandbox to run `argo lint`
  against, so verification is YAML-syntax validity plus internal structural consistency
  (entrypoint references an existing template, template names match) across multiple
  generated files -- weaker than the Kubeflow verification. If you have `argo` CLI
  access, running `argo lint` against a sample of generated manifests before the real
  benchmark run is a reasonable extra check this package doesn't do for you.
- **`common/metrics_schema.py`'s extraction functions** (turning each system's native
  REST response shape into the common `CommonRunTiming`) were tested against
  hand-constructed realistic fixture payloads for all three systems, including a
  missing-data edge case (no task instances recorded yet) -- all correct.
- **`analysis/analyze_comparative.py`** was run against a synthetic dataset with a known
  designed ordering (Argo fastest, Airflow middle, Kubeflow slowest) and correctly
  recovered that ordering across all 12 pairwise comparisons (3 pairs x 4 (N,F)
  configurations), with correctly-signed Cliff's deltas and properly Holm-Bonferroni-
  corrected p-values.
- **`harness/run_comparative_matrix.py --dry-run`** runs the full sweep across all three
  systems' generators in one pass without errors, and correctly skips F=0 for Kubeflow
  only (verified: Airflow and Argo both ran at F=0, Kubeflow ran only its F=5 configs).

## Architectural asymmetry discovered while wiring this up

Airflow needs a **deploy step** -- generated DAG files have to land in the scheduler's
`dag_folder` before they can run at all. Argo and Kubeflow need **no such step**: both
submit directly via their REST/SDK APIs (`ArgoClient.submit_workflow`,
`KubeflowClient.submit_run`), with no shared-filesystem hop in between. This is visible
directly in the harness code (`submit_and_collect_airflow` has a `deploy_dags_airflow()`
call the other two don't), and is worth citing directly in paper Section 2.4 as a concrete
instance of the "no separate scheduler process" architectural difference, rather than
only an abstract claim.

## What's still environment-specific (`NotImplementedError` or a config field)

- `config/minimal_matrix.yaml` -- **if you're racing the July 31 WORKS26 deadline, see
  `RUNBOOK.md` first**, not this section. It's a drastically reduced (N, F, M) matrix
  (48 runs vs. the full matrix's 432) designed to produce real numbers within hours, with
  the exact deviations from Table 1 documented in its header comment.
- `harness/run_comparative_matrix.py::deploy_dags_airflow()` -- now implemented for the
  common case (a local path or shared-volume mount the scheduler watches); set
  `airflow.dag_folder_path` in your config. If your cluster has no shared filesystem,
  replace the `shutil.copytree` call with `kubectl cp` / rsync / git-sync.
- `submit_and_collect_airflow()` -- **now fully implemented**, using the same
  `AirflowClient` already tested in the companion single-system harness (copied in at
  `airflow_adapter/airflow_client.py`).
- Argo and Kubeflow's `submit_and_collect_*` -- implemented; just need real `base_url` /
  `host` values in your config.

## Quickstart

```bash
pip install requests PyYAML pandas numpy scipy kfp

# Sanity-check the full three-system sweep without touching any live cluster:
python harness/run_comparative_matrix.py --config config/comparative_matrix.yaml --dry-run

# Once the Airflow hooks are wired in (see above) and config points at real clusters:
python harness/run_comparative_matrix.py --config config/comparative_matrix.yaml

# Cross-system statistical comparison (Section 4.4):
python analysis/analyze_comparative.py --raw-dir results/raw --out-dir results/analysis
```

## Relationship to the paper

| Paper artifact | Produced by |
|---|---|
| Figure 2 (shared benchmark methodology) | This package's three adapters + `run_comparative_matrix.py` |
| Section 4.3 (common metrics schema) | `common/metrics_schema.py` |
| Section 5.4 (measured DAG-authoring line counts, Table 4) | Already measured directly in the paper draft; not reproduced here |
| Section 6.1 (orchestration overhead by system) | `analysis/analyze_comparative.py` -> `pairwise_submission_to_running.csv` |
| Section 6.2 (K8s- vs. orchestrator-attributable overhead) | `common/metrics_schema.py::decompose_overhead()`, needs Prometheus wiring not yet built |
| Section 6.4 (representative-pipeline sanity check) | Not yet built -- would reuse the same three adapters against one fixed pipeline instead of the (N,F,M) sweep |
