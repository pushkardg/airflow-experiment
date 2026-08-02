# RUNBOOK: Getting real Section 6 numbers before July 31 AoE

Deadline: **WORKS26 Papers and Abstracts, July 31, 2026, AoE** (works-workshop.org).
Format: **IEEE conference template, 8 pages including figures/tables/references.**
This runbook assumes today is on or shortly after July 25, 2026.

This is the fastest realistic path from "no data" to "a real, if modest,
Section 6" -- it deliberately does NOT attempt the full paper's Table 1
matrix (432 runs; the single-system companion harness's equivalent sweep was
independently estimated at ~48 days serial -- see that repo's README). If you
don't have a cluster ready today, the honest fallback is the Abstract track
(4 pages, no full results required, same July 31 deadline) -- worth deciding
NOW rather than after burning days on infrastructure setup.

## Detailed setup instructions (do this before the Day 0 checklist below)

You need all three systems reachable via network from wherever you'll run the
harness. Two paths depending on what you already have:

### Path A: you already have Airflow / Argo / Kubeflow deployed somewhere

Skip to "Configure the harness" below -- just gather these four things:
- Airflow webserver URL + a user/password with API access, and a path the
  scheduler's `dag_folder` resolves to that you can write to.
- Argo Server URL (usually port 2746) + the namespace you're allowed to
  submit Workflows into + a bearer token if auth is required.
- Kubeflow Pipelines endpoint (usually `<host>/pipeline`).
- Network access from your harness-running machine to all three (VPN,
  same cluster, or public endpoints with auth).

### Path B: you need to stand up minimal local instances fast

This is the realistic option if you don't have existing clusters. Budget
most of Day 0-1 for this; do all three in parallel if you can.

**Airflow (fastest -- ~15 minutes, no Kubernetes needed):**
```bash
python3 -m venv airflow-venv && source airflow-venv/bin/activate
pip install apache-airflow
export AIRFLOW_HOME=~/airflow
airflow standalone
```
This starts webserver + scheduler + a SQLite metadata DB and prints an admin
password to the terminal. Note the printed URL (default
`http://localhost:8080`) and the `dag_folder` path
(`$AIRFLOW_HOME/dags`, i.e. `~/airflow/dags` by default) -- that's your
`airflow.dag_folder_path` in the config. SQLite is fine for this reduced
matrix's scale; don't use it if you later scale back up toward the full
Table 1 matrix (it doesn't handle concurrent writes well at higher N).

**A local Kubernetes cluster (needed for both Argo and Kubeflow):**
```bash
# kind (Kubernetes-in-Docker) is the fastest way to get a throwaway cluster:
curl -Lo ./kind https://kind.sigs.k8s.io/dl/latest/kind-linux-amd64
chmod +x ./kind && sudo mv ./kind /usr/local/bin/
kind create cluster --name bench
kubectl cluster-info   # confirm it's up
```

**Argo Workflows on that cluster (~10 minutes):**
```bash
kubectl create namespace argo
kubectl apply -n argo -f https://github.com/argoproj/argo-workflows/releases/latest/download/quick-start-minimal.yaml
kubectl -n argo port-forward svc/argo-server 2746:2746 &
```
Your `argo.base_url` is `https://localhost:2746`, `argo.namespace` is `argo`.
The quick-start manifest disables auth by default (fine for a throwaway
local cluster; never use `quick-start` manifests against anything internet-
reachable) -- leave `bearer_token: null`.

**Kubeflow Pipelines on that cluster (heaviest -- can take 20-30+ minutes,
start this first if going down Path B):**
```bash
export PIPELINE_VERSION=2.2.0
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=$PIPELINE_VERSION"
kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=$PIPELINE_VERSION"
kubectl wait pods -n kubeflow -l application-crd-id=kubeflow-pipelines --for condition=Ready --timeout=1800s
kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8888:80 &
```
Your `kubeflow.host` is `http://localhost:8888/pipeline` (some KFP versions
use a different sub-path -- if `get_kfp_healthz()` fails in the smoke test
below, check the exact ingress path with `kubectl get svc -n kubeflow`).

If the Kubeflow install is taking too long and eating your Day 0-1 budget,
that's itself useful signal -- see the Abstract-track fallback at the
bottom of this file rather than let it consume the whole window.

### Configure the harness

Edit `config/minimal_matrix.yaml` directly with the values gathered above:
```yaml
airflow:
  base_url: "http://localhost:8080"
  auth_username: "admin"          # from `airflow standalone`'s printed output
  auth_password: "<printed password>"
  dag_folder_path: "/home/<you>/airflow/dags"

argo:
  base_url: "https://localhost:2746"
  namespace: "argo"
  bearer_token: null

kubeflow:
  host: "http://localhost:8888/pipeline"
```

## Day 0 (today): infrastructure check -- do this first, before touching code

You need, reachable from wherever you'll run the harness:
- [ ] An Airflow 2.x instance with the stable REST API enabled, and a
      dag_folder path this host can write to (local disk if co-located,
      or a shared volume / git-sync target otherwise).
- [ ] An Argo Workflows instance with the Argo Server REST API reachable
      (`base_url` in config), and a namespace you can submit to.
- [ ] A Kubeflow Pipelines instance reachable via `kfp.Client` (the `host`
      in config is typically `<your-kfp-endpoint>/pipeline`).

If you don't have all three today and can't stand them up in ~1 day, stop
here and switch to the Abstract-track fallback (see bottom of this file) --
that decision is much cheaper to make on Day 0 than on Day 4.

## Day 1: wire the two remaining environment-specific pieces

Both are marked `NotImplementedError` or a config field to fill in --
nothing else in the harness needs code changes.

1. `config/minimal_matrix.yaml`: set `airflow.dag_folder_path`,
   `argo.base_url` / `namespace` / `bearer_token`, `kubeflow.host`.
2. Smoke-test connectivity for each system BEFORE running the sweep:
   ```bash
   python3 -c "
   from harness.run_comparative_matrix import load_config
   from airflow_adapter.airflow_client import AirflowClient
   from argo_adapter.argo_client import ArgoClient
   from kubeflow_adapter.kfp_client import KubeflowClient
   cfg = load_config('config/minimal_matrix.yaml')
   print('Airflow:', AirflowClient(cfg['airflow']['base_url'], (cfg['airflow']['auth_username'], cfg['airflow']['auth_password'])).list_dags(limit=1))
   print('Argo namespace:', cfg['argo']['namespace'])  # confirm this exists in your cluster
   print('Kubeflow healthz:', KubeflowClient(cfg['kubeflow']['host'])._client().get_kfp_healthz())
   "
   ```
   Fix connection errors here, not mid-sweep.

## Day 2: dry-run, then run the minimal matrix for real

```bash
# Confirm generation + compilation still works against your real config:
python3 harness/run_comparative_matrix.py --config config/minimal_matrix.yaml --dry-run

# The real run -- 48 total runs. Expect this to take from tens of minutes to
# a few hours depending on your cluster's actual scheduling latency (that's
# the thing you're measuring) and DAG-parse/pickup delay on the Airflow side
# especially. Run it, don't estimate it -- and consider running it in a
# screen/tmux session since it's a long-lived foreground process.
python3 harness/run_comparative_matrix.py --config config/minimal_matrix.yaml
```

If it fails partway through: raw records are written per-run
(`results/raw/*.json`), so a partial run is not a lost run -- fix the error
and re-run just the remaining configs if needed, or accept a smaller N for
whichever system failed and disclose that honestly in Section 8.

## Day 3: analyze, and update the paper -- do NOT skip the disclosure step

```bash
python3 analysis/analyze_comparative.py --raw-dir results/raw --out-dir results/analysis
```

This produces `pairwise_submission_to_running.csv` and
`pairwise_running_to_completion.csv` -- the direct source for Section 6.1's
table.

**Required, not optional:** update paper Section 4.1 / Table 1 to state the
*actual* tested ranges (N in {10, 50}, F in {1, 20}, M = 0.9, long-duration
tasks shortened to 15s -- see `config/minimal_matrix.yaml`'s header comment
for the full list and rationale), with the originally-planned full range
explicitly moved to Future Work. Do not leave Table 1 claiming ranges you
didn't actually test -- a reviewer cross-checking Section 6's data against
Table 1 and finding a mismatch is a much worse outcome than an honestly
smaller study.

## Day 4: reformat to the IEEE template

The current draft is built in ACM-style two-column formatting. WORKS26
requires the **IEEE conference proceedings template** (LaTeX "conference"
mode, or the IEEE MS Word template from
https://www.ieee.org/conferences/publishing/templates). This is a real
reformatting task, not a find-and-replace -- budget a half-day for it,
separate from the writing time above.

## Day 5: write Section 6 prose + Discussion updates, trim to 8 pages

With real numbers in hand: replace every `[Fill in]` in Section 6 with
actual findings, update the Decision Framework (Table 5) rows from
HYPOTHESIS to either CONFIRMED or CONTRADICTED based on what the data
actually shows (don't leave a row as HYPOTHESIS if you have data that
bears on it), and re-check the 8-page IEEE limit (includes references).

## Day 6 (July 31, AoE): submit

Submit via http://submissions.supercomputing.org. AoE means you technically
have until anywhere-on-Earth midnight, i.e. UTC-12 -- roughly a same-day
buffer past standard end-of-day, not an extra day.

---

## Fallback: Abstract track (if Day 0's infrastructure check fails)

WORKS26 also accepts 4-page Abstracts (same July 31 deadline, panel-track
review, no full results required) -- a legitimate, much lower-risk option
if real infrastructure isn't ready today. This paper's motivation,
architecture comparison (Section 2), and methodology (Section 4) already
contain more than enough content for a strong 4-page abstract describing
planned work, without needing Section 6 at all.
