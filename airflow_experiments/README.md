# Replication Package

**Paper:** "Empirical Evaluation of Failure Recovery Strategies in Apache Airflow:
Retry Policies, Deadline Alerts, and Alerting Architectures"

**Submitted to:** WORKS 2026 (Workshop on Workflows in Support of Large-Scale Science)

---

## Overview

This package contains everything needed to replicate the experiments in the paper.
All reported numbers (62% SCI reduction, 8.4 min recovery time, 47.8s SLA detection
latency, etc.) are produced by running the scripts in this directory.

**Estimated total runtime:** 18–24 hours on reference hardware.
**Reference hardware:** Ubuntu 22.04 LTS, 8 vCPUs, 32 GB RAM, single node.

---

## Option A: Docker (Recommended — 30-minute setup)

### Prerequisites
- Docker and Docker Compose installed
- 32 GB RAM available to Docker

### Steps

```bash
# 1. Clone / unzip this package
cd airflow_experiments/

# 2. (Optional) Set Slack webhook for Section 6 alerting experiments
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# 3. Start the environment
docker-compose up -d

# 4. Wait for Airflow to initialize (~60 seconds)
docker-compose logs -f airflow-init

# 5. Verify Airflow is up
curl http://localhost:8080/health   # should return {"status": "healthy", ...}

# 6. Run all experiments (from host, or exec into container)
docker-compose exec airflow-webserver \
    python /opt/airflow/scripts/run_all_experiments.py

# 7. Analyze and produce tables
docker-compose exec airflow-webserver \
    python /opt/airflow/scripts/analyze_results.py

# Results are in ./results/
```

Email alerts are captured by MailHog at http://localhost:8025 (no real SMTP needed).

---

## Option B: Manual Install

### Prerequisites

```bash
# Python 3.10
python --version  # 3.10.x

# PostgreSQL 15
psql --version    # 15.x

# Redis 7
redis-cli --version  # 7.x
```

### Install

```bash
pip install apache-airflow==2.9.3 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"

pip install -r requirements.txt
```

### Initialize Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=16
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow
export AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=5

airflow db init

airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@example.com

# Copy DAGs
cp dags/*.py $AIRFLOW_HOME/dags/

# Start services (two separate terminals)
airflow webserver --port 8080 &
airflow scheduler &
```

### Run experiments

```bash
# All sections (18-24 hours total)
python scripts/run_all_experiments.py

# Or run individually by section:
python scripts/run_all_experiments.py --section 4    # Retry (Sections 4.2-4.4)
python scripts/run_all_experiments.py --section 5    # SLA (Section 5.1)
python scripts/run_all_experiments.py --section 6    # Alerting (Section 6)

# Dry run to verify setup without triggering DAGs:
python scripts/run_all_experiments.py --dry-run

# Analyze results:
python scripts/analyze_results.py
```

---

## Experiment Details

### Section 4: Retry Policy Comparison

| DAG | Strategy | Failure rate | Trials |
|-----|----------|-------------|--------|
| exp_fixed_interval_single | Fixed-interval (d=5min, r=3) | 10% transient | 30 |
| exp_fixed_interval_concurrent | Fixed-interval, 20 concurrent DAGs | 10% transient | 30 |
| exp_backoff_nojitter_single | Exp backoff, no jitter | 10% transient | 30 |
| exp_jitter_single | Exp backoff + full jitter | 10% transient | 30 |
| exp_jitter_concurrent | Full jitter, 20 concurrent DAGs | 10% transient | 30 |
| exp_exception_aware | Exception-aware (AirflowFailException) | 50/50 transient/permanent | 30 |

**Key metric:** SCI (Scheduler Contention Index) = max tasks re-queued in any 5-second window.
Measured from the Airflow metadata DB by `scripts/measure_sci.py`.

**Expected result:** Full-jitter backoff reduces peak SCI by ~62% vs fixed-interval
under 20-concurrent-DAG scenarios.

### Section 5: SLA Detection Latency

**5.1 — Airflow 2.9.3 (polling-based):**
```bash
python scripts/run_all_experiments.py --section 5
```
Creates tasks that run 45 seconds against a 30-second SLA.
Measures time from task completion to sla_miss_callback invocation.
**Expected result:** ~47.8 seconds mean (well above the 30-second theoretical floor).

**5.2 — Airflow 3.1.0 AIP-86 (requires separate environment):**
```bash
# Install Airflow 3.1.0 in a separate virtualenv
python -m venv venv_airflow3
source venv_airflow3/bin/activate
pip install apache-airflow==3.1.0

# Copy the AIP-86 DAG only
cp dags/dag_aip86_deadline.py $AIRFLOW_HOME/dags/

# Run 30 trials
python scripts/run_all_experiments.py --section 5.2
```
**Expected result:** ~4.2 seconds mean detection latency.

### Section 6: Alerting MTTA Simulation

The MTTA measurements use a **simulated 2-minute human-response model**, not real
human acknowledgment. The script adds 120 seconds to raw alert delivery latency
to model an engineer checking alerts at a fixed 2-minute interval.

```bash
# Configure alerting channels first:
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."  # your webhook
export SMTP_HOST=localhost   # or use Docker MailHog on port 1025

python scripts/run_all_experiments.py --section 6
```

**Expected results (simulated model):**
- Email (SMTP): ~11.3 min mean MTTA
- Slack webhook: ~4.2 min mean MTTA (-63%)
- PagerDuty: ~2.1 min mean MTTA (-81%)

For PagerDuty, set:
```bash
export PAGERDUTY_ROUTING_KEY="your-events-api-v2-key"
```

---

## File Structure

```
airflow_experiments/
├── README.md                    ← This file
├── requirements.txt             ← Python dependencies
├── docker-compose.yml           ← Full environment (recommended)
│
├── config/
│   ├── experiment_config.py     ← ALL parameters (n=30, seed=42, etc.)
│   └── prometheus.yml           ← Prometheus config for Section 6 observability
│
├── dags/
│   ├── dag_fixed_interval.py    ← Section 4.2: Fixed-interval retry
│   ├── dag_exponential_jitter.py ← Section 4.3: Exp backoff + full jitter
│   ├── dag_exception_aware.py   ← Section 4.4: Exception-aware retry
│   ├── dag_sla_detection.py     ← Section 5.1: SLA detection (Airflow 2.9.3)
│   ├── dag_aip86_deadline.py    ← Section 5.2: AIP-86 (Airflow 3.1.0)
│   └── dag_alerting_mtta.py     ← Section 6: Email/Slack/PagerDuty MTTA
│
└── scripts/
    ├── failure_injector.py      ← FailureInjectorOperator (core component)
    ├── run_all_experiments.py   ← Orchestrator: triggers DAGs, collects results
    ├── measure_sci.py           ← Computes SCI from Airflow metadata DB
    └── analyze_results.py       ← Statistics, Table 1/2, Cohen's d, 95% CI
```

---

## Reproducing Specific Numbers

### Table 1: Retry Strategy Comparison

```bash
python scripts/run_all_experiments.py --section 4
python scripts/analyze_results.py --section 4
```

Produces `results/table1_retry_comparison.csv` with:
- Mean recovery time ± std dev per strategy
- 95% confidence intervals
- SCI values
- Mann-Whitney U p-values and Cohen's d

### Table 2: Alerting Comparison

```bash
python scripts/run_all_experiments.py --section 6
python scripts/analyze_results.py --section 6
```

Produces `results/table2_alerting_comparison.csv`.

### SLA Detection Latency (Section 5.1)

```bash
python scripts/run_all_experiments.py --section 5
python scripts/analyze_results.py --section 5
```

Produces `results/sla_detection_latency.csv` with the per-trial detection latencies
that should average ~47.8 seconds on Airflow 2.9.3 with default scheduler config.

---

## Troubleshooting

**DAGs not appearing in UI:**
```bash
airflow dags list  # check they're parsed
airflow dags trigger exp_fixed_interval_single  # manual trigger test
```

**SCI measurement fails:**
Check that `DB_CONN` in `config/experiment_config.py` matches your PostgreSQL DSN.

**AIP-86 import error:**
The `DeadlineAlert` import path may differ between Airflow 3.x patch versions.
Check the [AIP-86 wiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=323488182)
for the correct import path for your version.

**Results differ from paper:**
Minor variation is expected (n=30 means ~±5-10% on effect sizes). If directional
results differ (e.g., jitter performs *worse* than fixed-interval), check:
1. `AIRFLOW__CORE__PARALLELISM` is set to 16
2. `AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC` is set to 5
3. You are using LocalExecutor (not CeleryExecutor)

---

## Citation

```bibtex
@inproceedings{gopalakrishna2026airflow,
  title={Empirical Evaluation of Failure Recovery Strategies in Apache Airflow:
         Retry Policies, Deadline Alerts, and Alerting Architectures},
  author={Gopalakrishna, Pushkar Devanahalli},
  booktitle={Proceedings of the 21st Workshop on Workflows in Support of
             Large-Scale Science (WORKS '26)},
  year={2026},
  publisher={IEEE}
}
```

---

## Contact

pushkar87dg@gmail.com
