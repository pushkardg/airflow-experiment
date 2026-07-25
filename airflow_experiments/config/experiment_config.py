"""
Central configuration for all experiments.
All numbers in the paper come from these parameters.
"""

import os
from datetime import timedelta

# ── Reproducibility ───────────────────────────────────────────────────────────
RANDOM_SEED = 42

# ── Infrastructure ────────────────────────────────────────────────────────────
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
DB_CONN = os.environ.get(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@localhost/airflow"
)

# ── Experiment scale ──────────────────────────────────────────────────────────
N_TRIALS = 30                    # trials per configuration (paper: n=30)
ALPHA = 0.05                     # significance level

# ── DAG topology ─────────────────────────────────────────────────────────────
TASKS_PER_DAG_MIN = 5            # linear pipeline depth
TASKS_PER_DAG_MAX = 10
TASK_DURATION_MIN = 10           # seconds, uniform draw
TASK_DURATION_MAX = 120
CONCURRENT_DAGS = 20             # for concurrent failure scenarios

# ── Failure injection ─────────────────────────────────────────────────────────
TRANSIENT_FAILURE_RATE = 0.10    # 10% per-attempt (paper: Section 3.3)
MIXED_PERMANENT_RATE = 0.50      # 50/50 for exception-aware (Section 4.4)

# ── Retry configurations ──────────────────────────────────────────────────────
RETRY_COUNT = 3

FIXED_INTERVAL_DELAY = timedelta(minutes=5)

BACKOFF_BASE_SECONDS = 60
BACKOFF_MAX_SECONDS = 300

# ── SCI metric ────────────────────────────────────────────────────────────────
SCI_WINDOW_SECONDS = 5           # scheduler heartbeat window (paper: Section 3.3)

# ── SLA experiments ───────────────────────────────────────────────────────────
SLA_TASK_DURATION = 45           # seconds (exceeds 30s SLA)
SLA_THRESHOLD = timedelta(seconds=30)
SLA_TRIALS = 30

SLA_FP_MULTIPLIERS = [1.0, 1.2, 1.5]   # threshold as multiple of mean duration
SLA_FP_RUNS_PER_CONDITION = 500

# ── Alerting simulation ───────────────────────────────────────────────────────
HUMAN_POLLING_INTERVAL = 120     # seconds (2-minute simulated model, paper: Section 6)
ALERTING_TRIALS = 30

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "localhost")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "25"))

# ── Results output ────────────────────────────────────────────────────────────
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
