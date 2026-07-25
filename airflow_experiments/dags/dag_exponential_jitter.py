"""
Experiment: Exponential Backoff with Full Jitter (Section 4.3)
==============================================================
30 trials single + 30 trials concurrent (20 DAGs)

Also includes no-jitter variant for Table 1 intermediate row.

Measures:
  - Mean task recovery time
  - SCI under concurrent failures (62% reduction expected vs fixed-interval)
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime, timedelta
from airflow import DAG
from scripts.failure_injector import FailureInjectorOperator, make_jitter_callback
from config.experiment_config import (
    TRANSIENT_FAILURE_RATE, RETRY_COUNT,
    BACKOFF_BASE_SECONDS, BACKOFF_MAX_SECONDS,
    TASKS_PER_DAG_MIN, TASK_DURATION_MIN, TASK_DURATION_MAX,
)

jitter_cb = make_jitter_callback(BACKOFF_BASE_SECONDS, BACKOFF_MAX_SECONDS)

# ── Full-jitter single DAG ────────────────────────────────────────────────────
default_args_jitter = {
    "owner": "experiment",
    "retries": RETRY_COUNT,
    "retry_delay": timedelta(seconds=BACKOFF_BASE_SECONDS),
    "retry_exponential_backoff": False,   # jitter handled via callback
}

with DAG(
    dag_id="exp_jitter_single",
    default_args=default_args_jitter,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["experiment", "retry", "jitter"],
    description="Section 4.3: Full-jitter backoff, single DAG, 30 trials",
) as dag_jitter_single:

    tasks = []
    for i in range(TASKS_PER_DAG_MIN):
        t = FailureInjectorOperator(
            task_id=f"task_{i:02d}",
            failure_mode="probabilistic",
            failure_rate=TRANSIENT_FAILURE_RATE,
            permanent=False,
            task_duration_min=TASK_DURATION_MIN,
            task_duration_max=TASK_DURATION_MAX,
            seed_offset=i * 100,
            on_retry_callback=jitter_cb,
        )
        if tasks:
            tasks[-1] >> t
        tasks.append(t)


# ── Full-jitter concurrent DAG ────────────────────────────────────────────────
with DAG(
    dag_id="exp_jitter_concurrent",
    default_args=default_args_jitter,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["experiment", "retry", "jitter", "concurrent"],
    description="Section 4.3: Full-jitter backoff, concurrent DAG (1 of 20)",
) as dag_jitter_concurrent:

    tasks_c = []
    for i in range(TASKS_PER_DAG_MIN):
        t = FailureInjectorOperator(
            task_id=f"task_{i:02d}",
            failure_mode="probabilistic",
            failure_rate=TRANSIENT_FAILURE_RATE,
            permanent=False,
            task_duration_min=TASK_DURATION_MIN,
            task_duration_max=TASK_DURATION_MAX,
            seed_offset=i * 100,
            on_retry_callback=jitter_cb,
        )
        if tasks_c:
            tasks_c[-1] >> t
        tasks_c.append(t)


# ── No-jitter exponential backoff (Table 1 intermediate row) ─────────────────
default_args_nojitter = {
    "owner": "experiment",
    "retries": RETRY_COUNT,
    "retry_delay": timedelta(seconds=BACKOFF_BASE_SECONDS),
    "retry_exponential_backoff": True,   # native Airflow: deterministic multiplier
}

with DAG(
    dag_id="exp_backoff_nojitter_single",
    default_args=default_args_nojitter,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["experiment", "retry", "backoff-nojitter"],
    description="Table 1 intermediate: deterministic exponential backoff, no jitter",
) as dag_nojitter:

    tasks_n = []
    for i in range(TASKS_PER_DAG_MIN):
        t = FailureInjectorOperator(
            task_id=f"task_{i:02d}",
            failure_mode="probabilistic",
            failure_rate=TRANSIENT_FAILURE_RATE,
            permanent=False,
            task_duration_min=TASK_DURATION_MIN,
            task_duration_max=TASK_DURATION_MAX,
            seed_offset=i * 100,
        )
        if tasks_n:
            tasks_n[-1] >> t
        tasks_n.append(t)
