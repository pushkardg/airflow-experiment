"""
Experiment: Fixed-Interval Retry (Section 4.2)
===============================================
30 trials x single DAG + 30 trials x 20 concurrent DAGs

Measures:
  - Mean task recovery time
  - SCI (scheduler contention index) under concurrent failures
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.failure_injector import FailureInjectorOperator
from config.experiment_config import (
    TRANSIENT_FAILURE_RATE, RETRY_COUNT, FIXED_INTERVAL_DELAY,
    TASKS_PER_DAG_MIN, TASK_DURATION_MIN, TASK_DURATION_MAX, RANDOM_SEED,
)

# ── Single-DAG trial (run 30 times, tag trial number in run_id) ───────────────
default_args = {
    "owner": "experiment",
    "retries": RETRY_COUNT,
    "retry_delay": FIXED_INTERVAL_DELAY,
    "retry_exponential_backoff": False,
}

with DAG(
    dag_id="exp_fixed_interval_single",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["experiment", "retry", "fixed-interval"],
    description="Section 4.2: Fixed-interval retry, single DAG, 30 trials",
) as dag_single:

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
        )
        if tasks:
            tasks[-1] >> t
        tasks.append(t)


# ── Concurrent-DAG trial (20 DAGs triggered simultaneously) ──────────────────
with DAG(
    dag_id="exp_fixed_interval_concurrent",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["experiment", "retry", "fixed-interval", "concurrent"],
    description="Section 4.2: Fixed-interval retry, concurrent DAG (1 of 20)",
) as dag_concurrent:

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
        )
        if tasks_c:
            tasks_c[-1] >> t
        tasks_c.append(t)
