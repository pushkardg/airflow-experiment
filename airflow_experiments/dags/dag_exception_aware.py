"""
Experiment: Exception-Aware Retry (Section 4.4)
================================================
30 trials with 50/50 transient/permanent failure mix.

AirflowException  => retry consumed (transient)
AirflowFailException => immediate fail, no retry (permanent)

Measures:
  - Mean recovery time
  - Total wasted execution time (time spent on tasks that would never succeed)
  - Comparison: 44% reduction in wasted compute vs naive fixed retry

Note: Different workload (50/50 mix) from Sections 4.2/4.3 (10% transient only).
Table 1 footnote makes this explicit.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator
from scripts.failure_injector import FailureInjectorOperator
from config.experiment_config import (
    RETRY_COUNT, FIXED_INTERVAL_DELAY,
    TASKS_PER_DAG_MIN, TASK_DURATION_MIN, TASK_DURATION_MAX,
    RANDOM_SEED,
)

MIXED_PERMANENT_RATE = 0.50   # 50% of failures are permanent


def mixed_failure_task(seed_offset, **context):
    """
    Task that randomly selects transient vs permanent failure.
    50% of failure events raise AirflowFailException (permanent).
    50% raise AirflowException (transient, retry triggered).
    """
    import time, random
    ti = context["task_instance"]
    rng = random.Random(RANDOM_SEED + seed_offset + ti.try_number)

    duration = rng.uniform(TASK_DURATION_MIN, TASK_DURATION_MAX)
    time.sleep(duration)

    # 10% overall failure rate, 50/50 split permanent/transient
    if rng.random() < 0.10:
        if rng.random() < MIXED_PERMANENT_RATE:
            raise AirflowFailException("Permanent failure: deterministic error")
        else:
            raise AirflowException("Transient failure: intermittent error")


default_args = {
    "owner": "experiment",
    "retries": RETRY_COUNT,
    "retry_delay": FIXED_INTERVAL_DELAY,
    "retry_exponential_backoff": False,
}

with DAG(
    dag_id="exp_exception_aware",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["experiment", "retry", "exception-aware"],
    description="Section 4.4: Exception-aware retry, 50/50 failure mix, 30 trials",
) as dag:

    tasks = []
    for i in range(TASKS_PER_DAG_MIN):
        t = PythonOperator(
            task_id=f"task_{i:02d}",
            python_callable=mixed_failure_task,
            op_kwargs={"seed_offset": i * 100},
        )
        if tasks:
            tasks[-1] >> t
        tasks.append(t)
