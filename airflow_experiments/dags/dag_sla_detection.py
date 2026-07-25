"""
Experiment: SLA Detection Latency (Section 5.1)
================================================
Measures time between SLA miss event and callback firing.

Setup:
  - Task runs for 45 seconds
  - SLA threshold: 30 seconds
  - Every task should trigger a miss
  - Record: time of miss vs time of callback

Expected result: ~47.8s mean detection latency (vs 30s theoretical floor)

Also runs the false-positive threshold experiment:
  - 500 runs each at 1.0x, 1.2x, 1.5x of mean task duration
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time, logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from config.experiment_config import (
    SLA_TASK_DURATION, SLA_THRESHOLD, RESULTS_DIR
)

log = logging.getLogger(__name__)

# ── SLA miss detection latency ────────────────────────────────────────────────
detection_times = []

def record_sla_miss(dag, task_list, blocking_task_list, slas, session=None):
    """sla_miss_callback: records timestamp when callback fires."""
    import csv, os
    fired_at = datetime.utcnow().isoformat()
    log.info(f"SLA miss callback fired at {fired_at}")
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(os.path.join(RESULTS_DIR, "sla_detection_raw.csv"), "a") as f:
        writer = csv.writer(f)
        writer.writerow([fired_at, dag.dag_id, [t.task_id for t in task_list]])


def slow_task(**context):
    """Task that always exceeds its 30-second SLA."""
    log.info(f"Starting slow task, will run {SLA_TASK_DURATION}s (SLA: 30s)")
    time.sleep(SLA_TASK_DURATION)
    log.info("Slow task complete")


with DAG(
    dag_id="exp_sla_detection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    sla_miss_callback=record_sla_miss,
    tags=["experiment", "sla"],
    description="Section 5.1: SLA miss detection latency, 30 trials",
) as dag_sla:

    slow = PythonOperator(
        task_id="slow_task",
        python_callable=slow_task,
        sla=SLA_THRESHOLD,
    )


# ── False-positive threshold experiment ───────────────────────────────────────
def task_with_variable_duration(mean_duration, multiplier, seed, **context):
    """Task with duration drawn near mean_duration * multiplier."""
    import random
    rng = random.Random(seed + context["task_instance"].try_number)
    # Normal distribution around mean, std = 20% of mean
    duration = max(1, rng.gauss(mean_duration, mean_duration * 0.2))
    log.info(f"Running {duration:.1f}s (SLA threshold multiplier: {multiplier}x)")
    time.sleep(duration)


MEAN_TASK_DURATION = 60   # baseline mean for FP experiment

for multiplier in [1.0, 1.2, 1.5]:
    sla_seconds = MEAN_TASK_DURATION * multiplier

    with DAG(
        dag_id=f"exp_sla_fp_{str(multiplier).replace('.', '_')}x",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        sla_miss_callback=record_sla_miss,
        tags=["experiment", "sla", "false-positive"],
        description=f"SLA false-positive test at {multiplier}x mean duration",
    ) as dag_fp:

        PythonOperator(
            task_id="variable_task",
            python_callable=task_with_variable_duration,
            op_kwargs={
                "mean_duration": MEAN_TASK_DURATION,
                "multiplier": multiplier,
                "seed": 42,
            },
            sla=timedelta(seconds=sla_seconds),
        )
