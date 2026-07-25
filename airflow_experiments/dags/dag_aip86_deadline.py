"""
Experiment: AIP-86 Deadline Alerts - Airflow 3.1.0 (Section 5.2)
==================================================================
IMPORTANT: This DAG requires Airflow 3.1.0 with AIP-86 enabled.
Run separately from the main experiment suite:

    # Install Airflow 3.1.0 in a separate environment
    pip install apache-airflow==3.1.0

    # Then run this DAG only:
    python scripts/run_all_experiments.py --section 5.2

Measures:
  - SLA miss detection latency using AIP-86 Deadline Alerts
  - Comparison baseline: dag_sla_detection.py (Airflow 2.9.3 result = 47.8s mean)

Expected result: ~4.2 seconds mean detection latency (paper Section 5.2)
"""

import time
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# AIP-86 Deadline Alerts API (Airflow 3.1.0+)
# The import path may vary between 3.x patch releases.
# If this import fails, check: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=323488182
try:
    from airflow.timetables.deadlines import DeadlineAlert
    AIP86_AVAILABLE = True
except ImportError:
    AIP86_AVAILABLE = False
    log.warning("AIP-86 DeadlineAlert not available. Are you running Airflow 3.1.0+?")

# ── Task that runs slightly over the deadline ─────────────────────────────────

def slow_task(**context):
    """
    Simulates a task that takes 45 seconds, intentionally exceeding
    a 30-second deadline. Used to measure detection latency.
    """
    run_id = context['run_id']
    start = time.time()
    log.info(f"Task started: {run_id}")
    time.sleep(45)   # Exceeds the 30-second deadline
    elapsed = time.time() - start
    log.info(f"Task completed in {elapsed:.1f}s")

    # Record task completion timestamp for latency measurement
    ti = context['task_instance']
    ti.xcom_push(key='task_end_ts', value=time.time())


def record_detection(**context):
    """
    Placeholder: in production, this would be the deadline callback.
    Detection timestamp is recorded by the deadline alert listener.
    """
    detection_ts = time.time()
    ti = context['task_instance']
    ti.xcom_push(key='detection_ts', value=detection_ts)
    log.info(f"Deadline alert fired at: {detection_ts}")


# ── DAG with AIP-86 Deadline Alert ───────────────────────────────────────────

if AIP86_AVAILABLE:
    # AIP-86 style: deadline relative to DAG run queued time
    deadline_alert = DeadlineAlert(
        reference="DAGRUN_QUEUED_AT",
        deadline=timedelta(seconds=30),
        callback=record_detection,
    )

    with DAG(
        dag_id="exp_aip86_deadline_single",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["experiment", "sla", "aip86", "airflow-3"],
        description="Section 5.2: AIP-86 Deadline Alerts detection latency, 30 trials",
        # AIP-86 deadline configuration
        deadline_alerts=[deadline_alert],
    ) as dag_aip86:

        task_slow = PythonOperator(
            task_id="slow_task",
            python_callable=slow_task,
        )

else:
    # Fallback: create a dummy DAG that fails loudly if AIP-86 not available
    with DAG(
        dag_id="exp_aip86_deadline_single",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["experiment", "sla", "aip86", "airflow-3"],
        description="AIP-86 not available - requires Airflow 3.1.0",
    ) as dag_aip86:

        def fail_with_message():
            raise RuntimeError(
                "This DAG requires Airflow 3.1.0 with AIP-86 enabled.\n"
                "Install: pip install apache-airflow==3.1.0\n"
                "See: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=323488182"
            )

        PythonOperator(
            task_id="aip86_not_available",
            python_callable=fail_with_message,
        )
