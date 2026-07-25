"""
Experiment: Alerting MTTA Simulation (Section 6)
=================================================
Simulates three alerting channels and measures mean time to acknowledgment
under a fixed 2-minute simulated human-response interval.

Channels:
  - Email (SMTP): baseline
  - Slack webhook: on_failure_callback
  - PagerDuty (simulated via webhook)

MTTA = delivery_latency + HUMAN_POLLING_INTERVAL (120s)

This is a SIMULATED operational model, not a real production measurement.
The 2-minute polling interval is held constant across all channels to isolate
delivery latency effects. Actual MTTA will vary by team.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time, csv, logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.failure_injector import FailureInjectorOperator
from config.experiment_config import (
    TRANSIENT_FAILURE_RATE, RETRY_COUNT, FIXED_INTERVAL_DELAY,
    HUMAN_POLLING_INTERVAL, SLACK_WEBHOOK_URL, RESULTS_DIR,
    TASK_DURATION_MIN, TASK_DURATION_MAX,
)

log = logging.getLogger(__name__)

RESULTS_FILE = os.path.join(RESULTS_DIR, "alerting_mtta_raw.csv")


def record_alert(channel: str, context):
    """Write alert delivery timestamp to results."""
    fired_at = datetime.utcnow()
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(RESULTS_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([fired_at.isoformat(), channel, dag_id, task_id, run_id])
    log.info(f"Alert recorded: channel={channel} at {fired_at.isoformat()}")


# ── Email callback (simulates SMTP-based alerting) ────────────────────────────
def email_failure_callback(context):
    """
    Simulates SMTP email delivery latency (45-120s median).
    In a real setup, Airflow sends this via email_on_failure=True.
    Here we simulate the delivery delay.
    """
    import random
    rng = random.Random(42 + hash(context["run_id"]) % 1000)
    delivery_latency = rng.uniform(45, 120)
    log.info(f"Simulating email delivery latency: {delivery_latency:.1f}s")
    time.sleep(delivery_latency)
    record_alert("email", context)


# ── Slack webhook callback ─────────────────────────────────────────────────────
def slack_failure_callback(context):
    """
    Sends to Slack webhook or simulates sub-second delivery.
    Real delivery: median 0.8s.
    """
    import requests, random
    rng = random.Random(42 + hash(context["run_id"]) % 1000)

    if SLACK_WEBHOOK_URL:
        ti = context["task_instance"]
        payload = {
            "text": (
                f":red_circle: *Task Failed*\n"
                f"DAG: `{ti.dag_id}`  Task: `{ti.task_id}`\n"
                f"Run: `{context['run_id']}`\n"
                f"Log: {ti.log_url}"
            )
        }
        start = time.monotonic()
        try:
            r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
            delivery_latency = time.monotonic() - start
            log.info(f"Slack delivered in {delivery_latency:.3f}s (status {r.status_code})")
        except Exception as e:
            log.warning(f"Slack delivery failed: {e}, simulating")
            delivery_latency = rng.uniform(0.5, 1.5)
            time.sleep(delivery_latency)
    else:
        # Simulate: median 0.8s, std 0.3s
        delivery_latency = max(0.1, rng.gauss(0.8, 0.3))
        log.info(f"Simulating Slack delivery: {delivery_latency:.3f}s")
        time.sleep(delivery_latency)

    record_alert("slack", context)


# ── PagerDuty simulation ──────────────────────────────────────────────────────
def pagerduty_failure_callback(context):
    """
    Simulates PagerDuty webhook delivery (median <2s).
    Push notification model reduces MTTA vs polling.
    """
    import random
    rng = random.Random(42 + hash(context["run_id"]) % 1000)
    delivery_latency = max(0.1, rng.gauss(1.5, 0.5))
    log.info(f"Simulating PagerDuty delivery: {delivery_latency:.3f}s")
    time.sleep(delivery_latency)
    record_alert("pagerduty", context)


# ── DAGs for each channel ─────────────────────────────────────────────────────
for channel, callback in [
    ("email", email_failure_callback),
    ("slack", slack_failure_callback),
    ("pagerduty", pagerduty_failure_callback),
]:
    with DAG(
        dag_id=f"exp_alerting_{channel}",
        default_args={
            "owner": "experiment",
            "retries": 0,
            "on_failure_callback": callback,
        },
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["experiment", "alerting", channel],
        description=f"Section 6: Alerting MTTA simulation ({channel} channel)",
    ) as dag:

        FailureInjectorOperator(
            task_id="failing_task",
            failure_mode="count",
            recovery_after=999,   # always fail
            permanent=False,
            task_duration_min=TASK_DURATION_MIN,
            task_duration_max=TASK_DURATION_MAX,
        )
