"""
Scheduler Contention Index (SCI) Measurement
============================================
Queries the Airflow metadata database to compute SCI for a given DAG run.

SCI = max tasks re-queued within any 5-second window during a failure event.

Usage:
    python measure_sci.py --run-id <run_id> --dag-id <dag_id>
    python measure_sci.py --all  # compute for all completed experiment runs
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta

import psycopg2
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.experiment_config import DB_CONN, SCI_WINDOW_SECONDS, RESULTS_DIR


def get_db_conn():
    """Connect to Airflow metadata PostgreSQL database."""
    # Strip sqlalchemy prefix if present
    conn_str = DB_CONN.replace("postgresql+psycopg2://", "postgresql://")
    import psycopg2
    from urllib.parse import urlparse
    p = urlparse(conn_str)
    return psycopg2.connect(
        host=p.hostname,
        port=p.port or 5432,
        database=p.path.lstrip("/"),
        user=p.username,
        password=p.password,
    )


def compute_sci(dag_id: str, run_id: str, conn) -> dict:
    """
    Compute the Scheduler Contention Index for a specific DAG run.

    Returns dict with:
      - sci: peak tasks re-queued in any SCI_WINDOW_SECONDS window
      - window_start: timestamp of the peak window
      - total_requeues: total number of retry events in this run
    """
    query = """
        SELECT
            queued_dttm,
            task_id,
            try_number,
            state
        FROM task_instance
        WHERE dag_id = %s
          AND run_id = %s
          AND state IN ('up_for_retry', 'queued')
          AND queued_dttm IS NOT NULL
        ORDER BY queued_dttm ASC
    """
    with conn.cursor() as cur:
        cur.execute(query, (dag_id, run_id))
        rows = cur.fetchall()

    if not rows:
        return {"sci": 0, "window_start": None, "total_requeues": 0}

    df = pd.DataFrame(rows, columns=["queued_dttm", "task_id", "try_number", "state"])
    df["queued_dttm"] = pd.to_datetime(df["queued_dttm"])

    # Count re-queues (try_number > 1 means it was a retry)
    retries = df[df["try_number"] > 1].copy()
    total_requeues = len(retries)

    if retries.empty:
        return {"sci": 0, "window_start": None, "total_requeues": 0}

    # Sliding window: for each event, count events within the next 5 seconds
    window_delta = timedelta(seconds=SCI_WINDOW_SECONDS)
    max_count = 0
    peak_window = None

    times = retries["queued_dttm"].values
    for i, t in enumerate(times):
        t_dt = pd.Timestamp(t)
        window_end = t_dt + window_delta
        count = sum(1 for t2 in times if t_dt <= pd.Timestamp(t2) <= window_end)
        if count > max_count:
            max_count = count
            peak_window = t_dt

    return {
        "sci": max_count,
        "window_start": peak_window,
        "total_requeues": total_requeues,
        "dag_id": dag_id,
        "run_id": run_id,
    }


def compute_sci_all_runs(dag_ids: list, conn) -> pd.DataFrame:
    """Compute SCI for all runs of the specified DAGs."""
    query = """
        SELECT DISTINCT dag_id, run_id, state
        FROM dag_run
        WHERE dag_id = ANY(%s)
          AND state = 'success'
        ORDER BY dag_id, run_id
    """
    with conn.cursor() as cur:
        cur.execute(query, (dag_ids,))
        runs = cur.fetchall()

    results = []
    for dag_id, run_id, state in runs:
        sci_result = compute_sci(dag_id, run_id, conn)
        results.append(sci_result)
        print(f"  {dag_id}/{run_id}: SCI={sci_result['sci']}")

    return pd.DataFrame(results)


def compute_recovery_time(dag_id: str, run_id: str, conn) -> dict:
    """
    Compute mean task recovery time for a DAG run.
    Recovery time = time from first failure to final success for each task.
    """
    query = """
        SELECT
            task_id,
            try_number,
            start_date,
            end_date,
            state
        FROM task_instance
        WHERE dag_id = %s
          AND run_id = %s
        ORDER BY task_id, try_number
    """
    with conn.cursor() as cur:
        cur.execute(query, (dag_id, run_id))
        rows = cur.fetchall()

    if not rows:
        return {"mean_recovery_time": None, "n_tasks": 0}

    df = pd.DataFrame(rows, columns=["task_id", "try_number", "start_date", "end_date", "state"])
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    recovery_times = []
    for task_id, group in df.groupby("task_id"):
        first_attempt = group.nsmallest(1, "try_number").iloc[0]
        final_success = group[group["state"] == "success"]
        if not final_success.empty:
            final = final_success.nlargest(1, "try_number").iloc[0]
            if pd.notna(first_attempt["start_date"]) and pd.notna(final["end_date"]):
                recovery_sec = (final["end_date"] - first_attempt["start_date"]).total_seconds()
                recovery_times.append(recovery_sec / 60.0)  # convert to minutes

    if not recovery_times:
        return {"mean_recovery_time": None, "n_tasks": 0}

    return {
        "mean_recovery_time": sum(recovery_times) / len(recovery_times),
        "std_recovery_time": pd.Series(recovery_times).std(),
        "n_tasks": len(recovery_times),
        "dag_id": dag_id,
        "run_id": run_id,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag-id", help="Specific DAG ID")
    parser.add_argument("--run-id", help="Specific run ID")
    parser.add_argument("--all", action="store_true", help="Process all experiment DAGs")
    parser.add_argument("--output", default=os.path.join(RESULTS_DIR, "sci_results.csv"))
    args = parser.parse_args()

    conn = get_db_conn()

    if args.all:
        experiment_dags = [
            "exp_fixed_interval_concurrent",
            "exp_jitter_concurrent",
        ]
        df = compute_sci_all_runs(experiment_dags, conn)
        os.makedirs(RESULTS_DIR, exist_ok=True)
        df.to_csv(args.output, index=False)
        print(f"\nSCI results written to {args.output}")
        print(df.groupby("dag_id")["sci"].describe())

    elif args.dag_id and args.run_id:
        result = compute_sci(args.dag_id, args.run_id, conn)
        print(f"SCI: {result}")
        recovery = compute_recovery_time(args.dag_id, args.run_id, conn)
        print(f"Recovery: {recovery}")

    conn.close()
