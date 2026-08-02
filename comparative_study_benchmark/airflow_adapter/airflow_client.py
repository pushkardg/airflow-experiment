"""
Thin wrapper around the Airflow stable REST API (v1) used by the benchmark
harness (paper Section 4.1/4.2). Deliberately dependency-light (requests only)
so it can run from a control host outside the Airflow cluster.

Requires an Airflow 2.x instance with the stable API enabled and basic-auth
(or a token) configured -- see Airflow docs: "Stable REST API".
"""
from __future__ import annotations
import time
import requests
from dataclasses import dataclass


@dataclass
class AirflowClient:
    base_url: str          # e.g. "http://airflow-webserver:8080"
    auth: tuple             # (username, password)
    timeout_sec: float = 30.0

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/api/v1{path}"

    def list_dags(self, limit: int = 100, offset: int = 0) -> dict:
        r = requests.get(self._url("/dags"), auth=self.auth,
                          params={"limit": limit, "offset": offset},
                          timeout=self.timeout_sec)
        r.raise_for_status()
        return r.json()

    def unpause_dag(self, dag_id: str) -> None:
        r = requests.patch(
            self._url(f"/dags/{dag_id}"),
            auth=self.auth, json={"is_paused": False}, timeout=self.timeout_sec,
        )
        r.raise_for_status()

    def trigger_dag(self, dag_id: str, run_id: str | None = None,
                     conf: dict | None = None) -> dict:
        payload = {}
        if run_id:
            payload["dag_run_id"] = run_id
        if conf:
            payload["conf"] = conf
        r = requests.post(
            self._url(f"/dags/{dag_id}/dagRuns"),
            auth=self.auth, json=payload, timeout=self.timeout_sec,
        )
        r.raise_for_status()
        return r.json()

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        r = requests.get(
            self._url(f"/dags/{dag_id}/dagRuns/{dag_run_id}"),
            auth=self.auth, timeout=self.timeout_sec,
        )
        r.raise_for_status()
        return r.json()

    def list_task_instances(self, dag_id: str, dag_run_id: str) -> dict:
        r = requests.get(
            self._url(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"),
            auth=self.auth, timeout=self.timeout_sec,
        )
        r.raise_for_status()
        return r.json()

    def wait_for_dag_run(self, dag_id: str, dag_run_id: str,
                          poll_interval_sec: float = 2.0,
                          timeout_sec: float = 3600.0) -> dict:
        """Poll until a DAG run reaches a terminal state (success/failed)."""
        deadline = time.monotonic() + timeout_sec
        terminal = {"success", "failed"}
        while time.monotonic() < deadline:
            run = self.get_dag_run(dag_id, dag_run_id)
            if run.get("state") in terminal:
                return run
            time.sleep(poll_interval_sec)
        raise TimeoutError(
            f"DAG run {dag_id}/{dag_run_id} did not reach a terminal state "
            f"within {timeout_sec}s"
        )
