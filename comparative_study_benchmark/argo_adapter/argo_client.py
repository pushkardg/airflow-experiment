"""
Thin wrapper around the Argo Server REST API, mirroring the Airflow harness's
harness/airflow_client.py in structure and in what's left unimplemented.
"""
from __future__ import annotations
import time
import requests
from dataclasses import dataclass


@dataclass
class ArgoClient:
    base_url: str          # e.g. "https://argo-server.example.com:2746"
    namespace: str
    bearer_token: str | None = None
    timeout_sec: float = 30.0

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.bearer_token:
            h["Authorization"] = f"Bearer {self.bearer_token}"
        return h

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/api/v1{path}"

    def submit_workflow(self, manifest: dict) -> dict:
        r = requests.post(
            self._url(f"/workflows/{self.namespace}"),
            headers=self._headers(), json={"workflow": manifest},
            timeout=self.timeout_sec,
        )
        r.raise_for_status()
        return r.json()

    def get_workflow(self, name: str) -> dict:
        r = requests.get(
            self._url(f"/workflows/{self.namespace}/{name}"),
            headers=self._headers(), timeout=self.timeout_sec,
        )
        r.raise_for_status()
        return r.json()

    def wait_for_completion(self, name: str, poll_interval_sec: float = 2.0,
                             timeout_sec: float = 3600.0) -> dict:
        deadline = time.monotonic() + timeout_sec
        terminal = {"Succeeded", "Failed", "Error"}
        while time.monotonic() < deadline:
            wf = self.get_workflow(name)
            phase = wf.get("status", {}).get("phase")
            if phase in terminal:
                return wf
            time.sleep(poll_interval_sec)
        raise TimeoutError(f"Argo workflow {self.namespace}/{name} did not "
                            f"reach a terminal phase within {timeout_sec}s")
