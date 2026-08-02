"""
Common metrics schema across Airflow, Argo Workflows, and Kubeflow Pipelines
(paper Section 4.3).

The central methodological problem this module addresses: "scheduling
latency" is not the same architectural quantity in all three systems
(paper Section 2). This module defines the two measurement points that ARE
comparable across all three, because they're defined from OUTSIDE each
system's internals (via each system's own REST API), not from
system-specific instrumentation:

    submission_to_running_ms  -- time from workflow/DAG-run submission to
                                  the first task's container entering the
                                  Running state.
    running_to_completion_ms  -- time from first container Running to the
                                  whole workflow/DAG-run reaching a terminal
                                  state (success/failed).

It also defines the Kubernetes-attributable vs. orchestrator-attributable
decomposition described in Section 4.3, for the two systems (Argo, Kubeflow)
that run on Kubernetes natively, and for Airflow when using KubernetesExecutor.

This module intentionally raises NotImplementedError at each system's actual
network boundary (the REST/API calls), same convention as the Airflow-only
harness's metrics/collect_metrics.py -- the timestamp *extraction* logic
(what field means what, in each system's response schema) is fully
implemented and tested against realistic fixture payloads; only the live
HTTP calls are stubbed.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _parse_ts(ts: str | None) -> datetime | None:
    if ts is None:
        return None
    # Handle both airflow/argo/kfp's common ISO-8601 'Z' suffix and offset forms.
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


@dataclass
class CommonRunTiming:
    system: str                    # "airflow" | "argo" | "kubeflow"
    run_id: str
    submitted_at: datetime | None
    first_task_running_at: datetime | None
    completed_at: datetime | None
    final_state: str | None

    @property
    def submission_to_running_ms(self) -> float | None:
        if self.submitted_at is None or self.first_task_running_at is None:
            return None
        return (self.first_task_running_at - self.submitted_at).total_seconds() * 1000.0

    @property
    def running_to_completion_ms(self) -> float | None:
        if self.first_task_running_at is None or self.completed_at is None:
            return None
        return (self.completed_at - self.first_task_running_at).total_seconds() * 1000.0

    @property
    def total_ms(self) -> float | None:
        if self.submitted_at is None or self.completed_at is None:
            return None
        return (self.completed_at - self.submitted_at).total_seconds() * 1000.0


# ----------------------------------------------------------------------
# Per-system extraction: turns each system's native REST response shape
# into a CommonRunTiming. These are pure functions over already-fetched
# JSON, so they are fully unit-testable without a live cluster (see the
# accompanying tests) -- only the HTTP calls that produce that JSON are
# left for the harness to wire in against a live system.
# ----------------------------------------------------------------------

def from_airflow_dag_run(dag_run: dict, task_instances: list[dict]) -> CommonRunTiming:
    """
    dag_run: response body of GET /dags/{dag_id}/dagRuns/{dag_run_id}
    task_instances: response body's 'task_instances' list from
        GET /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances
    """
    submitted_at = _parse_ts(dag_run.get("queued_at") or dag_run.get("start_date"))
    start_dates = [
        _parse_ts(ti.get("start_date")) for ti in task_instances if ti.get("start_date")
    ]
    first_running = min(start_dates) if start_dates else None
    completed_at = _parse_ts(dag_run.get("end_date"))
    return CommonRunTiming(
        system="airflow", run_id=dag_run.get("dag_run_id", ""),
        submitted_at=submitted_at, first_task_running_at=first_running,
        completed_at=completed_at, final_state=dag_run.get("state"),
    )


def from_argo_workflow_status(workflow: dict) -> CommonRunTiming:
    """
    workflow: response body of GET /api/v1/workflows/{namespace}/{name}
    (Argo Server REST API), specifically its .status and .metadata fields.
    """
    meta = workflow.get("metadata", {})
    status = workflow.get("status", {})
    submitted_at = _parse_ts(meta.get("creationTimestamp"))
    completed_at = _parse_ts(status.get("finishedAt"))

    # First node to reach Running, across status.nodes (a dict keyed by node ID).
    node_start_times = []
    for node in (status.get("nodes") or {}).values():
        if node.get("phase") in ("Running", "Succeeded", "Failed") and node.get("startedAt"):
            node_start_times.append(_parse_ts(node["startedAt"]))
    first_running = min(node_start_times) if node_start_times else None

    return CommonRunTiming(
        system="argo", run_id=meta.get("name", ""),
        submitted_at=submitted_at, first_task_running_at=first_running,
        completed_at=completed_at, final_state=status.get("phase"),
    )


def from_kubeflow_run(run: dict) -> CommonRunTiming:
    """
    run: response body of GET /apis/v2beta1/runs/{run_id} (KFP API server),
    specifically its .created_at / .scheduled_at / .finished_at / .state
    and .run_details.task_details list.
    """
    submitted_at = _parse_ts(run.get("created_at"))
    completed_at = _parse_ts(run.get("finished_at"))

    task_details = (run.get("run_details") or {}).get("task_details") or []
    start_times = [
        _parse_ts(t.get("start_time")) for t in task_details if t.get("start_time")
    ]
    first_running = min(start_times) if start_times else None

    return CommonRunTiming(
        system="kubeflow", run_id=run.get("run_id", ""),
        submitted_at=submitted_at, first_task_running_at=first_running,
        completed_at=completed_at, final_state=run.get("state"),
    )


# ----------------------------------------------------------------------
# Kubernetes-attributable vs. orchestrator-attributable decomposition
# (Section 4.3). Requires Kubernetes API server metrics (Prometheus)
# alongside the per-run timing above; the decomposition itself is just a
# subtraction once both are collected, so it's expressed as a small
# pure function rather than requiring its own network calls here.
# ----------------------------------------------------------------------

def decompose_overhead(total_submission_to_running_ms: float,
                        k8s_pod_scheduling_ms: float) -> dict[str, float]:
    """
    total_submission_to_running_ms: from CommonRunTiming.submission_to_running_ms
    k8s_pod_scheduling_ms: Kubernetes-attributable portion, e.g. from
        Prometheus's apiserver/scheduler pod-scheduling-latency metrics
        for the pod(s) involved in this run (paper Section 3.5's shared-
        substrate point).

    Returns the orchestrator-attributable remainder alongside the input
    values, so callers get an explicit accounting rather than a bare
    subtraction result they have to interpret themselves.
    """
    orchestrator_ms = max(0.0, total_submission_to_running_ms - k8s_pod_scheduling_ms)
    return {
        "total_ms": total_submission_to_running_ms,
        "k8s_attributable_ms": k8s_pod_scheduling_ms,
        "orchestrator_attributable_ms": orchestrator_ms,
    }
