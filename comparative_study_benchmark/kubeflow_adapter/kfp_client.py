"""
Wrapper around the real Kubeflow Pipelines SDK client (kfp.Client), not a
hand-rolled REST client -- unlike the Airflow and Argo adapters, KFP ships
an official Python client, so we use it directly rather than reimplementing
HTTP calls.

Method signatures below are verified against the installed kfp==2.17.0 SDK
(checked via inspect.signature during development, not assumed from memory):
    kfp.Client.create_run_from_pipeline_package(pipeline_file, arguments=None,
        run_name=None, experiment_name=None, namespace=None, ...) -> RunPipelineResult
    kfp.Client.get_run(run_id) -> V2beta1Run
    kfp.Client.wait_for_run_completion(run_id, timeout, sleep_duration=5) -> V2beta1Run
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class KubeflowClient:
    host: str               # e.g. "https://kubeflow.example.com/pipeline"
    namespace: str | None = None
    existing_token: str | None = None

    def _client(self):
        # Constructed lazily (not in __init__) so that simply instantiating
        # this dataclass -- e.g. for the harness's dry-run / config-sweep
        # planning, which never needs a live connection -- doesn't require
        # kfp.Client() to succeed against a real KFP deployment.
        import kfp
        kwargs = {"host": self.host}
        if self.namespace:
            kwargs["namespace"] = self.namespace
        if self.existing_token:
            kwargs["existing_token"] = self.existing_token
        return kfp.Client(**kwargs)

    def submit_run(self, pipeline_ir_path: str, run_name: str,
                    experiment_name: str = "benchmark") -> str:
        """Submits a compiled pipeline IR file (as produced by
        kubeflow_adapter/generate_pipeline.py --compile) and returns the run_id."""
        result = self._client().create_run_from_pipeline_package(
            pipeline_file=pipeline_ir_path,
            run_name=run_name,
            experiment_name=experiment_name,
        )
        return result.run_id

    def get_run(self, run_id: str):
        return self._client().get_run(run_id)

    def wait_for_completion(self, run_id: str, timeout_sec: int = 3600,
                             poll_interval_sec: int = 5):
        return self._client().wait_for_run_completion(
            run_id, timeout=timeout_sec, sleep_duration=poll_interval_sec
        )
