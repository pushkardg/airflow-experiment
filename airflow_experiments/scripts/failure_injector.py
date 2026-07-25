"""
FailureInjectorOperator
=======================
Custom PythonOperator wrapper used in all retry experiments.

Supports three failure modes:
  - probabilistic: Bernoulli draw against failure_rate each invocation
  - count: fail on the n-th invocation and recover
  - timed: fail after elapsed_seconds, then recover

Exception types:
  - AirflowException       => retry is triggered (transient failure)
  - AirflowFailException   => immediate fail, no retry consumed (permanent)

All experiments use seed=42 for reproducibility.
"""

import random
import time
import logging
from typing import Optional

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException, AirflowFailException

log = logging.getLogger(__name__)


class FailureInjectorOperator(BaseOperator):
    """
    Injects configurable failures into an Airflow task for experiment purposes.

    Parameters
    ----------
    failure_mode : str
        One of 'probabilistic', 'count', or 'timed'.
    failure_rate : float
        For probabilistic mode: probability of failure per invocation (0.0–1.0).
    recovery_after : int, optional
        For count mode: recover after this many invocations.
    elapsed_fail_after : float, optional
        For timed mode: fail after this many seconds have elapsed.
    permanent : bool
        If True, raise AirflowFailException (no retry) instead of AirflowException.
    task_duration_min : float
        Minimum simulated task duration in seconds (uniform draw).
    task_duration_max : float
        Maximum simulated task duration in seconds (uniform draw).
    seed_offset : int
        Added to base seed (42) to vary per-task randomness while staying reproducible.
    """

    def __init__(
        self,
        failure_mode: str = "probabilistic",
        failure_rate: float = 0.10,
        recovery_after: Optional[int] = None,
        elapsed_fail_after: Optional[float] = None,
        permanent: bool = False,
        task_duration_min: float = 10.0,
        task_duration_max: float = 120.0,
        seed_offset: int = 0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.failure_mode = failure_mode
        self.failure_rate = failure_rate
        self.recovery_after = recovery_after
        self.elapsed_fail_after = elapsed_fail_after
        self.permanent = permanent
        self.task_duration_min = task_duration_min
        self.task_duration_max = task_duration_max
        self.seed_offset = seed_offset

    def execute(self, context):
        ti = context["task_instance"]
        try_number = ti.try_number
        run_id = context["run_id"]

        # Deterministic seed: base + task offset + try number
        rng = random.Random(42 + self.seed_offset + try_number)

        # Simulate task work
        duration = rng.uniform(self.task_duration_min, self.task_duration_max)
        log.info(f"Task running for {duration:.1f}s (try {try_number}, run {run_id})")

        start = time.monotonic()
        time.sleep(duration)
        elapsed = time.monotonic() - start

        # Determine whether to fail
        should_fail = self._should_fail(rng, try_number, elapsed)

        if should_fail:
            exc_class = AirflowFailException if self.permanent else AirflowException
            msg = f"{'Permanent' if self.permanent else 'Transient'} failure injected"
            log.warning(msg)
            raise exc_class(msg)

        log.info(f"Task succeeded after {elapsed:.1f}s")
        return {"duration": duration, "try_number": try_number}

    def _should_fail(self, rng, try_number: int, elapsed: float) -> bool:
        if self.failure_mode == "probabilistic":
            return rng.random() < self.failure_rate

        elif self.failure_mode == "count":
            if self.recovery_after is None:
                return False
            return try_number <= self.recovery_after

        elif self.failure_mode == "timed":
            if self.elapsed_fail_after is None:
                return False
            return elapsed >= self.elapsed_fail_after

        return False


def make_jitter_callback(base_seconds: int = 60, max_seconds: int = 300):
    """
    Returns an on_retry_callback that implements full-jitter exponential backoff.

    This is used instead of Airflow's native retry_exponential_backoff=True,
    which applies a deterministic multiplier without randomization.

    delay = random.uniform(0, min(base * 2^attempt, max_seconds))
    """

    def jitter_callback(context):
        ti = context["task_instance"]
        attempt = ti.try_number
        rng = random.Random(42 + attempt)
        cap = min(base_seconds * (2 ** attempt), max_seconds)
        delay = rng.uniform(0, cap)
        log.info(f"Full-jitter backoff: sleeping {delay:.1f}s (attempt {attempt})")
        time.sleep(delay)

    return jitter_callback
