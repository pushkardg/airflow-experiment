"""
Cross-system statistical analysis (paper Section 4.4): pairwise comparisons
across Airflow, Argo Workflows, and Kubeflow Pipelines, replacing the
single-system harness's mitigation-vs-baseline comparison with a
system-vs-system comparison.

For each (N, F, M) configuration, this produces the three pairwise
comparisons -- Airflow-vs-Argo, Airflow-vs-Kubeflow, Argo-vs-Kubeflow --
each with a bootstrap CI, a significance test, and a Cliff's delta effect
size, with Holm-Bonferroni correction applied across the three pairwise
tests per configuration (paper Section 4.4).

Reuses the same bootstrap_ci / cliffs_delta implementations already tested
in the single-system harness (analysis/analyze_results.py there) -- the
statistics don't change, only what's being compared does.
"""
from __future__ import annotations
import argparse
import glob
import itertools
import json
import os

import numpy as np
import pandas as pd
from scipy import stats


def bootstrap_ci(values: np.ndarray, stat_fn=np.median, n_boot: int = 5000,
                  ci: float = 0.95, seed: int = 42) -> tuple[float, float, float]:
    values = np.asarray(values)
    values = values[~np.isnan(values)]
    if len(values) == 0:
        return (float("nan"), float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    point = stat_fn(values)
    if len(values) == 1:
        return (float(point), float(point), float(point))
    boots = np.array([
        stat_fn(rng.choice(values, size=len(values), replace=True))
        for _ in range(n_boot)
    ])
    alpha = (1 - ci) / 2
    lo, hi = np.quantile(boots, [alpha, 1 - alpha])
    return (float(point), float(lo), float(hi))


def cliffs_delta(a: np.ndarray, b: np.ndarray) -> float:
    a, b = np.asarray(a), np.asarray(b)
    gt = sum(1 for x in a for y in b if x > y)
    lt = sum(1 for x in a for y in b if x < y)
    return (gt - lt) / (len(a) * len(b))


def mann_whitney_p(a: np.ndarray, b: np.ndarray) -> float:
    """Mann-Whitney U test p-value for a system-vs-system comparison --
    non-parametric, appropriate since latency distributions are not assumed
    normal (paper Section 4.4's general statistical-rigor stance)."""
    if len(a) < 2 or len(b) < 2:
        return float("nan")
    try:
        _, p = stats.mannwhitneyu(a, b, alternative="two-sided")
        return float(p)
    except ValueError:
        return float("nan")  # e.g. all-identical values in both samples


def holm_bonferroni_correct(p_values: list[float], alpha: float = 0.05) -> list[dict]:
    """Standard Holm-Bonferroni step-down correction across a family of
    p-values (paper Section 4.4's "corrected across the three pairwise
    tests per configuration")."""
    indexed = [(p, i) for i, p in enumerate(p_values) if not np.isnan(p)]
    indexed.sort(key=lambda t: t[0])
    m = len(indexed)
    results = [None] * len(p_values)
    for rank, (p, orig_idx) in enumerate(indexed):
        threshold = alpha / (m - rank)
        corrected = min(p * (m - rank), 1.0)
        results[orig_idx] = {"p_value": p, "p_corrected": corrected, "significant": p <= threshold}
    for i, r in enumerate(results):
        if r is None:
            results[i] = {"p_value": float("nan"), "p_corrected": float("nan"), "significant": None}
    return results


SYSTEM_PAIRS = [("airflow", "argo"), ("airflow", "kubeflow"), ("argo", "kubeflow")]


def load_raw_records(raw_dir: str) -> pd.DataFrame:
    """
    Expects one JSON file per completed run, each with:
        {"config": {"system": "airflow"|"argo"|"kubeflow", "n":.., "fan_out":..,
                     "short_ratio":.., "repeat_index":.., "is_warmup": bool},
         "metrics": {"submission_to_running_ms": float, "running_to_completion_ms": float}}
    matching common/metrics_schema.py's CommonRunTiming fields.
    """
    rows = []
    for path in glob.glob(os.path.join(raw_dir, "*.json")):
        with open(path) as f:
            rec = json.load(f)
        cfg = rec["config"]
        if cfg.get("is_warmup"):
            continue
        m = rec["metrics"]
        rows.append({
            "system": cfg["system"], "n": cfg["n"], "fan_out": cfg["fan_out"],
            "short_ratio": cfg["short_ratio"], "repeat_index": cfg["repeat_index"],
            "submission_to_running_ms": m.get("submission_to_running_ms"),
            "running_to_completion_ms": m.get("running_to_completion_ms"),
        })
    if not rows:
        raise FileNotFoundError(f"No raw run records found in {raw_dir}.")
    return pd.DataFrame(rows)


def pairwise_comparison(df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    results = []
    for (n, fan_out, short_ratio), group in df.groupby(["n", "fan_out", "short_ratio"]):
        by_system = {
            sysname: group[group["system"] == sysname][metric_col].dropna().values
            for sysname in ("airflow", "argo", "kubeflow")
        }
        pair_results = []
        p_values = []
        for sys_a, sys_b in SYSTEM_PAIRS:
            a, b = by_system[sys_a], by_system[sys_b]
            if len(a) < 2 or len(b) < 2:
                pair_results.append(None)
                p_values.append(float("nan"))
                continue
            pt_a, lo_a, hi_a = bootstrap_ci(a, np.median)
            pt_b, lo_b, hi_b = bootstrap_ci(b, np.median)
            delta = cliffs_delta(a, b)
            p = mann_whitney_p(a, b)
            pair_results.append({
                "sys_a": sys_a, "sys_b": sys_b,
                "median_a": pt_a, "ci_a": (lo_a, hi_a),
                "median_b": pt_b, "ci_b": (lo_b, hi_b),
                "cliffs_delta": delta,
            })
            p_values.append(p)

        corrected = holm_bonferroni_correct(p_values)
        for pair, corr in zip(pair_results, corrected):
            if pair is None:
                continue
            row = {"n": n, "fan_out": fan_out, "short_ratio": short_ratio, **pair, **corr}
            results.append(row)

    return pd.DataFrame(results)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args(argv)

    os.makedirs(args.out_dir, exist_ok=True)
    df = load_raw_records(args.raw_dir)

    for metric_col, out_name in [
        ("submission_to_running_ms", "pairwise_submission_to_running.csv"),
        ("running_to_completion_ms", "pairwise_running_to_completion.csv"),
    ]:
        result_df = pairwise_comparison(df, metric_col)
        result_df.to_csv(os.path.join(args.out_dir, out_name), index=False)
        print(f"Wrote {out_name} ({len(result_df)} pairwise comparisons)")


if __name__ == "__main__":
    main()
