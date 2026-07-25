"""
Statistical Analysis
====================
Reads raw experiment results and produces the statistical tables from the paper.

Outputs:
  - Table 1: Retry strategy comparison (recovery time, SCI, std dev)
  - Table 2: Alerting MTTA comparison
  - Console: Mann-Whitney U p-values and Cohen's d for all comparisons

Usage:
    python scripts/analyze_results.py
"""

import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.experiment_config import RESULTS_DIR, HUMAN_POLLING_INTERVAL, ALPHA


def cohen_d(a, b):
    """Compute Cohen's d effect size."""
    na, nb = len(a), len(b)
    pooled_std = np.sqrt(((na - 1) * np.std(a, ddof=1)**2 +
                          (nb - 1) * np.std(b, ddof=1)**2) / (na + nb - 2))
    if pooled_std == 0:
        return 0.0
    return (np.mean(a) - np.mean(b)) / pooled_std


def ci_95(data):
    """95% confidence interval for the mean."""
    n = len(data)
    if n < 2:
        return (np.nan, np.nan)
    se = stats.sem(data)
    h = se * stats.t.ppf(0.975, n - 1)
    m = np.mean(data)
    return (m - h, m + h)


def mann_whitney(a, b):
    """Mann-Whitney U test, returns (statistic, p-value)."""
    if len(a) < 2 or len(b) < 2:
        return (np.nan, np.nan)
    return stats.mannwhitneyu(a, b, alternative="two-sided")


def load_csv(filename):
    path = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def analyze_retry_section():
    print("\n" + "=" * 70)
    print("TABLE 1: Retry Strategy Comparison")
    print("=" * 70)

    # Load raw results
    fixed_single = load_csv("retry_fixed_single.csv")
    nojitter_single = load_csv("retry_nojitter_single.csv")
    jitter_single = load_csv("retry_jitter_single.csv")
    exception_aware = load_csv("retry_exception_aware.csv")

    fixed_conc = load_csv("retry_fixed_concurrent.csv")
    jitter_conc = load_csv("retry_jitter_concurrent.csv")

    sci_results = load_csv("sci_results.csv")

    # ── Recovery time ──
    for label, df in [
        ("Fixed-interval", fixed_single),
        ("Exp. backoff (no jitter)", nojitter_single),
        ("Exp. backoff + full jitter", jitter_single),
        ("Exception-aware (50/50 mix)", exception_aware),
    ]:
        if df.empty or "elapsed_min" not in df.columns:
            print(f"  {label}: NO DATA")
            continue
        data = df["elapsed_min"].dropna().values
        if len(data) == 0:
            continue
        lo, hi = ci_95(data)
        print(f"\n  {label}:")
        print(f"    n={len(data)}, mean={np.mean(data):.1f}min, "
              f"σ={np.std(data, ddof=1):.1f}, 95%CI=[{lo:.1f},{hi:.1f}]")

    # ── SCI comparison (jitter vs fixed) ──
    if not sci_results.empty and "dag_id" in sci_results.columns:
        sci_fixed = sci_results[sci_results["dag_id"].str.contains("fixed")]["sci"].dropna()
        sci_jitter = sci_results[sci_results["dag_id"].str.contains("jitter")]["sci"].dropna()

        if len(sci_fixed) > 1 and len(sci_jitter) > 1:
            u_stat, p_val = mann_whitney(sci_fixed.values, sci_jitter.values)
            d = cohen_d(sci_fixed.values, sci_jitter.values)
            pct_reduction = (1 - sci_jitter.mean() / sci_fixed.mean()) * 100 if sci_fixed.mean() > 0 else 0

            print(f"\n  SCI comparison (concurrent failure):")
            print(f"    Fixed: mean={sci_fixed.mean():.1f}, σ={sci_fixed.std(ddof=1):.1f}")
            print(f"    Jitter: mean={sci_jitter.mean():.1f}, σ={sci_jitter.std(ddof=1):.1f}")
            print(f"    Reduction: {pct_reduction:.0f}%")
            print(f"    Mann-Whitney U: p={p_val:.4f}, Cohen's d={d:.2f}")
            if p_val < ALPHA:
                print(f"    ** Significant at α={ALPHA} **")

    # ── Exception-aware vs fixed (wasted time) ──
    if not exception_aware.empty and not fixed_single.empty:
        # Wasted time = time spent on tasks that raised AirflowFailException
        # Approximated here as: recovery_time[fixed] - recovery_time[exception_aware]
        fixed_data = fixed_single["elapsed_min"].dropna().values
        exc_data = exception_aware["elapsed_min"].dropna().values
        if len(fixed_data) > 1 and len(exc_data) > 1:
            u_stat, p_val = mann_whitney(exc_data, fixed_data)
            d = cohen_d(exc_data, fixed_data)
            pct_reduction = (1 - exc_data.mean() / fixed_data.mean()) * 100 if fixed_data.mean() > 0 else 0
            print(f"\n  Exception-aware vs fixed (wasted compute proxy):")
            print(f"    Reduction in elapsed time: {pct_reduction:.0f}%")
            print(f"    p={p_val:.4f}, Cohen's d={d:.2f}")


def analyze_sla_section():
    print("\n" + "=" * 70)
    print("SECTION 5.1: SLA Detection Latency")
    print("=" * 70)

    # SLA detection raw events
    sla_raw = load_csv("sla_detection_raw.csv")
    sla_runs = load_csv("sla_detection_runs.csv")

    if not sla_raw.empty:
        # Parse detection timestamps and compute latency
        # The task ran 45s, SLA was 30s, so detection latency = callback_time - (run_start + 30s)
        # Simplified: use elapsed between task completion and callback
        print(f"  SLA detection raw events: {len(sla_raw)}")

    if not sla_runs.empty:
        elapsed = sla_runs["elapsed_min"].dropna().values
        if len(elapsed) > 0:
            print(f"  n={len(elapsed)} trials")
            print(f"  Mean elapsed (incl. scheduler polling): {np.mean(elapsed)*60:.1f}s")
            print(f"  σ={np.std(elapsed, ddof=1)*60:.1f}s")

    # False positive rates
    print("\n  False-positive rates by SLA threshold multiplier:")
    for mult in ["1_0", "1_2", "1_5"]:
        fp_df = load_csv(f"sla_fp_{mult}x.csv")
        if not fp_df.empty:
            n_failed = (fp_df["state"] == "failed").sum()
            n_total = len(fp_df)
            fp_rate = n_failed / n_total * 100 if n_total > 0 else 0
            label = mult.replace("_", ".")
            print(f"    {label}x: FP rate = {fp_rate:.1f}% (n={n_total})")


def analyze_alerting_section():
    print("\n" + "=" * 70)
    print("TABLE 2: Alerting MTTA Comparison")
    print("=" * 70)
    print(f"  (Simulated model: human polling interval = {HUMAN_POLLING_INTERVAL}s)")

    mtta_data = {}
    for channel in ["email", "slack", "pagerduty"]:
        df = load_csv(f"alerting_{channel}.csv")
        if df.empty or "elapsed_min" not in df.columns:
            continue

        # MTTA = delivery_latency + human_polling_interval
        # elapsed_min already includes the simulated delivery latency
        # Add the polling interval to get MTTA
        delivery_min = df["elapsed_min"].dropna().values
        mtta_min = delivery_min + (HUMAN_POLLING_INTERVAL / 60.0)
        mtta_data[channel] = mtta_min

        lo, hi = ci_95(mtta_min)
        print(f"\n  {channel.upper()}:")
        print(f"    n={len(mtta_min)}, MTTA={np.mean(mtta_min):.1f}min, "
              f"σ={np.std(mtta_min, ddof=1):.1f}, 95%CI=[{lo:.1f},{hi:.1f}]")

    # Compare all channels vs email baseline
    if "email" in mtta_data and len(mtta_data["email"]) > 1:
        email_mtta = mtta_data["email"]
        for channel in ["slack", "pagerduty"]:
            if channel in mtta_data and len(mtta_data[channel]) > 1:
                ch_mtta = mtta_data[channel]
                u_stat, p_val = mann_whitney(ch_mtta, email_mtta)
                d = cohen_d(ch_mtta, email_mtta)
                pct_reduction = (1 - ch_mtta.mean() / email_mtta.mean()) * 100
                print(f"\n  {channel} vs email:")
                print(f"    MTTA reduction: {pct_reduction:.0f}%")
                print(f"    Mann-Whitney U: p={p_val:.4f}, Cohen's d={abs(d):.2f}")


def check_data_availability():
    print("\n" + "=" * 70)
    print("DATA AVAILABILITY CHECK")
    print("=" * 70)

    files = [
        ("retry_fixed_single.csv", "Section 4.2 - Fixed interval single"),
        ("retry_nojitter_single.csv", "Section 4.3 - No-jitter backoff"),
        ("retry_jitter_single.csv", "Section 4.3 - Full-jitter backoff"),
        ("retry_exception_aware.csv", "Section 4.4 - Exception-aware"),
        ("retry_fixed_concurrent.csv", "Section 4.2 - Fixed concurrent"),
        ("retry_jitter_concurrent.csv", "Section 4.3 - Jitter concurrent"),
        ("sci_results.csv", "SCI measurements"),
        ("sla_detection_raw.csv", "Section 5.1 - SLA detection raw"),
        ("alerting_email.csv", "Section 6 - Email MTTA"),
        ("alerting_slack.csv", "Section 6 - Slack MTTA"),
        ("alerting_pagerduty.csv", "Section 6 - PagerDuty MTTA"),
    ]

    for filename, label in files:
        path = os.path.join(RESULTS_DIR, filename)
        if os.path.exists(path):
            df = pd.read_csv(path)
            print(f"  [OK] {label}: {len(df)} rows")
        else:
            print(f"  [MISSING] {label}: {filename}")


if __name__ == "__main__":
    check_data_availability()
    analyze_retry_section()
    analyze_sla_section()
    analyze_alerting_section()
    print("\n" + "=" * 70)
    print("Analysis complete. Update paper tables with values above.")
    print("=" * 70)
