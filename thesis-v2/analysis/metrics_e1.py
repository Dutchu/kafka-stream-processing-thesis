"""E1 calibration metrics: tau_ack / tau_e2e / c_s^2 / c_a^2 per run and per config.

Split out of the former metrics.py (facade kept for imports).
"""
from __future__ import annotations

import math
import statistics

import kingman
from loaders import (
    CONFIGS, ack_in_window, series_in_window, dispersion,
)
from parsers import parse_error_types, format_error_types, prom_min_of_mean_by_label, \
    prom_max_of_mean_by_label, prom_mean_all, prom_fraction_above
from style import warn


def _col_mean(rows: list[dict], col: str) -> float:
    vals = [r[col] for r in rows if col in r and r[col] == r[col]]
    return sum(vals) / len(vals) if vals else math.nan


def _col_median(rows: list[dict], col: str) -> float:
    vals = [r[col] for r in rows if col in r and r[col] == r[col]]
    return dispersion(vals)["median"]


def _weighted_mean(rows: list[dict], val_col: str, weight_col: str) -> float:
    pairs = [(r[val_col], r[weight_col]) for r in rows
             if r.get(val_col) == r.get(val_col) and r.get(weight_col) == r.get(weight_col)
             and r.get(weight_col, 0) > 0]
    total_w = sum(w for _v, w in pairs)
    if total_w <= 0:
        return math.nan
    return sum(v * w for v, w in pairs) / total_w



def e1_per_run(run) -> dict:
    """One tab-e1-tau.csv row for a single E1 run.

    tau_ack_ms: PREFERRED source is run.json -> summary.ackP50 (architect
    decision: the dashboard computes this from the merged HdrHistogram of
    the whole run -- that IS the "scalony histogram" spec-analysis.md SS2
    refers to). Fallback (manifest/summary.ackP50 missing) = median of
    ack_p50_ms across the windowed ack.json rows.
    method in {"summary_hist_p50", "median_of_ack_p50"} records which
    source was used. tau_e2e_ms = median of e2e_p50_ms from the windowed
    series.
    cs2: ROBUST estimator (dashboard mirror since the 1b-rf1-esmall stalls):
    median over in-window ack.json buckets of ((p95-p50)/1.645), squared over
    summary.ackMean. Single multi-second stalls (cold start/metadata/ISR at
    +10/+14/+20 s) poison the whole-run histogram sd (cs2=432 vs ~0.01) while
    leaving bucket medians untouched; the median-of-buckets is immune.
    Needs >=3 usable buckets, else NaN with a warning. Legacy
    summary.cs2Est accepted as fallback; raw (ackSdMs/ackMean)^2 is kept in
    the manifest only for audit, never used for prediction.
    ca2: median of ca2_est in the window; when E1-level load is too thin to
    measure dispersion (bins average < 5 arrivals -> estimator NaN by
    construction), the documented Poisson fallback 1.0 is used (mirrors the
    dashboard from-run endpoint), with a warning.
    """
    ack_rows = ack_in_window(run)
    series_rows = series_in_window(run)

    summary = (run.manifest or {}).get("summary") or {}
    ack_p50_summary = summary.get("ackP50")

    if isinstance(ack_p50_summary, (int, float)):
        tau_ack_ms = float(ack_p50_summary)
        method = "summary_hist_p50"
        if ack_rows:
            ack_count = sum(r.get("acked", 0) for r in ack_rows if r.get("acked") == r.get("acked"))
        else:
            ack_count = math.nan
    elif ack_rows:
        tau_ack_ms = _col_median(ack_rows, "ack_p50_ms")
        method = "median_of_ack_p50"
        ack_count = sum(r.get("acked", 0) for r in ack_rows if r.get("acked") == r.get("acked"))
    else:
        warn(f"{run.path}: no manifest summary.ackP50 and no ack.json rows "
             f"in analysis window; tau_ack_ms = NaN")
        tau_ack_ms = math.nan
        method = "unavailable"
        ack_count = math.nan

    if series_rows:
        tau_e2e_ms = _col_median(series_rows, "e2e_p50_ms")
        e2e_count = sum(r.get("e2e_count", 0) for r in series_rows if r.get("e2e_count") == r.get("e2e_count"))
        ca2 = _col_median(series_rows, "ca2_est")
        if ca2 != ca2:
            warn(f"{run.path}: ca2_est unmeasurable in window; Poisson fallback ca2 = 1.0")
            ca2 = 1.0
    else:
        warn(f"{run.path}: no series.json rows in analysis window; tau_e2e_ms/ca2 = NaN")
        tau_e2e_ms = math.nan
        e2e_count = math.nan
        ca2 = math.nan

    cs2 = math.nan
    ack_mean = summary.get("ackMean")
    spreads = []
    for r in ack_rows:
        p50 = r.get("ack_p50_ms")
        p95 = r.get("ack_p95_ms")
        if (isinstance(p50, (int, float)) and isinstance(p95, (int, float))
                and p50 == p50 and p95 == p95 and p95 >= p50):
            spreads.append((float(p95) - float(p50)) / 1.645)
    if (isinstance(ack_mean, (int, float)) and ack_mean > 0 and len(spreads) >= 3):
        r = statistics.median(spreads) / float(ack_mean)
        cs2 = r * r
    elif summary.get("cs2Est") is not None:
        cs2 = float(summary.get("cs2Est"))
    else:
        warn(f"{run.path}: <3 usable ack buckets with p95 (or no ackMean); cs2 = NaN")

    return {
        "config": run.config,
        "run": run.run_index,
        "tau_ack_ms": tau_ack_ms,
        "tau_e2e_ms": tau_e2e_ms,
        "cs2": cs2,
        "ca2": ca2,
        "ack_count": ack_count,
        "e2e_count": e2e_count,
        "method": method,
    }


def e1_summary(config: str, per_run_rows: list[dict]) -> dict:
    """tab-e1-summary.csv row: median + CoV over the (<=3) E1 runs."""
    rows = [r for r in per_run_rows if r["config"] == config]
    tau_ack = dispersion([r["tau_ack_ms"] for r in rows])
    tau_e2e = dispersion([r["tau_e2e_ms"] for r in rows])
    cs2 = dispersion([r["cs2"] for r in rows])
    ca2 = dispersion([r["ca2"] for r in rows])
    if not rows:
        warn(f"E1 summary: no runs found for config={config}")
    return {
        "config": config,
        "tau_ack_med": tau_ack["median"],
        "tau_ack_cov": tau_ack["cov"],
        "tau_e2e_med": tau_e2e["median"],
        "tau_e2e_cov": tau_e2e["cov"],
        "cs2_med": cs2["median"],
        "ca2_med": ca2["median"],
    }
