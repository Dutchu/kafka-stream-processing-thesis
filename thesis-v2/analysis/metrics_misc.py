"""Scalability rows, run census, time budget, partition imbalance.

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


def scalability_rows(mu_by_config: dict, tau_ack_by_config: dict,
                      tau_e2e_by_config: dict) -> list[dict]:
    """tab-scalability.csv rows (metric x config, + derived S/eff/rf_cost).

    mu_by_config: {config: {"mu_msgs": .., "mu_mbps": ..}} (from tab-e4-mu).
    tau_*_by_config: {config: value} (from tab-e1-summary medians).
    Missing configs -> NaN with a warning (partial-data tolerance).
    """
    def g(d, cfg, key=None):
        v = d.get(cfg)
        if v is None:
            warn(f"scalability: config={cfg} missing for metric derivation")
            return math.nan
        return v.get(key, math.nan) if key is not None else v

    mu_1b = g(mu_by_config, "1b-rf1", "mu_msgs")
    mu_3b_rf1 = g(mu_by_config, "3b-rf1", "mu_msgs")
    mu_3b_rf3 = g(mu_by_config, "3b-rf3", "mu_msgs")
    mbps_1b = g(mu_by_config, "1b-rf1", "mu_mbps")
    mbps_3b_rf1 = g(mu_by_config, "3b-rf1", "mu_mbps")
    mbps_3b_rf3 = g(mu_by_config, "3b-rf3", "mu_mbps")

    tau_ack_1b = tau_ack_by_config.get("1b-rf1", math.nan)
    tau_ack_3b1 = tau_ack_by_config.get("3b-rf1", math.nan)
    tau_ack_3b3 = tau_ack_by_config.get("3b-rf3", math.nan)
    tau_e2e_1b = tau_e2e_by_config.get("1b-rf1", math.nan)
    tau_e2e_3b1 = tau_e2e_by_config.get("3b-rf1", math.nan)
    tau_e2e_3b3 = tau_e2e_by_config.get("3b-rf3", math.nan)

    def s_rf1(v3, v1):
        return v3 / v1 if v3 == v3 and v1 == v1 and v1 != 0 else math.nan

    def eff(s):
        return s / 3.0 if s == s else math.nan

    def rf_cost(v_rf3, v_rf1):
        return v_rf3 / v_rf1 if v_rf3 == v_rf3 and v_rf1 == v_rf1 and v_rf1 != 0 else math.nan

    rows = []
    for metric, v1, v_rf1, v_rf3 in (
        ("mu_msgs", mu_1b, mu_3b_rf1, mu_3b_rf3),
        ("mu_mbps", mbps_1b, mbps_3b_rf1, mbps_3b_rf3),
        ("tau_ack_ms", tau_ack_1b, tau_ack_3b1, tau_ack_3b3),
        ("tau_e2e_ms", tau_e2e_1b, tau_e2e_3b1, tau_e2e_3b3),
    ):
        s = s_rf1(v_rf1, v1)
        rows.append({
            "metric": metric,
            "1b-rf1": v1,
            "3b-rf1": v_rf1,
            "3b-rf3": v_rf3,
            "S_rf1": s,
            "eff_rf1": eff(s),
            "rf_cost": rf_cost(v_rf3, v_rf1),
            "S_rf3": s_rf1(v_rf3, v1),
        })
    return rows



def run_census_row(exp: str, config: str, label: str, runs: list) -> dict:
    """tab-run-census.csv row: counts of ok/failed/consumer-limited runs
    and total messages processed, for one (exp, config, label) group.
    label is "" for E1, "P<P>" for E4, "rho<R>" for E2."""
    runs_ok = 0
    runs_failed = 0
    runs_consumer_limited = 0
    total_msgs = 0.0
    for r in runs:
        summary = (r.manifest or {}).get("summary") or {}
        status = (r.manifest or {}).get("status")
        if not r.complete or status == "FAILED" or status == "aborted":
            runs_failed += 1
        else:
            runs_ok += 1
        if summary.get("consumerLimited"):
            runs_consumer_limited += 1
        sent = summary.get("sent")
        if isinstance(sent, (int, float)):
            total_msgs += sent
    return {
        "exp": exp,
        "config": config,
        "label": label,
        "runs_ok": runs_ok,
        "runs_failed": runs_failed,
        "runs_consumer_limited": runs_consumer_limited,
        "total_msgs": total_msgs,
    }


def time_budget_row(exp: str, config: str, runs: list) -> dict:
    """tab-time-budget.csv row: total wall-clock minutes spent, derived
    from manifest start/stop epochs (spec-analysis.md: "z epok manifestow")."""
    total_s = 0.0
    n = 0
    for r in runs:
        start = (r.manifest or {}).get("startEpochMs")
        stop = (r.manifest or {}).get("stopEpochMs")
        if isinstance(start, (int, float)) and isinstance(stop, (int, float)) and stop >= start:
            total_s += (stop - start) / 1000.0
            n += 1
    if n == 0:
        warn(f"time-budget: no valid epochs for exp={exp} config={config}")
    return {
        "exp": exp,
        "config": config,
        "runs": n,
        "wall_clock_min": total_s / 60.0 if n else math.nan,
    }



def partition_imbalance(detail_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Splits logdirs/prom detail rows into (detail, summary).

    Detail row: {topic, partition, broker, size_bytes, source}. Summary row
    per topic: n partitions, min/max bytes, max/min ratio (inf when min is
    0 with nonzero max — the hot-partition signature), CoV. NaN sizes
    dropped; topics with no finite sizes skipped with a warning.
    """
    by_topic: dict = {}
    for r in detail_rows:
        try:
            size = float(r.get("size_bytes", math.nan))
        except (TypeError, ValueError):
            size = math.nan
        if size != size:
            continue
        by_topic.setdefault(str(r.get("topic", "?")), []).append(
            {"topic": str(r.get("topic", "?")), "partition": str(r.get("partition", "?")),
             "broker": str(r.get("broker", "?")), "size_bytes": size,
             "source": str(r.get("source", "?"))})
    detail, summary = [], []
    for topic in sorted(by_topic):
        rows = by_topic[topic]
        detail.extend(rows)
        sizes = [r["size_bytes"] for r in rows]
        d = dispersion(sizes)
        lo, hi = d["min"], d["max"]
        if lo == lo and hi == hi:
            ratio = math.inf if lo == 0 and hi > 0 else (hi / lo if lo > 0 else math.nan)
        else:
            ratio = math.nan
        summary.append({"topic": topic, "n_partitions": len(rows),
                        "min_bytes": lo, "max_bytes": hi,
                        "max_min_ratio": ratio, "cov": d["cov"]})
    if not summary:
        warn("partition-imbalance: no finite partition sizes found")
    return detail, summary
