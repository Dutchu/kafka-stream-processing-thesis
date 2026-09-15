"""E2 rho-ladder metrics: per-run Kingman prediction/error, rung summary, MAE/MAPE.

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


def e2_per_run(run, mu_msgs: float, tau_ack: float, tau_e2e: float,
               ca2: float, cs2: float) -> dict:
    """One tab-e2-ladder-<config>.csv row.

    mu_msgs/tau_ack/tau_e2e/ca2/cs2 come from this config's E4/E1 results
    (independent computation from the dashboard's own live prediction,
    per spec-analysis.md SS2: "pipeline liczy niezaleznie od dashboardu").

    Backlog gate: a run starting with consumer_lag > 100k measures drain
    of its predecessors (transient), not steady-state — its e2e_* columns
    stay populated (audit) but e2_summary/kingman-error exclude its e2e
    (backlog_contaminated=true). ACK/lambda are producer-side, unaffected.
    """
    series_rows = series_in_window(run)
    ack_rows = ack_in_window(run)

    lag_start = math.nan
    if series_rows:
        try:
            lag_start = float(series_rows[0].get("consumer_lag", math.nan))
        except (TypeError, ValueError):
            lag_start = math.nan
    backlog_contaminated = lag_start == lag_start and lag_start > 100_000
    if backlog_contaminated:
        warn(f"{run.path}: starts with lag={lag_start:.0f} (>100k) — e2e measures drain, excluded from model validation")

    if series_rows:
        lambda_mean = _col_mean(series_rows, "lambda_leo")
        e2e_mean = _col_mean(series_rows, "e2e_mean_ms")
        e2e_p50 = _col_median(series_rows, "e2e_p50_ms")
        e2e_p99 = _col_median(series_rows, "e2e_p99_ms")
    else:
        warn(f"{run.path}: no series.json rows in window; E2 lambda/e2e = NaN")
        lambda_mean = e2e_mean = e2e_p50 = e2e_p99 = math.nan

    if ack_rows:
        ack_mean = _weighted_mean(ack_rows, "ack_mean_ms", "acked")
        ack_p50 = _col_median(ack_rows, "ack_p50_ms")
        ack_p99 = _col_median(ack_rows, "ack_p99_ms")
    else:
        warn(f"{run.path}: no ack.json rows in window; E2 ack stats = NaN")
        ack_mean = ack_p50 = ack_p99 = math.nan

    rho_real = (lambda_mean / mu_msgs
                if mu_msgs == mu_msgs and mu_msgs != 0 and lambda_mean == lambda_mean
                else math.nan)

    wq_pred = kingman.wq(rho_real, ca2, cs2, tau_ack) if rho_real == rho_real else math.nan
    pred_ack = kingman.pred_latency(tau_ack, rho_real, ca2, cs2, tau_ack) if rho_real == rho_real else math.nan
    pred_e2e = kingman.pred_latency(tau_e2e, rho_real, ca2, cs2, tau_ack) if rho_real == rho_real else math.nan
    err_ack, err_ack_rel = kingman.error(ack_mean, pred_ack)
    err_e2e, err_e2e_rel = kingman.error(e2e_mean, pred_e2e)

    summary = (run.manifest or {}).get("summary") or {}
    consumer_limited = summary.get("consumerLimited")
    if consumer_limited is None:
        warn(f"{run.path}: manifest summary.consumerLimited missing")

    return {
        "rho_target": run.rung,
        "run": run.run_index,
        "lambda_mean": lambda_mean,
        "rho_real": rho_real,
        "ack_mean": ack_mean,
        "ack_p50": ack_p50,
        "ack_p99": ack_p99,
        "e2e_mean": e2e_mean,
        "e2e_p50": e2e_p50,
        "e2e_p99": e2e_p99,
        "wq_pred": wq_pred,
        "pred_ack": pred_ack,
        "pred_e2e": pred_e2e,
        "err_ack": err_ack,
        "err_ack_rel": err_ack_rel,
        "err_e2e": err_e2e,
        "err_e2e_rel": err_e2e_rel,
        "consumer_limited": bool(consumer_limited) if consumer_limited is not None else None,
        "lag_start": lag_start,
        "backlog_contaminated": backlog_contaminated,
    }


def e2_summary(config: str, rho_target: int, rows: list[dict]) -> dict:
    """tab-e2-summary.csv row: medians over the (<=3) runs of one rung.

    e2e_* medians use only backlog-clean runs; when every run started in
    backlog, e2e stays NaN (no steady-state evidence) while ack stays.
    """
    if not rows:
        warn(f"E2 summary: no runs for config={config} rho={rho_target}")
    clean = [r for r in rows if not r.get("backlog_contaminated")]
    e2e_rows = clean if clean else []
    if rows and not e2e_rows:
        warn(f"E2 summary: config={config} rho={rho_target}: all runs backlog-contaminated — e2e excluded from validation")
    rho_real = dispersion([r["rho_real"] for r in rows])["median"]
    ack_mean = dispersion([r["ack_mean"] for r in rows])["median"]
    pred_ack = dispersion([r["pred_ack"] for r in rows])["median"]
    err_ack = dispersion([r["err_ack"] for r in rows])["median"]
    err_ack_rel = dispersion([r["err_ack_rel"] for r in rows])["median"]
    e2e_mean = dispersion([r["e2e_mean"] for r in e2e_rows])["median"]
    pred_e2e = dispersion([r["pred_e2e"] for r in e2e_rows])["median"]
    err_e2e = dispersion([r["err_e2e"] for r in e2e_rows])["median"]
    err_e2e_rel = dispersion([r["err_e2e_rel"] for r in e2e_rows])["median"]
    return {
        "config": config,
        "rho_target": rho_target,
        "rho_real_med": rho_real,
        "ack_mean_med": ack_mean,
        "pred_ack": pred_ack,
        "err_ack_med": err_ack,
        "err_ack_rel_med": err_ack_rel,
        "e2e_mean_med": e2e_mean,
        "pred_e2e": pred_e2e,
        "err_e2e_med": err_e2e,
        "err_e2e_rel_med": err_e2e_rel,
    }


def kingman_error_row(config: str, all_rungs_rows: dict) -> dict:
    """tab-kingman-error.csv row: MAE/MAPE over the 4 rungs (medians of
    each rung's per-run values), plus best/worst rho by |err_ack_rel|.

    all_rungs_rows: {rho_target: [per-run e2_per_run dict, ...]}.
    """
    rung_medians = {}
    for rho_target, rows in all_rungs_rows.items():
        if not rows:
            continue
        rung_medians[rho_target] = {
            "err_ack": dispersion([r["err_ack"] for r in rows])["median"],
            "err_ack_rel": dispersion([r["err_ack_rel"] for r in rows])["median"],
            "err_e2e": dispersion([r["err_e2e"] for r in rows])["median"],
            "err_e2e_rel": dispersion([r["err_e2e_rel"] for r in rows])["median"],
        }
    if not rung_medians:
        warn(f"kingman-error: no E2 rungs with data for config={config}")
        return {
            "config": config, "MAE_ack_ms": math.nan, "MAPE_ack": math.nan,
            "MAE_e2e_ms": math.nan, "MAPE_e2e": math.nan,
            "best_rho": math.nan, "worst_rho": math.nan,
        }

    ack_abs = [abs(v["err_ack"]) for v in rung_medians.values() if v["err_ack"] == v["err_ack"]]
    ack_rel = [abs(v["err_ack_rel"]) for v in rung_medians.values() if v["err_ack_rel"] == v["err_ack_rel"]]
    e2e_abs = [abs(v["err_e2e"]) for v in rung_medians.values() if v["err_e2e"] == v["err_e2e"]]
    e2e_rel = [abs(v["err_e2e_rel"]) for v in rung_medians.values() if v["err_e2e_rel"] == v["err_e2e_rel"]]

    mae_ack = sum(ack_abs) / len(ack_abs) if ack_abs else math.nan
    mape_ack = sum(ack_rel) / len(ack_rel) if ack_rel else math.nan
    mae_e2e = sum(e2e_abs) / len(e2e_abs) if e2e_abs else math.nan
    mape_e2e = sum(e2e_rel) / len(e2e_rel) if e2e_rel else math.nan

    ranked = sorted(
        ((rho, v["err_ack_rel"]) for rho, v in rung_medians.items() if v["err_ack_rel"] == v["err_ack_rel"]),
        key=lambda t: abs(t[1]),
    )
    best_rho = ranked[0][0] if ranked else math.nan
    worst_rho = ranked[-1][0] if ranked else math.nan

    return {
        "config": config,
        "MAE_ack_ms": mae_ack,
        "MAPE_ack": mape_ack,
        "MAE_e2e_ms": mae_e2e,
        "MAPE_e2e": mape_e2e,
        "best_rho": best_rho,
        "worst_rho": worst_rho,
    }
