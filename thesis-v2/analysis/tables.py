"""Emission of every frozen CSV table (spec-analysis.md SS3) to
--out-tables. Every function here ALWAYS writes its file, even with zero
rows or all-NaN values (spec-analysis.md SS4: "wszystkie pliki z SS3
powstaja"), so the LaTeX workstream (W6) never hits a missing \\input.

Numeric formatting: spec-analysis.md SS5 says "2-3 miejsca dla ms, 3 dla
rho/wspolczynnikow" -- values are written with a bounded number of decimal
digits (rounded), NaN as an empty cell, matching csvsimple's expectations
(headers without spaces, ',' separator, '.' decimal point).
"""
from __future__ import annotations

import math

from loaders import CONFIGS, E2_RUNGS
from style import write_csv


def _fmt(v, digits=3):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return v
    if isinstance(v, int):
        return str(v)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return ""
    if f != f:
        return ""
    if math.isinf(f):
        return "inf" if f > 0 else "-inf"
    return f"{round(f, digits):.{digits}f}"


def _row(dct: dict, columns: list, digits_map: dict = None) -> list:
    digits_map = digits_map or {}
    out = []
    for c in columns:
        out.append(_fmt(dct.get(c), digits_map.get(c, 3)))
    return out



E1_TAU_COLUMNS = ["config", "run", "tau_ack_ms", "tau_e2e_ms", "cs2", "ca2",
                  "ack_count", "e2e_count", "method"]
E1_TAU_DIGITS = {"tau_ack_ms": 2, "tau_e2e_ms": 2, "cs2": 3, "ca2": 3,
                 "ack_count": 0, "e2e_count": 0}


def write_tab_e1_tau(rows: list[dict], out_tables) -> None:
    body = [_row(r, E1_TAU_COLUMNS, E1_TAU_DIGITS) for r in rows]
    write_csv("tab-e1-tau", E1_TAU_COLUMNS, body, out_tables)



OMB_PROFILE_COLUMNS = ["config", "mu_msgs", "mu_sd", "mu_cov", "mu_n",
                       "tau_ack_ms", "tau_ack_sd", "tau_e2e_ms", "tau_e2e_sd",
                       "payload_bytes", "method"]
OMB_PROFILE_DIGITS = {"mu_msgs": 0, "mu_sd": 0, "mu_n": 0, "tau_ack_ms": 2,
                      "tau_ack_sd": 2, "tau_e2e_ms": 2, "tau_e2e_sd": 2,
                      "payload_bytes": 0}


def write_tab_omb_profile(rows: list[dict], out_tables) -> None:
    body = [_row(r, OMB_PROFILE_COLUMNS, OMB_PROFILE_DIGITS) for r in rows]
    write_csv("tab-omb-profile", OMB_PROFILE_COLUMNS, body, out_tables)


E1_SUMMARY_COLUMNS = ["config", "tau_ack_med", "tau_ack_cov", "tau_e2e_med",
                      "tau_e2e_cov", "cs2_med", "ca2_med"]
E1_SUMMARY_DIGITS = {"tau_ack_med": 2, "tau_ack_cov": 3, "tau_e2e_med": 2,
                     "tau_e2e_cov": 3, "cs2_med": 3, "ca2_med": 3}


def write_tab_e1_summary(rows: list[dict], out_tables) -> None:
    all_rows = {r["config"]: r for r in rows}
    body = []
    for cfg in CONFIGS:
        r = all_rows.get(cfg, {"config": cfg})
        body.append(_row(r, E1_SUMMARY_COLUMNS, E1_SUMMARY_DIGITS))
    write_csv("tab-e1-summary", E1_SUMMARY_COLUMNS, body, out_tables)



E4_LADDER_COLUMNS = [
    "config", "P", "run", "lambda_mean", "lambda_jmx", "lambda_p50", "mbps", "ack_mean",
    "ack_p50", "ack_p99", "e2e_p50", "failed", "error_types",
    "handler_idle_min", "netproc_idle_min", "cpu_busy_max",
    "request_queue_mean", "nic_rx_max_mbps", "disk_write_max_mbps",
    "signature_fired", "binding_resource",
]
E4_LADDER_DIGITS = {
    "lambda_mean": 1, "lambda_jmx": 1, "lambda_p50": 1, "mbps": 3, "ack_mean": 2, "ack_p50": 2,
    "ack_p99": 2, "e2e_p50": 2, "failed": 0, "handler_idle_min": 3,
    "netproc_idle_min": 3, "cpu_busy_max": 3, "request_queue_mean": 2,
    "nic_rx_max_mbps": 3, "disk_write_max_mbps": 3,
}


def write_tab_e4_ladder(rows: list[dict], out_tables) -> None:
    body = [_row(r, E4_LADDER_COLUMNS, E4_LADDER_DIGITS) for r in rows]
    write_csv("tab-e4-ladder", E4_LADDER_COLUMNS, body, out_tables)


E4_MU_COLUMNS = ["config", "P_plateau", "mu_msgs", "mu_cov", "mu_mbps",
                 "ack_p50_plateau", "ack_p50_pmax", "p50_growth",
                 "binding_resource", "mu_status"]
E4_MU_DIGITS = {"P_plateau": 0, "mu_msgs": 1, "mu_cov": 3, "mu_mbps": 3,
                "ack_p50_plateau": 2, "ack_p50_pmax": 2, "p50_growth": 3}


def write_tab_e4_mu(rows: list[dict], out_tables) -> None:
    all_rows = {r["config"]: r for r in rows}
    body = []
    seen = set()
    for r in rows:
        body.append(_row(r, E4_MU_COLUMNS, E4_MU_DIGITS))
        seen.add(r["config"])
    for cfg in CONFIGS:
        if cfg not in seen:
            r = {"config": cfg, "mu_status": "client-limited",
                 "binding_resource": "none"}
            body.append(_row(r, E4_MU_COLUMNS, E4_MU_DIGITS))
    write_csv("tab-e4-mu", E4_MU_COLUMNS, body, out_tables)


E4R_LADDER_COLUMNS = [
    "config", "rate", "run", "lambda_mean", "lambda_jmx", "lambda_p50", "mbps", "ack_mean",
    "ack_p50", "ack_p99", "e2e_p50", "failed", "error_types",
    "handler_idle_min", "netproc_idle_min", "cpu_busy_max",
    "request_queue_mean", "nic_rx_max_mbps", "disk_write_max_mbps",
    "signature_fired", "binding_resource",
]
E4R_MU_COLUMNS = ["config", "rate_plateau", "mu_msgs", "mu_cov", "mu_mbps",
                  "ack_p50_plateau", "ack_p50_pmax", "p50_growth",
                  "binding_resource", "mu_status"]
E4R_MU_DIGITS = {"rate_plateau": 0, "mu_msgs": 1, "mu_cov": 3, "mu_mbps": 3,
                 "ack_p50_plateau": 2, "ack_p50_pmax": 2, "p50_growth": 3}


def write_tab_e4r_ladder(rows: list[dict], out_tables) -> None:
    body = [_row(r, E4R_LADDER_COLUMNS, E4_LADDER_DIGITS) for r in rows]
    write_csv("tab-e4r-ladder", E4R_LADDER_COLUMNS, body, out_tables)


def write_tab_e4r_mu(rows: list[dict], out_tables) -> None:
    body = [_row(r, E4R_MU_COLUMNS, E4R_MU_DIGITS) for r in rows]
    write_csv("tab-e4r-mu", E4R_MU_COLUMNS, body, out_tables)



SCALABILITY_COLUMNS = ["metric", "1b-rf1", "3b-rf1", "3b-rf3", "S_rf1",
                       "eff_rf1", "rf_cost", "S_rf3"]
SCALABILITY_DIGITS = {"1b-rf1": 3, "3b-rf1": 3, "3b-rf3": 3, "S_rf1": 3,
                      "eff_rf1": 3, "rf_cost": 3, "S_rf3": 3}
SCALABILITY_METRICS = ["mu_msgs", "mu_mbps", "tau_ack_ms", "tau_e2e_ms"]


def write_tab_scalability(rows: list[dict], out_tables) -> None:
    by_metric = {r["metric"]: r for r in rows}
    body = []
    for metric in SCALABILITY_METRICS:
        r = by_metric.get(metric, {"metric": metric})
        digits = 1 if metric in ("mu_msgs",) else (2 if "tau" in metric else 3)
        dmap = dict(SCALABILITY_DIGITS)
        for k in ("1b-rf1", "3b-rf1", "3b-rf3"):
            dmap[k] = digits
        body.append(_row(r, SCALABILITY_COLUMNS, dmap))
    write_csv("tab-scalability", SCALABILITY_COLUMNS, body, out_tables)



E2_LADDER_COLUMNS = [
    "rho_target", "run", "lambda_mean", "rho_real", "ack_mean", "ack_p50",
    "ack_p99", "e2e_mean", "e2e_p50", "e2e_p99", "wq_pred", "pred_ack",
    "pred_e2e", "err_ack", "err_ack_rel", "err_e2e", "err_e2e_rel",
    "consumer_limited", "lag_start", "backlog_contaminated",
]
E2_LADDER_DIGITS = {
    "lambda_mean": 1, "rho_real": 3, "ack_mean": 2, "ack_p50": 2,
    "ack_p99": 2, "e2e_mean": 2, "e2e_p50": 2, "e2e_p99": 2, "wq_pred": 2,
    "pred_ack": 2, "pred_e2e": 2, "err_ack": 2, "err_ack_rel": 3,
    "err_e2e": 2, "err_e2e_rel": 3, "lag_start": 0,
}


def write_tab_e2_ladder(config: str, rows: list[dict], out_tables) -> None:
    body = [_row(r, E2_LADDER_COLUMNS, E2_LADDER_DIGITS) for r in rows]
    write_csv(f"tab-e2-ladder-{config}", E2_LADDER_COLUMNS, body, out_tables)


E2_SUMMARY_COLUMNS = ["config", "rho_target", "rho_real_med", "ack_mean_med",
                      "pred_ack", "err_ack_med", "err_ack_rel_med",
                      "e2e_mean_med", "pred_e2e", "err_e2e_med",
                      "err_e2e_rel_med"]
E2_SUMMARY_DIGITS = {"rho_real_med": 3, "ack_mean_med": 2, "pred_ack": 2,
                     "err_ack_med": 2, "err_ack_rel_med": 3,
                     "e2e_mean_med": 2, "pred_e2e": 2, "err_e2e_med": 2,
                     "err_e2e_rel_med": 3}


def write_tab_e2_summary(rows: list[dict], out_tables) -> None:
    body = []
    by_key = {(r["config"], r["rho_target"]): r for r in rows}
    for cfg in CONFIGS:
        for rho in E2_RUNGS:
            r = by_key.get((cfg, rho), {"config": cfg, "rho_target": rho})
            body.append(_row(r, E2_SUMMARY_COLUMNS, E2_SUMMARY_DIGITS))
    write_csv("tab-e2-summary", E2_SUMMARY_COLUMNS, body, out_tables)


E2BP_LADDER_COLUMNS = ["config", "variant", "rho_target"] + E2_LADDER_COLUMNS[1:]
E2BP_SUMMARY_COLUMNS = ["config", "variant"] + E2_SUMMARY_COLUMNS[1:]


def write_tab_e2bp(ladder_rows: list[dict], summary_rows: list[dict], out_tables) -> None:
    """Backpressure-confirmation variants (rho90bp, ...): same shapes as the
    E2 tables plus a variant column; empty inputs still emit the files."""
    ladder_body = [_row(r, E2BP_LADDER_COLUMNS, E2_LADDER_DIGITS) for r in ladder_rows]
    write_csv("tab-e2bp-ladder", E2BP_LADDER_COLUMNS, ladder_body, out_tables)
    summary_body = [_row(r, E2BP_SUMMARY_COLUMNS, E2_SUMMARY_DIGITS) for r in summary_rows]
    write_csv("tab-e2bp-summary", E2BP_SUMMARY_COLUMNS, summary_body, out_tables)


PARTITION_USAGE_COLUMNS = ["topic", "partition", "broker", "size_bytes", "source"]
PARTITION_USAGE_DIGITS = {"size_bytes": 0}

PARTITION_IMBALANCE_COLUMNS = ["topic", "n_partitions", "min_bytes",
                               "max_bytes", "max_min_ratio", "cov"]
PARTITION_IMBALANCE_DIGITS = {"n_partitions": 0, "min_bytes": 0,
                              "max_bytes": 0, "max_min_ratio": 1}


def write_tab_partition_usage(detail: list[dict], summary: list[dict], out_tables) -> None:
    """One-time imbalance proof -> manuscript tables (spec-analysis.md)."""
    write_csv("tab-partition-usage",
              PARTITION_USAGE_COLUMNS,
              [_row(r, PARTITION_USAGE_COLUMNS, PARTITION_USAGE_DIGITS) for r in detail],
              out_tables)
    write_csv("tab-partition-imbalance",
              PARTITION_IMBALANCE_COLUMNS,
              [_row(r, PARTITION_IMBALANCE_COLUMNS, PARTITION_IMBALANCE_DIGITS) for r in summary],
              out_tables)



KINGMAN_ERROR_COLUMNS = ["config", "MAE_ack_ms", "MAPE_ack", "MAE_e2e_ms",
                         "MAPE_e2e", "best_rho", "worst_rho"]
KINGMAN_ERROR_DIGITS = {"MAE_ack_ms": 2, "MAPE_ack": 3, "MAE_e2e_ms": 2,
                        "MAPE_e2e": 3, "best_rho": 0, "worst_rho": 0}


def write_tab_kingman_error(rows: list[dict], out_tables) -> None:
    by_cfg = {r["config"]: r for r in rows}
    body = []
    for cfg in CONFIGS:
        r = by_cfg.get(cfg, {"config": cfg})
        body.append(_row(r, KINGMAN_ERROR_COLUMNS, KINGMAN_ERROR_DIGITS))
    write_csv("tab-kingman-error", KINGMAN_ERROR_COLUMNS, body, out_tables)



CENSUS_COLUMNS = ["exp", "config", "label", "runs_ok", "runs_failed",
                  "runs_consumer_limited", "total_msgs"]
CENSUS_DIGITS = {"runs_ok": 0, "runs_failed": 0, "runs_consumer_limited": 0,
                 "total_msgs": 0}


def write_tab_run_census(rows: list[dict], out_tables) -> None:
    body = [_row(r, CENSUS_COLUMNS, CENSUS_DIGITS) for r in rows]
    write_csv("tab-run-census", CENSUS_COLUMNS, body, out_tables)


TIME_BUDGET_COLUMNS = ["exp", "config", "runs", "wall_clock_min"]
TIME_BUDGET_DIGITS = {"runs": 0, "wall_clock_min": 1}


def write_tab_time_budget(rows: list[dict], out_tables) -> None:
    body = [_row(r, TIME_BUDGET_COLUMNS, TIME_BUDGET_DIGITS) for r in rows]
    write_csv("tab-time-budget", TIME_BUDGET_COLUMNS, body, out_tables)
