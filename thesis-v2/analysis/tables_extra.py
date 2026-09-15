"""Additional frozen CSV tables introduced in the manuscript session.

* ``tab-e4-attempts-<config>.csv`` -- every E4 run of a config with BOTH
  throughput witnesses (committed lambda_leo vs accepted lambda_jmx),
* ``tab-calibration.csv``           -- E1 value vs value USED by the model,
* ``tab-mu-summary.csv``            -- mu per config with SD/CoV/n and witness ratio,
* ``tab-scalability-err.csv``       -- S, eff, rf_cost with propagated errors.

All writers always emit their file (possibly header-only), like tables.py.
"""
from __future__ import annotations

from loaders import CONFIGS
from style import write_csv
from tables import _row

E4_ATTEMPTS_COLUMNS = [
    "config", "label", "P", "rate", "acks", "key", "run", "lambda_leo", "lambda_jmx",
    "mbps_in", "ack_p50", "ack_p99", "e2e_p50", "failed", "lag_max", "cpu_busy_max",
    "nic_rx_max_mbps", "nic_tx_max_mbps", "disk_write_max_mbps",
]
E4_ATTEMPTS_DIGITS = {"P": 0, "rate": 0, "run": 0, "lambda_leo": 0, "lambda_jmx": 0,
                      "mbps_in": 1, "ack_p50": 1, "ack_p99": 1, "e2e_p50": 1, "failed": 0,
                      "lag_max": 0, "cpu_busy_max": 2, "nic_rx_max_mbps": 1,
                      "nic_tx_max_mbps": 1, "disk_write_max_mbps": 1}


def write_tab_e4_attempts(config: str, rows: list[dict], out_tables) -> None:
    body = [_row(r, E4_ATTEMPTS_COLUMNS, E4_ATTEMPTS_DIGITS) for r in rows]
    write_csv(f"tab-e4-attempts-{config}", E4_ATTEMPTS_COLUMNS, body, out_tables)


CALIBRATION_COLUMNS = ["config", "parameter", "e1_value", "used_value", "source", "reason"]
CALIBRATION_DIGITS = {"e1_value": 3, "used_value": 3}


def write_tab_calibration(rows: list[dict], out_tables) -> None:
    body = [_row(r, CALIBRATION_COLUMNS, CALIBRATION_DIGITS) for r in rows]
    write_csv("tab-calibration", CALIBRATION_COLUMNS, body, out_tables)


MU_SUMMARY_COLUMNS = [
    "config", "n", "mu_committed", "mu_committed_sd", "mu_committed_cov",
    "mu_accepted_jmx", "mu_accepted_jmx_sd", "witness_ratio", "mu_mbps",
    "acks1_n", "acks1_accepted_jmx", "acks1_committed_leo",
]
MU_SUMMARY_DIGITS = {"n": 0, "mu_committed": 0, "mu_committed_sd": 0, "mu_committed_cov": 3,
                     "mu_accepted_jmx": 0, "mu_accepted_jmx_sd": 0, "witness_ratio": 3,
                     "mu_mbps": 1, "acks1_n": 0, "acks1_accepted_jmx": 0,
                     "acks1_committed_leo": 0}


def write_tab_mu_summary(rows_by_config: dict, out_tables) -> None:
    body = []
    for cfg in CONFIGS:
        r = rows_by_config.get(cfg, {"config": cfg})
        body.append(_row(r, MU_SUMMARY_COLUMNS, MU_SUMMARY_DIGITS))
    for cfg, r in sorted(rows_by_config.items()):
        if cfg not in CONFIGS:
            body.append(_row(r, MU_SUMMARY_COLUMNS, MU_SUMMARY_DIGITS))
    write_csv("tab-mu-summary", MU_SUMMARY_COLUMNS, body, out_tables)


SCAL_ERR_COLUMNS = ["quantity", "value", "err_abs", "err_rel", "formula"]
SCAL_ERR_DIGITS = {"value": 3, "err_abs": 3, "err_rel": 3}


def write_tab_scalability_err(rows: list[dict], out_tables) -> None:
    body = [_row(r, SCAL_ERR_COLUMNS, SCAL_ERR_DIGITS) for r in rows]
    write_csv("tab-scalability-err", SCAL_ERR_COLUMNS, body, out_tables)
