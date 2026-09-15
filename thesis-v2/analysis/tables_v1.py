"""CSV tables of the v1 supplement (first campaign, 3x e2-standard-4).

* ``tab-v1-e1.csv``         -- perf-test baseline runs (tau of v1),
* ``tab-v1-e4.csv``         -- two-producer perf-test saturation attempts,
* ``tab-v1-e2-ladder.csv``  -- OMB ladder per run with v1 Kingman prediction,
* ``tab-v1-e2-summary.csv`` -- medians per rung,
* ``tab-e2-error-v1-v2.csv`` -- prediction error v1 (OMB) vs v2 (functions).
"""
from __future__ import annotations

from style import write_csv
from tables import _row

V1_E1_COLUMNS = ["run", "rps", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms", "source"]
V1_E1_DIGITS = {"rps": 2, "avg_ms": 2, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "max_ms": 0}

V1_E4_COLUMNS = ["run", "producer", "rps", "mbps", "avg_ms", "p50_ms", "p99_ms", "max_ms", "source"]
V1_E4_DIGITS = {"rps": 0, "mbps": 2, "avg_ms": 2, "p50_ms": 0, "p99_ms": 0, "max_ms": 0}

V1_E2_LADDER_COLUMNS = ["rung", "run", "publish_rate", "mbps", "rho_real", "avg_ms", "p50_ms",
                        "p99_ms", "pred_ms", "err_ms", "err_rel", "source"]
V1_E2_LADDER_DIGITS = {"rung": 0, "publish_rate": 0, "mbps": 2, "rho_real": 3, "avg_ms": 2,
                       "p50_ms": 2, "p99_ms": 2, "pred_ms": 2, "err_ms": 2, "err_rel": 3}

V1_E2_SUMMARY_COLUMNS = ["rung", "n", "rho_real_med", "avg_ms_med", "avg_ms_cov", "pred_ms",
                         "err_ms_med", "err_rel_med"]
V1_E2_SUMMARY_DIGITS = {"rung": 0, "n": 0, "rho_real_med": 3, "avg_ms_med": 2, "avg_ms_cov": 3,
                        "pred_ms": 2, "err_ms_med": 2, "err_rel_med": 3}

E2_ERR_CMP_COLUMNS = ["source", "rho_target", "rho_real", "obs_ms", "pred_ms", "err_rel"]
E2_ERR_CMP_DIGITS = {"rho_target": 0, "rho_real": 3, "obs_ms": 2, "pred_ms": 2, "err_rel": 3}


def write_tab_v1_e1(rows, out_tables) -> None:
    write_csv("tab-v1-e1", V1_E1_COLUMNS, [_row(r, V1_E1_COLUMNS, V1_E1_DIGITS) for r in rows], out_tables)


def write_tab_v1_e4(rows, out_tables) -> None:
    write_csv("tab-v1-e4", V1_E4_COLUMNS, [_row(r, V1_E4_COLUMNS, V1_E4_DIGITS) for r in rows], out_tables)


def write_tab_v1_e2(ladder, summary, out_tables) -> None:
    write_csv("tab-v1-e2-ladder", V1_E2_LADDER_COLUMNS,
              [_row(r, V1_E2_LADDER_COLUMNS, V1_E2_LADDER_DIGITS) for r in ladder], out_tables)
    write_csv("tab-v1-e2-summary", V1_E2_SUMMARY_COLUMNS,
              [_row(r, V1_E2_SUMMARY_COLUMNS, V1_E2_SUMMARY_DIGITS) for r in summary], out_tables)


OMB_EVIDENCE_COLUMNS = ["config", "rung", "run", "rps", "cpu_load_max", "cpu_broker_max",
                        "disk_write_max_mbps"]
OMB_EVIDENCE_DIGITS = {"run": 0, "rps": 0, "cpu_load_max": 3, "cpu_broker_max": 3,
                       "disk_write_max_mbps": 1}


def write_tab_omb_evidence(rows, out_tables) -> None:
    write_csv("tab-omb-evidence", OMB_EVIDENCE_COLUMNS,
              [_row(r, OMB_EVIDENCE_COLUMNS, OMB_EVIDENCE_DIGITS) for r in rows], out_tables)


def write_tab_e2_error_cmp(rows, out_tables) -> None:
    write_csv("tab-e2-error-v1-v2", E2_ERR_CMP_COLUMNS,
              [_row(r, E2_ERR_CMP_COLUMNS, E2_ERR_CMP_DIGITS) for r in rows], out_tables)
