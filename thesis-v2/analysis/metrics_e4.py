"""E4 saturation metrics: broker signature, per-run ladder rows, plateau mu.

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



NIC_LIMIT_BPS = 8e9 / 8.0
DISK_LIMIT_BPS = 48.0 * 1e6
REQUEST_QUEUE_DURATION_THRESHOLD = 0.5


def _binding_resource(handler_idle_min, netproc_idle_min, cpu_busy_max,
                       request_queue_frac_above_zero, nic_rx_max, disk_write_max):
    """First threshold exceeded, in the fixed priority order of
    experiment-plan.md SS5.4 (i -> ii -> iii -> iv); 'none' if none fired."""
    if handler_idle_min == handler_idle_min and handler_idle_min < 0.1:
        return "handler"
    if netproc_idle_min == netproc_idle_min and netproc_idle_min < 0.1:
        return "network_processor"
    if cpu_busy_max == cpu_busy_max and cpu_busy_max > 0.9:
        return "cpu"
    if (request_queue_frac_above_zero == request_queue_frac_above_zero
            and request_queue_frac_above_zero > REQUEST_QUEUE_DURATION_THRESHOLD):
        return "request_queue"
    if nic_rx_max == nic_rx_max and nic_rx_max >= 0.8 * NIC_LIMIT_BPS:
        return "nic"
    if disk_write_max == disk_write_max and disk_write_max >= 0.8 * DISK_LIMIT_BPS:
        return "disk"
    return "none"


def e4_signature(run) -> dict:
    """Saturation-signature fields from prom/*.json for one E4 run
    (spec-analysis.md SS2 E4; experiment-plan.md SS5.4).

    Missing prom/ dir -> every field NaN, signature_fired=False,
    binding_resource="none" (client-limited candidate), with a warning.
    """
    if run.prom_dir is None or not run.prom_dir.is_dir():
        warn(f"{run.path}: prom/ missing; saturation signature unavailable")
        return {
            "handler_idle_min": math.nan, "netproc_idle_min": math.nan,
            "cpu_busy_max": math.nan, "request_queue_mean": math.nan,
            "nic_rx_max_bps": math.nan, "disk_write_max_bps": math.nan,
            "signature_fired": False, "binding_resource": "none",
        }

    def p(name):
        f = run.prom_dir / name
        return f if f.exists() else None

    handler_idle_min = prom_min_of_mean_by_label(p("prom_handler_idle.json"), "broker") \
        if p("prom_handler_idle.json") else math.nan
    netproc_idle_min = prom_min_of_mean_by_label(p("prom_netproc_idle.json"), "broker") \
        if p("prom_netproc_idle.json") else math.nan
    cpu_busy_max = prom_max_of_mean_by_label(p("prom_cpu_broker.json"), "node") \
        if p("prom_cpu_broker.json") else math.nan
    request_queue_mean = prom_mean_all(p("prom_request_queue.json")) \
        if p("prom_request_queue.json") else math.nan
    request_queue_frac = prom_fraction_above(p("prom_request_queue.json"), 0.0) \
        if p("prom_request_queue.json") else math.nan
    nic_rx_max_bps = prom_max_of_mean_by_label(p("prom_nic_rx.json"), "node") \
        if p("prom_nic_rx.json") else math.nan
    disk_write_max_bps = prom_max_of_mean_by_label(p("prom_disk_write.json"), "node") \
        if p("prom_disk_write.json") else math.nan

    binding = _binding_resource(handler_idle_min, netproc_idle_min, cpu_busy_max,
                                 request_queue_frac, nic_rx_max_bps, disk_write_max_bps)
    fired = binding != "none"

    return {
        "handler_idle_min": handler_idle_min,
        "netproc_idle_min": netproc_idle_min,
        "cpu_busy_max": cpu_busy_max,
        "request_queue_mean": request_queue_mean,
        "nic_rx_max_bps": nic_rx_max_bps,
        "disk_write_max_bps": disk_write_max_bps,
        "signature_fired": fired,
        "binding_resource": binding,
    }


def e4_per_run(run) -> dict:
    """One tab-e4-ladder.csv row for a single E4 run at a given P."""
    series_rows = series_in_window(run)
    ack_rows = ack_in_window(run)

    if series_rows:
        lambda_mean = _col_mean(series_rows, "lambda_leo")
        lambda_p50 = dispersion([r["lambda_leo"] for r in series_rows])["median"]
        bytes_rate_mean = _col_mean(series_rows, "bytes_rate")
        e2e_p50 = _col_median(series_rows, "e2e_p50_ms")
    else:
        warn(f"{run.path}: no series.json rows in window; E4 lambda/e2e = NaN")
        lambda_mean = lambda_p50 = bytes_rate_mean = e2e_p50 = math.nan

    if ack_rows:
        ack_mean = _weighted_mean(ack_rows, "ack_mean_ms", "acked")
        ack_p50 = _col_median(ack_rows, "ack_p50_ms")
        ack_p99 = _col_median(ack_rows, "ack_p99_ms")
        failed = sum(r.get("failed", 0) for r in ack_rows if r.get("failed") == r.get("failed"))
        error_counts: dict = {}
        for r in ack_rows:
            for k, v in parse_error_types(r.get("errors")).items():
                error_counts[k] = error_counts.get(k, 0) + v
        error_types = format_error_types(error_counts)
    else:
        warn(f"{run.path}: no ack.json rows in window; E4 ack stats = NaN")
        ack_mean = ack_p50 = ack_p99 = math.nan
        failed = math.nan
        error_types = ""

    mbps = bytes_rate_mean / 1e6 if bytes_rate_mean == bytes_rate_mean else math.nan

    sig = e4_signature(run)

    return {
        "config": run.config,
        "P": run.rung,
        "run": run.run_index,
        "parallelism": (run.manifest or {}).get("parallelism", math.nan),
        "lambda_mean": lambda_mean,
        "lambda_p50": lambda_p50,
        "mbps": mbps,
        "ack_mean": ack_mean,
        "ack_p50": ack_p50,
        "ack_p99": ack_p99,
        "e2e_p50": e2e_p50,
        "failed": failed,
        "error_types": error_types,
        "handler_idle_min": sig["handler_idle_min"],
        "netproc_idle_min": sig["netproc_idle_min"],
        "cpu_busy_max": sig["cpu_busy_max"],
        "request_queue_mean": sig["request_queue_mean"],
        "nic_rx_max_mbps": sig["nic_rx_max_bps"] / 1e6 if sig["nic_rx_max_bps"] == sig["nic_rx_max_bps"] else math.nan,
        "disk_write_max_mbps": sig["disk_write_max_bps"] / 1e6 if sig["disk_write_max_bps"] == sig["disk_write_max_bps"] else math.nan,
        "signature_fired": sig["signature_fired"],
        "binding_resource": sig["binding_resource"],
    }


def e4_plateau(config: str, ladder_rows_by_p: dict) -> dict:
    """tab-e4-mu.csv row: plateau detection over ascending P.

    ladder_rows_by_p: {P: [per-run ladder-row dict, ...]} for this config
    (each row is one e4_per_run() output). Per spec-analysis.md SS2:
    mu_plateau = smallest P after which the median lambda_mean does not
    grow by > 5% for any subsequent P; mu_msgs = that median; mu_status =
    "client-limited" when the saturation signature never fired at the
    plateau P.
    """
    ps = sorted(p for p in ladder_rows_by_p if ladder_rows_by_p[p])
    if not ps:
        warn(f"E4 plateau: no P rungs with data for config={config}")
        return {
            "config": config, "P_plateau": math.nan, "mu_msgs": math.nan,
            "mu_cov": math.nan, "mu_mbps": math.nan,
            "ack_p50_plateau": math.nan, "ack_p50_pmax": math.nan,
            "p50_growth": math.nan, "binding_resource": "none",
            "mu_status": "client-limited",
        }

    medians = {}
    for p in ps:
        rows = ladder_rows_by_p[p]
        medians[p] = dispersion([r.get("lambda_mean", math.nan) for r in rows])["median"]

    plateau_p = ps[-1]
    for i, p in enumerate(ps):
        if medians[p] != medians[p]:
            continue
        is_plateau = True
        for q in ps[i + 1:]:
            prev = medians[p]
            nxt = medians[q]
            if prev == prev and nxt == nxt and prev > 0 and (nxt - prev) / prev > 0.05:
                is_plateau = False
                break
        if is_plateau:
            plateau_p = p
            break

    plateau_rows = ladder_rows_by_p[plateau_p]
    mu_disp = dispersion([r.get("lambda_mean", math.nan) for r in plateau_rows])
    mu_msgs = mu_disp["median"]
    mu_cov = mu_disp["cov"]
    mu_mbps = dispersion([r.get("mbps", math.nan) for r in plateau_rows])["median"]
    ack_p50_plateau = dispersion([r.get("ack_p50", math.nan) for r in plateau_rows])["median"]

    p_max = ps[-1]
    max_rows = ladder_rows_by_p[p_max]
    ack_p50_pmax = dispersion([r.get("ack_p50", math.nan) for r in max_rows])["median"]
    p50_growth = (ack_p50_pmax / ack_p50_plateau
                  if ack_p50_plateau == ack_p50_plateau and ack_p50_plateau != 0
                  else math.nan)

    fired_at_plateau = any(bool(r.get("signature_fired")) for r in plateau_rows)
    binding_resources = [r.get("binding_resource") for r in plateau_rows
                         if r.get("binding_resource") not in (None, "none")]
    binding_resource = binding_resources[0] if binding_resources else "none"
    mu_status = "ok" if fired_at_plateau else "client-limited"
    if mu_status == "client-limited":
        warn(f"E4 plateau config={config}: saturation signature did not fire "
             f"at plateau P={plateau_p}; mu_status=client-limited")

    return {
        "config": config,
        "P_plateau": plateau_p,
        "mu_msgs": mu_msgs,
        "mu_cov": mu_cov,
        "mu_mbps": mu_mbps,
        "ack_p50_plateau": ack_p50_plateau,
        "ack_p50_pmax": ack_p50_pmax,
        "p50_growth": p50_growth,
        "binding_resource": binding_resource,
        "mu_status": mu_status,
    }
