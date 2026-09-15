"""E4 throughput *witnesses* and saturation *attempts* (any rung directory name).

Two independent throughput witnesses exist for every run:

* ``lambda_leo``  -- dashboard AdminClient: delta of summed ``listOffsets(latest)``,
  i.e. the *high watermark* (committed, replicated) throughput,
* ``lambda_jmx``  -- broker JMX ``MessagesInPerSec`` summed over brokers
  (``prom/prom_msgs_in_broker.json``), i.e. records *accepted by leaders*.

With ``acks=all`` the two agree (leaders wait for the ISR before answering);
with ``acks=1`` they diverge by the replication lag -- which is exactly what
the 3b-rf3 campaign showed (1.1 M accepted vs 0.12 M committed).

The 3b-rf3 campaign ended without a regular ``R<rate>`` ladder: the runs live in
directories named after the attempt (``P20r0``, ``P50r0``, ``p20r0ukey``,
``P100R0-ukey-acks1``, ``P100r100000``, ``r10000``).  ``discover_attempts`` walks
every ``results/E4/<config>/<label>/run<N>/`` directory regardless of the label
and ``attempt_row`` decodes the parameters from label + manifest.

Output rows feed ``tab-e4-attempts-<config>.csv`` (tables_extra.write_tab_e4_attempts).
"""
from __future__ import annotations

import math
import re
import statistics
import sys
from pathlib import Path

from loaders import RunData, analysis_window, load_run, series_in_window
from parsers import parse_prom_json
from style import warn

_RUN_RE = re.compile(r"^run(\d+)$")
_P_RE = re.compile(r"[Pp](\d+)")
_R_RE = re.compile(r"[Rr](\d+)")


def decode_label(label: str, manifest: dict) -> dict:
    """Attempt parameters: prefer the manifest, fall back to the label."""
    params = manifest.get("params") or {}
    p = params.get("parallelism", manifest.get("parallelism"))
    rate = params.get("ratePerSec", manifest.get("ratePerSec"))
    if p is None:
        m = _P_RE.search(label)
        p = int(m.group(1)) if m else math.nan
    if rate is None:
        m = _R_RE.search(label)
        rate = int(m.group(1)) if m else math.nan
    low = label.lower()
    return {
        "label": label,
        "P": p,
        "rate": rate,
        "acks": "1" if "acks1" in low else "all",
        "key": "unique" if "ukey" in low else "zoneId",
    }


def discover_attempts(results_root: Path, config: str) -> list[RunData]:
    """Every run directory under results/E4/<config>/*/run<N>/ (any label)."""
    base = results_root / "E4" / config
    runs: list[RunData] = []
    if not base.is_dir():
        return runs
    for label_dir in sorted(base.iterdir()):
        if not label_dir.is_dir():
            continue
        for run_dir in sorted(label_dir.iterdir()):
            m = _RUN_RE.match(run_dir.name)
            if not run_dir.is_dir() or not m:
                continue
            runs.append(load_run(run_dir, "E4", config, rung=label_dir.name,
                                 run_index=int(m.group(1))))
    return runs


def _prom_sum_over_labels_mean(path: Path, window) -> float:
    """Mean over time of the per-timestamp SUM across all series (e.g. sum of
    per-broker MessagesIn rates), restricted to the analysis window."""
    if not path or not path.is_file():
        return math.nan
    series = parse_prom_json(path)
    if not series:
        return math.nan
    start_s, stop_s = (window[0] / 1000.0, window[1] / 1000.0) if window else (-math.inf, math.inf)
    per_ts: dict[float, float] = {}
    for _labels, points in series:
        for ts, val in points:
            if start_s <= ts <= stop_s:
                per_ts[ts] = per_ts.get(ts, 0.0) + val
    if not per_ts:
        return math.nan
    return statistics.fmean(per_ts.values())


def _prom_max_over_labels(path: Path, window) -> float:
    """Max over time and series of a per-node gauge/rate (e.g. NIC rx)."""
    if not path or not path.is_file():
        return math.nan
    series = parse_prom_json(path)
    start_s, stop_s = (window[0] / 1000.0, window[1] / 1000.0) if window else (-math.inf, math.inf)
    best = math.nan
    for _labels, points in series:
        for ts, val in points:
            if start_s <= ts <= stop_s and (best != best or val > best):
                best = val
    return best


def lambda_jmx(run: RunData) -> float:
    """Broker-side throughput witness for any run (msg/s, mean in window)."""
    prom = run.prom_dir or Path("/nonexistent")
    return _prom_sum_over_labels_mean(prom / "prom_msgs_in_broker.json", analysis_window(run))


def attempt_row(run: RunData) -> dict:
    """One per-run row with both throughput witnesses and the saturation signals."""
    info = decode_label(str(run.rung), run.manifest)
    summ = run.manifest.get("summary") or {}
    win = analysis_window(run)
    rows = series_in_window(run)
    lam_vals = [r["lambda_leo"] for r in rows if r.get("lambda_leo", math.nan) == r.get("lambda_leo")]
    lambda_leo = statistics.fmean(lam_vals) if lam_vals else summ.get("lambdaMean", math.nan)
    prom = run.prom_dir or Path("/nonexistent")
    lambda_jmx = _prom_sum_over_labels_mean(prom / "prom_msgs_in_broker.json", win)
    bytes_in = _prom_sum_over_labels_mean(prom / "prom_bytes_in_broker.json", win)
    nic_rx = _prom_max_over_labels(prom / "prom_nic_rx.json", win)
    nic_tx = _prom_max_over_labels(prom / "prom_nic_tx.json", win)
    disk = _prom_max_over_labels(prom / "prom_disk_write.json", win)
    cpu = _prom_max_over_labels(prom / "prom_cpu_broker.json", win)
    return {
        "config": run.config,
        **info,
        "run": run.run_index,
        "lambda_leo": lambda_leo,
        "lambda_jmx": lambda_jmx,
        "mbps_in": bytes_in / 1e6 if bytes_in == bytes_in else math.nan,
        "ack_p50": summ.get("ackP50", math.nan),
        "ack_p99": summ.get("ackP99", math.nan),
        "e2e_p50": summ.get("e2eP50", math.nan),
        "failed": summ.get("failed", math.nan),
        "lag_max": summ.get("consumerLagMax", math.nan),
        "cpu_busy_max": cpu,
        "nic_rx_max_mbps": nic_rx / 1e6 if nic_rx == nic_rx else math.nan,
        "nic_tx_max_mbps": nic_tx / 1e6 if nic_tx == nic_tx else math.nan,
        "disk_write_max_mbps": disk / 1e6 if disk == disk else math.nan,
    }


def attempts_rows(results_root: Path, config: str) -> list[dict]:
    runs = discover_attempts(results_root, config)
    if not runs:
        warn(f"E4 attempts: no runs found for config={config}")
    return [attempt_row(r) for r in runs]


def _cli(argv):
    root = Path(argv[1]) if len(argv) > 1 else Path("results")
    config = argv[2] if len(argv) > 2 else "3b-rf3"
    for row in attempts_rows(root, config):
        print({k: (round(v, 1) if isinstance(v, float) else v) for k, v in row.items()})


if __name__ == "__main__":
    _cli(sys.argv)
