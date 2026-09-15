"""OMB reference campaign: client-bound evidence from the per-run Prometheus captures.

For every ``results/OMB-reference/<config-hw>/<rung>/run<N>/`` directory with a
``prom/`` folder, compute inside the run window (``run_window.env`` /
``window.env``: RUN_START/RUN_END or START_EPOCH_S/STOP_EPOCH_S):

* ``cpu_load_max``   -- max CPU busy fraction of the load-generator node (kafka-load),
* ``cpu_broker_max`` -- max CPU busy fraction over broker nodes (kafka-1..3),
* ``disk_write_max_mbps`` -- max per-node disk write [MB/s] over brokers,
* ``rps``            -- achieved records/s from the perf aggregate line(s) (sum for dual).

Output rows -> ``tab-omb-evidence.csv`` (tables_v1.write_tab_omb_evidence).  This is the
quantitative basis of the "single generator cannot saturate the cluster" argument
(client CPU pinned while brokers idle) and of the falsified 48 MB/s disk premise.
"""
from __future__ import annotations

import math
import re
from pathlib import Path

from omb_import import parse_perf_aggregate
from parsers import parse_env_file, parse_prom_json
from style import warn

_RUN_RE = re.compile(r"^run(\d+)$")


def _window(run_dir: Path):
    for name in ("run_window.env", "window.env"):
        env = parse_env_file(run_dir / name)
        if not env:
            continue
        start = env.get("RUN_START") or env.get("START_EPOCH_S")
        stop = env.get("RUN_END") or env.get("STOP_EPOCH_S")
        try:
            return float(start), float(stop)
        except (TypeError, ValueError):
            continue
    return None


def _max_by_node(path: Path, window, node_filter) -> float:
    if not path.is_file():
        return math.nan
    best = math.nan
    for labels, points in parse_prom_json(path):
        node = labels.get("node", labels.get("instance", ""))
        if not node_filter(node):
            continue
        for ts, val in points:
            if window and not (window[0] <= ts <= window[1]):
                continue
            if best != best or val > best:
                best = val
    return best


def _rps(run_dir: Path) -> float:
    """Sum of achieved records/s over producers.  Dual runs keep perf-a/perf-b
    (per producer) and perf.txt (their concatenation) -- count each producer once."""
    files = sorted(run_dir.glob("perf-[a-z].txt")) or [run_dir / "perf.txt"]
    total = 0.0
    found = False
    for f in files:
        if not f.is_file():
            continue
        agg = parse_perf_aggregate(f.read_text(encoding="utf-8", errors="ignore"))
        if agg:
            total += agg.get("records_per_sec", agg.get("rps", 0.0)) or 0.0
            found = True
    return total if found else math.nan


def omb_evidence_rows(results_root: Path) -> list[dict]:
    base = Path(results_root) / "OMB-reference"
    rows: list[dict] = []
    if not base.is_dir():
        warn("OMB evidence: OMB-reference/ missing")
        return rows
    for cfg_dir in sorted(p for p in base.iterdir() if p.is_dir()):
        for rung_dir in sorted(p for p in cfg_dir.iterdir() if p.is_dir()):
            for run_dir in sorted(rung_dir.iterdir()):
                m = _RUN_RE.match(run_dir.name)
                if not m or not (run_dir / "prom").is_dir():
                    continue
                win = _window(run_dir)
                prom = run_dir / "prom"
                cpu_file = prom / "prom_cpu.json" if (prom / "prom_cpu.json").is_file() else prom / "prom_cpu_broker.json"
                rows.append({
                    "config": cfg_dir.name,
                    "rung": rung_dir.name,
                    "run": int(m.group(1)),
                    "rps": _rps(run_dir),
                    "cpu_load_max": _max_by_node(cpu_file, win, lambda n: "load" in n),
                    "cpu_broker_max": _max_by_node(cpu_file, win, lambda n: re.match(r"kafka-\d", n) is not None),
                    "disk_write_max_mbps": _max_by_node(prom / "prom_disk_write.json", win,
                                                        lambda n: re.match(r"kafka-\d", n) is not None) / 1e6,
                })
    if not rows:
        warn("OMB evidence: no runs with prom/ found")
    return rows
