"""Import of the *first-campaign* (v1) measurements: 3x e2-standard-4 brokers.

The v1 campaign (single load-generator VM: kafka-producer-perf-test and the
OpenMessaging Benchmark) is the road that led to the v2 method.  The
manuscript uses three of its data sets as a supplement -- never re-analysed
from the old pipeline, always re-parsed here so every number has a v2 source:

* E1  -- ``results/baseline_<N>.txt``        (perf-test, 5 msg/s, tau of v1),
* E4  -- ``results/heavy_load_<all|1>_v<N>.txt`` (2 parallel perf-test producers,
         unlimited rate; the single-VM "saturation" that was client-bound),
* E2  -- ``results/E2/<rung>/run<N>/omb.json`` (OMB rate ladder 25/50/75/90 % of
         the nominal 120 MB/s; 1 KiB records).

The v1 Kingman constants are reproduced VERBATIM from ``C:/thesis/analysis/theory.py``
(tau=6 ms, mu=98 MB/s, c_a^2=1.0, c_s^2=0.5) so the v1 prediction error is the
one the first manuscript reported, not a re-fit.
"""
from __future__ import annotations

import json
import math
import re
from pathlib import Path

from kingman import pred_latency
from loaders import dispersion
from style import warn

V1_TAU_MS = 6.0
V1_MU_MBPS = 98.0
V1_CA2 = 1.0
V1_CS2 = 0.5
V1_RECORD_BYTES = 1024
V1_RUNGS = (25, 50, 75, 90)

_AGG_RE = re.compile(
    r"(?P<records>[\d,]+) records sent, (?P<rps>[\d.]+) records/sec \((?P<mbps>[\d.]+) MB/sec\), "
    r"(?P<avg>[\d.]+) ms avg latency, (?P<max>[\d.]+) ms max latency, "
    r"(?P<p50>\d+) ms 50th, (?P<p95>\d+) ms 95th, (?P<p99>\d+) ms 99th, (?P<p999>\d+) ms 99\.9th"
)


def parse_perf_aggregates(text: str) -> list[dict]:
    """All final aggregate lines of kafka-producer-perf-test output."""
    out = []
    for m in _AGG_RE.finditer(text):
        out.append({
            "records": int(m.group("records").replace(",", "")),
            "rps": float(m.group("rps")),
            "mbps": float(m.group("mbps")),
            "avg_ms": float(m.group("avg")),
            "max_ms": float(m.group("max")),
            "p50_ms": float(m.group("p50")),
            "p95_ms": float(m.group("p95")),
            "p99_ms": float(m.group("p99")),
            "p999_ms": float(m.group("p999")),
        })
    return out


def v1_e1_rows(v1_root: Path) -> list[dict]:
    rows = []
    for path in sorted(Path(v1_root).glob("baseline_*.txt")):
        aggs = parse_perf_aggregates(path.read_text(encoding="utf-8", errors="ignore"))
        if not aggs:
            warn(f"v1 E1: no aggregate line in {path.name}")
            continue
        a = aggs[-1]
        rows.append({"run": path.stem.split("_")[-1], "rps": a["rps"], "avg_ms": a["avg_ms"],
                     "p50_ms": a["p50_ms"], "p95_ms": a["p95_ms"], "p99_ms": a["p99_ms"],
                     "max_ms": a["max_ms"], "source": path.name})
    return rows


def v1_e4_rows(v1_root: Path) -> list[dict]:
    """One row per producer per heavy_load_all_v<N>.txt (two parallel producers)."""
    rows = []
    for path in sorted(Path(v1_root).glob("heavy_load_all_v*.txt")):
        aggs = parse_perf_aggregates(path.read_text(encoding="utf-8", errors="ignore"))
        if not aggs:
            warn(f"v1 E4: no aggregate line in {path.name}")
            continue
        run = path.stem.split("_v")[-1]
        for i, a in enumerate(aggs, start=1):
            rows.append({"run": run, "producer": i, "rps": a["rps"], "mbps": a["mbps"],
                         "avg_ms": a["avg_ms"], "p50_ms": a["p50_ms"], "p99_ms": a["p99_ms"],
                         "max_ms": a["max_ms"], "source": path.name})
        total_rps = sum(a["rps"] for a in aggs)
        total_mbps = sum(a["mbps"] for a in aggs)
        rows.append({"run": run, "producer": "sum", "rps": total_rps, "mbps": total_mbps,
                     "avg_ms": math.nan, "p50_ms": math.nan, "p99_ms": math.nan,
                     "max_ms": math.nan, "source": path.name})
    return rows


def _omb_agg(data: dict, *names):
    for n in names:
        if n in data:
            v = data[n]
            if isinstance(v, list):
                xs = [float(x) for x in v]
                return sum(xs) / len(xs) if xs else math.nan
            return float(v)
    return math.nan


def v1_e2_ladder_rows(v1_root: Path) -> list[dict]:
    """tab-v1-e2-ladder.csv rows with the v1 Kingman prediction and error."""
    rows = []
    base = Path(v1_root) / "E2"
    if not base.is_dir():
        warn(f"v1 E2: {base} missing")
        return rows
    for rung in V1_RUNGS:
        for run_dir in sorted((base / str(rung)).glob("run*")):
            omb = run_dir / "omb.json"
            if not omb.is_file():
                continue
            try:
                data = json.loads(omb.read_text(encoding="utf-8"))
            except ValueError:
                warn(f"v1 E2: unparsable {omb}")
                continue
            rate = _omb_agg(data, "publishRate", "aggregatedPublishRate")
            msize = float(data.get("messageSize", V1_RECORD_BYTES))
            mbps = rate * msize / 1e6
            rho = mbps / V1_MU_MBPS if V1_MU_MBPS else math.nan
            avg = _omb_agg(data, "aggregatedPublishLatencyAvg", "publishLatencyAvg")
            pred = pred_latency(V1_TAU_MS, min(rho, 0.999), V1_CA2, V1_CS2, V1_TAU_MS)
            err = avg - pred if avg == avg and pred == pred else math.nan
            rows.append({
                "rung": rung, "run": run_dir.name.replace("run", ""),
                "publish_rate": rate, "mbps": mbps, "rho_real": rho,
                "avg_ms": avg,
                "p50_ms": _omb_agg(data, "aggregatedPublishLatency50pct", "publishLatency50pct"),
                "p99_ms": _omb_agg(data, "aggregatedPublishLatency99pct", "publishLatency99pct"),
                "pred_ms": pred, "err_ms": err,
                "err_rel": err / pred if err == err and pred else math.nan,
                "source": str(omb.relative_to(v1_root)).replace("\\", "/"),
            })
    if not rows:
        warn("v1 E2: no omb.json runs found")
    return rows


def v1_e2_summary_rows(ladder: list[dict]) -> list[dict]:
    out = []
    for rung in V1_RUNGS:
        rs = [r for r in ladder if r["rung"] == rung]
        rho = dispersion([r["rho_real"] for r in rs])
        avg = dispersion([r["avg_ms"] for r in rs])
        err = dispersion([r["err_ms"] for r in rs])
        rel = dispersion([r["err_rel"] for r in rs])
        out.append({"rung": rung, "n": len(rs), "rho_real_med": rho["median"],
                    "avg_ms_med": avg["median"], "avg_ms_cov": avg["cov"],
                    "pred_ms": rs[0]["pred_ms"] if rs else math.nan,
                    "err_ms_med": err["median"], "err_rel_med": rel["median"]})
    return out


def e2_error_comparison_rows(v1_summary: list[dict], v2_summary_rows: list[dict],
                             v2_config: str = "3b-rf1") -> list[dict]:
    """tab-e2-error-v1-v2.csv: relative ACK/publish-latency prediction error per
    rung, v1 (OMB, single VM, 3x e2-standard-4) vs v2 (functions, <v2_config>)."""
    rows = []
    for r in v1_summary:
        rows.append({"source": "v1-omb-3b-es4", "rho_target": r["rung"],
                     "rho_real": r["rho_real_med"], "obs_ms": r["avg_ms_med"],
                     "pred_ms": r["pred_ms"], "err_rel": r["err_rel_med"]})
    for r in v2_summary_rows:
        if r.get("config") != v2_config:
            continue
        rows.append({"source": f"v2-functions-{v2_config}", "rho_target": r["rho_target"],
                     "rho_real": r.get("rho_real_med", math.nan),
                     "obs_ms": r.get("ack_mean_med", math.nan),
                     "pred_ms": r.get("pred_ack", math.nan),
                     "err_rel": r.get("err_ack_rel_med", math.nan)})
    return rows
