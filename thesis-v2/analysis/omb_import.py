"""OMB campaign importer (spec-analysis.md, E0): perf-test rung outputs
(OMB-reference/1b-rf1-<hw>/T<rung>/run<N>/perf.txt) -> per-rung ladder rows,
plateau mu with n-replicate errors, tab-omb-profile.csv rows and the OMB
profiles.json entry. Pure parsing/statistics: never plots, never writes.
perf-test line formats are ported from v1 analysis/parsers.py (AGGREGATE_RE
with percentiles, INTERIM_RE without); v1 is read-only, nothing imported.
"""
from __future__ import annotations

import math
import re
from pathlib import Path

from loaders import dispersion
from style import warn

AGGREGATE_RE = re.compile(
    r"(?P<records>[\d,]+) records sent,\s+"
    r"(?P<rps>[\d.]+) records/sec\s+\((?P<mbps>[\d.]+) MB/sec\),\s+"
    r"(?P<avg>[\d.]+) ms avg latency,\s+(?P<max>[\d.]+) ms max latency,\s+"
    r"(?P<p50>[\d.]+) ms 50th,\s+(?P<p95>[\d.]+) ms 95th,\s+"
    r"(?P<p99>[\d.]+) ms 99th,\s+(?P<p999>[\d.]+) ms 99\.9th"
)

INTERIM_RE = re.compile(
    r"(?P<records>[\d,]+) records sent,\s+"
    r"(?P<rps>[\d.]+) records/sec\s+\((?P<mbps>[\d.]+) MB/sec\),\s+"
    r"(?P<avg>[\d.]+) ms avg latency,\s+(?P<max>[\d.]+) ms max latency\."
)

RUNG_RE = re.compile(r"^T(?P<thr>\d+|inf2?x?)$", re.IGNORECASE)
RUN_DIR_RE = re.compile(r"^run(?P<n>\d+)$")


def parse_perf_aggregate(text: str) -> dict | None:
    """Last final-aggregate line of a perf.txt, or None when absent."""
    found = None
    for m in AGGREGATE_RE.finditer(text):
        found = {
            "records": int(m.group("records").replace(",", "")),
            "rps": float(m.group("rps")),
            "mbps": float(m.group("mbps")),
            "avg_ms": float(m.group("avg")),
            "max_ms": float(m.group("max")),
            "p50_ms": float(m.group("p50")),
            "p95_ms": float(m.group("p95")),
            "p99_ms": float(m.group("p99")),
            "p999_ms": float(m.group("p999")),
        }
    return found


def throttle_of(rung: str) -> float:
    """Rung label -> target msg/s (Tinf/Tinf2x -> +inf)."""
    m = RUNG_RE.match(rung)
    if not m:
        return math.nan
    thr = m.group("thr").lower()
    return math.inf if thr.startswith("inf") else float(thr)




def parse_consumer_summary(text: str) -> dict | None:
    """Last pure-CSV consumer summary line -> avg fetch ms per message."""
    found = None
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 10:
            continue
        try:
            nmsg = float(parts[4])
            fetchms = float(parts[7])
        except ValueError:
            continue
        if nmsg > 0:
            found = {"nmsg": nmsg, "fetch_ms_total": fetchms,
                     "fetch_ms_per_msg": fetchms / nmsg}
    return found


def discover_omb_e2e(results_root: Path, config_hw: str) -> dict:
    """OMB-reference/<config-hw>/T<rung>/run<N>/perf-consumer.txt
    -> {rung: [fetch_ms_per_msg, ...]} (tau_e2e proxy, method=fetch-proxy)."""
    base = Path(results_root) / "OMB-reference" / config_hw
    out: dict = {}
    if not base.is_dir():
        return out
    for rung_dir in sorted(base.iterdir()):
        if not (rung_dir.is_dir() and RUNG_RE.match(rung_dir.name)):
            continue
        vals = []
        for run_dir in sorted(rung_dir.iterdir()):
            if not (run_dir.is_dir() and RUN_DIR_RE.match(run_dir.name)):
                continue
            for perf in sorted(run_dir.glob("perf-consumer*.txt")):
                try:
                    text = perf.read_text(encoding="utf-8", errors="replace")
                except OSError as e:
                    warn(f"OMB: cannot read {perf}: {e}")
                    continue
                s = parse_consumer_summary(text)
                if s is None:
                    warn(f"OMB: no consumer summary in {perf}")
                else:
                    vals.append(s["fetch_ms_per_msg"])
        if vals:
            out[rung_dir.name] = vals
    return out


def discover_omb(results_root: Path, config_hw: str) -> dict:
    """OMB-reference/<config-hw>/T<rung>/run<N>/perf.txt -> {rung: [aggregates]}.

    Tinf2x dual-producer runs land under rung "Tinf2x" with perf-a.txt /
    perf-b.txt (plus the summary perf.txt); every producer perf*.txt aggregate
    counts as one observation. Consumer logs belong to discover_omb_e2e.
    Missing/unparseable producer files warn, never crash.
    """
    base = Path(results_root) / "OMB-reference" / config_hw
    out: dict = {}
    if not base.is_dir():
        warn(f"OMB: no directory for {config_hw}")
        return out
    for rung_dir in sorted(base.iterdir()):
        if not (rung_dir.is_dir() and RUNG_RE.match(rung_dir.name)):
            continue
        obs = []
        for run_dir in sorted(rung_dir.iterdir()):
            if not (run_dir.is_dir() and RUN_DIR_RE.match(run_dir.name)):
                continue
            for perf in sorted(run_dir.glob("perf*.txt")):
                if perf.name.startswith("perf-consumer"):
                    continue
                try:
                    agg = parse_perf_aggregate(perf.read_text(encoding="utf-8", errors="replace"))
                except OSError as e:
                    warn(f"OMB: cannot read {perf}: {e}")
                    continue
                if agg is None:
                    warn(f"OMB: no aggregate line in {perf}")
                    continue
                agg["run"] = run_dir.name
                obs.append(agg)
        if obs:
            out[rung_dir.name] = obs
    return out


def ladder_rows(config_hw: str, by_rung: dict) -> list[dict]:
    """One row per rung with medians across replicate runs/files."""
    rows = []
    for rung in sorted(by_rung, key=lambda r: (throttle_of(r), r)):
        obs = by_rung[rung]
        rps = [o["rps"] for o in obs]
        d = dispersion(rps)
        p50 = [o["p50_ms"] for o in obs]
        p99 = [o["p99_ms"] for o in obs]
        rows.append({
            "config_hw": config_hw,
            "rung": rung,
            "thr_target": throttle_of(rung),
            "n": d["n"],
            "rps_med": d["median"],
            "rps_sd": d["stdev"],
            "rps_cov": d["cov"],
            "mbps_med": sorted(o["mbps"] for o in obs)[len(obs) // 2],
            "avg_ms_med": sorted(o["avg_ms"] for o in obs)[len(obs) // 2],
            "p50_ms_med": sorted(p50)[len(p50) // 2],
            "p99_ms_med": sorted(p99)[len(p99) // 2],
        })
    return rows


def plateau_mu(ladder: list[dict]) -> dict:
    """Plateau mu from the two highest finite throttles: flat within 5%
    (relative to the higher rung) counts as the wall. Returns mu_msgs
    (median of the top rung), mu_sd (sample stdev across its replicates),
    mu_cov, mu_n, plus the rung pair and growth for the audit trail.
    Single-rung ladders report mu_status=no-plateau-yet instead of a number.
    """
    finite = [r for r in ladder if r["thr_target"] == r["thr_target"]
              and r["thr_target"] != math.inf and r["n"] > 0]
    out = {"mu_msgs": math.nan, "mu_sd": math.nan, "mu_cov": math.nan,
           "mu_n": 0, "mu_status": "no-data", "rungs": "", "growth": math.nan}
    if len(finite) < 2:
        out["mu_status"] = "no-plateau-yet" if finite else "no-data"
        return out
    lo, hi = finite[-2], finite[-1]
    growth = (hi["rps_med"] - lo["rps_med"]) / hi["rps_med"] if hi["rps_med"] else math.nan
    out["rungs"] = f"{lo['rung']},{hi['rung']}"
    out["growth"] = growth
    if growth == growth and abs(growth) <= 0.05:
        out["mu_msgs"] = hi["rps_med"]
        out["mu_sd"] = hi["rps_sd"]
        out["mu_cov"] = hi["rps_cov"]
        out["mu_n"] = hi["n"]
        out["mu_status"] = "plateau"
    else:
        out["mu_status"] = "still-scaling"
    return out


def omb_profile_row(config_hw: str, by_rung: dict, mu: dict,
                      e2e_by_rung: dict | None = None) -> dict:
    """Single tab-omb-profile.csv row: mu from the plateau, tau_ack from the
    lowest-throttle rung's per-replicate p50 values (median ± sample stdev),
    tau_e2e from consumer fetch summaries when present (method=fetch-proxy),
    payload from the perf header when present (else NaN + method note).
    ca2/cs2 are unavailable from perf-test by construction (NaN + method).
    """
    keyed = [(throttle_of(r), r) for r in by_rung if throttle_of(r) == throttle_of(r)]
    tau = dispersion([])
    if keyed:
        low = min(keyed)[1]
        tau = dispersion([o["p50_ms"] for o in by_rung[low]])
    e2e = dispersion([])
    if e2e_by_rung:
        keyed_e = [(throttle_of(r), r) for r in e2e_by_rung if throttle_of(r) == throttle_of(r)]
        if keyed_e:
            low_e = min(keyed_e)[1]
            e2e = dispersion(e2e_by_rung[low_e])
    method = f"plateau({mu['rungs']},{mu['mu_status']})+T-low-p50"
    if e2e["n"] > 0:
        method += "+fetch-proxy"
    return {
        "config": config_hw,
        "mu_msgs": mu["mu_msgs"],
        "mu_sd": mu["mu_sd"],
        "mu_cov": mu["mu_cov"],
        "mu_n": mu["mu_n"],
        "tau_ack_ms": tau["median"],
        "tau_ack_sd": tau["stdev"],
        "tau_e2e_ms": e2e["median"],
        "tau_e2e_sd": e2e["stdev"],
        "payload_bytes": math.nan,
        "method": method,
    }


def omb_profiles_json_entry(row: dict) -> dict:
    """profiles.json OMB entry. Unknown fields are OMITTED (never null):
    the dashboard Profile POJO keeps its NaN field defaults for absent keys,
    while an explicit null would coerce to 0.0 in Java primitives and corrupt
    Kingman math (mu=0!). Same rule as analyze.py profiles_out.
    """
    def _v(k):
        v = row.get(k, math.nan)
        return None if v is None or v != v else v
    entry = {"config": "1b-rf1",
             "source": f"OMB({row.get('config', '?')},{row.get('method', '?')})"}
    for py_key, java_key in (("mu_msgs", "muMsgs"), ("tau_ack_ms", "tauAckMs"),
                             ("tau_e2e_ms", "tauE2eMs")):
        v = _v(py_key)
        if v is not None:
            entry[java_key] = v
    return entry
