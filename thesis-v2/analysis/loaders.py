"""Discovery of results/ directories (experiment-plan.md SS5.1 layout) and
loading of one run's full artifact set into a `RunData` dataclass, plus
shared n=3 sample statistics (spec-analysis.md SS2 "Statystyki n=3").

Directory layout (frozen, experiment-plan.md SS5.1):
    results/<EXP>/<CONFIG>/run<N>/                      (E1)
    results/<EXP>/<CONFIG>/P<P>/run<N>/                 (E4)
    results/<EXP>/<CONFIG>/rho<R>/run<N>/               (E2)
Files per run dir: run.json, series.json, ack.json, prom/*.json,
config.env, window.env (spec-analysis.md SS1; series/ack are JSON arrays
since the SQLite/JSON dashboard rewrite).
"""
from __future__ import annotations

import math
import re
import statistics
from dataclasses import dataclass, field
from pathlib import Path

from parsers import (
    parse_run_json,
    parse_dashboard_series_json,
    parse_ack_reports_json,
    parse_env_file,
)
from style import warn

CONFIGS = ["1b-rf1", "3b-rf1", "3b-rf3"]
E4_RUNGS = [10, 25, 50, 100, 200, 300]
E2_RUNGS = [25, 50, 75, 90]

SERIES_FILE = "series.json"
ACK_FILE = "ack.json"

_RUN_DIR_RE = re.compile(r"^run(\d+)$")
_P_DIR_RE = re.compile(r"^P(\d+)$")
_R_DIR_RE = re.compile(r"^R(\d+)$")
_RHO_DIR_RE = re.compile(r"^rho(\d+)([a-z]*)$")


@dataclass
class RunData:
    """Everything loaded from one results/.../run<N>/ directory."""
    exp: str
    config: str
    rung: "int | str | None"
    run_index: int
    path: Path
    manifest: dict = field(default_factory=dict)
    series: list = field(default_factory=list)
    ack: list = field(default_factory=list)
    config_env: dict = field(default_factory=dict)
    window_env: dict = field(default_factory=dict)
    prom_dir: "Path | None" = None
    complete: bool = True


def _analysis_window_ms(manifest: dict, run_dir: Path):
    """[start+15000, stop] from manifest (spec-analysis.md SS1); falls back
    to window.env start/stop if the manifest lacks the epoch fields."""
    start = manifest.get("startEpochMs")
    stop = manifest.get("stopEpochMs")
    if start is None or stop is None:
        env = parse_env_file(run_dir / "window.env")
        try:
            start = float(env.get("start")) if start is None else start
            stop = float(env.get("stop")) if stop is None else stop
        except (TypeError, ValueError):
            pass
    if start is None or stop is None:
        return None
    return float(start) + 15000.0, float(stop)


def load_run(run_dir: Path, exp: str, config: str, rung=None, run_index: int = 0) -> RunData:
    """Load one run<N> directory into a RunData; missing files are
    tolerated (empty containers), each logging one warning."""
    complete = True

    run_json = run_dir / "run.json"
    manifest = parse_run_json(run_json)
    if not manifest:
        warn(f"{run_dir}: run.json missing or unparsable")
        complete = False

    series = parse_dashboard_series_json(run_dir / SERIES_FILE)
    if not series:
        warn(f"{run_dir}: {SERIES_FILE} missing, unparsable or empty")
        complete = False

    ack = parse_ack_reports_json(run_dir / ACK_FILE)
    if not ack:
        warn(f"{run_dir}: {ACK_FILE} missing, unparsable or empty")
        complete = False

    config_env = parse_env_file(run_dir / "config.env")
    window_env = parse_env_file(run_dir / "window.env")
    if not config_env:
        warn(f"{run_dir}: config.env missing or empty")
    if not window_env:
        warn(f"{run_dir}: window.env missing or empty")

    prom_dir = run_dir / "prom"
    if not prom_dir.is_dir():
        warn(f"{run_dir}: prom/ directory missing")
        prom_dir = None
        complete = False

    return RunData(exp=exp, config=config, rung=rung, run_index=run_index,
                    path=run_dir, manifest=manifest, series=series, ack=ack,
                    config_env=config_env, window_env=window_env,
                    prom_dir=prom_dir, complete=complete)


def analysis_window(run: RunData):
    """(start_ms, stop_ms) analysis window for this run, or None."""
    return _analysis_window_ms(run.manifest, run.path)


def series_in_window(run: RunData) -> list[dict]:
    """series.json rows whose ts_ms falls within the analysis window.
    If the window cannot be determined, returns the full series (best
    effort) and the caller's own warnings already recorded the gap."""
    win = analysis_window(run)
    if win is None:
        return run.series
    start, stop = win
    return [r for r in run.series if start <= r.get("ts_ms", math.nan) <= stop]


def ack_in_window(run: RunData) -> list[dict]:
    """ack.json rows whose window_end_ms falls within the analysis
    window; best-effort fallback to full list like series_in_window."""
    win = analysis_window(run)
    if win is None:
        return run.ack
    start, stop = win
    return [r for r in run.ack if start <= r.get("window_end_ms", math.nan) <= stop]



def discover_configs(results_root: Path, exp: str) -> list[str]:
    """Subdirectory names under results/<EXP>/ (each is a config or
    config-hw variant)."""
    base = Path(results_root) / exp
    if not base.is_dir():
        return []
    return sorted(d.name for d in base.iterdir() if d.is_dir())


def _variant_dirs(base: Path, config: str) -> list[Path]:
    """Dirs named exactly <config> or <config>-<hw> (hw suffix never
    contains another bare config name collision by construction)."""
    if not base.is_dir():
        return []
    return sorted((d for d in base.iterdir()
                   if d.is_dir() and (d.name == config or d.name.startswith(config + "-"))),
                  key=lambda d: d.name)

def discover_e1(results_root: Path, config: str) -> list[RunData]:
    """results/E1/<config>[-<hw>]/run<N>/ -> [RunData] (run.config = full
    dirname, so hardware never mixes silently)."""
    out = []
    for cdir in _variant_dirs(Path(results_root) / "E1", config):
        for d in sorted(cdir.iterdir()):
            m = _RUN_DIR_RE.match(d.name)
            if d.is_dir() and m:
                out.append(load_run(d, "E1", cdir.name, rung=None, run_index=int(m.group(1))))
    out.sort(key=lambda r: (r.config, r.run_index))
    return out


def discover_e4(results_root: Path, config: str) -> dict:
    """results/E4/<config>[-<hw>]/P<P>/run<N>/ -> {(config_hw, P-int): [RunData]}.

    Keys are (config_hw, P) tuples: callers group by run.config downstream.
    """
    out: dict = {}
    for cdir in _variant_dirs(Path(results_root) / "E4", config):
        for pdir in sorted(cdir.iterdir()):
            mp = _P_DIR_RE.match(pdir.name)
            if not (pdir.is_dir() and mp):
                continue
            p_val = int(mp.group(1))
            runs = []
            for d in sorted(pdir.iterdir()):
                mrn = _RUN_DIR_RE.match(d.name)
                if d.is_dir() and mrn:
                    runs.append(load_run(d, "E4", cdir.name, rung=p_val, run_index=int(mrn.group(1))))
            runs.sort(key=lambda r: r.run_index)
            out[(cdir.name, p_val)] = runs
    return out


def discover_e4r(results_root: Path, config: str) -> dict:
    """results/E4/<config>[-<hw>]/R<rate>/run<N>/ -> {(config_hw, rate-int): [RunData]}.

    Rate ladder (E4R mode): fixed parallelism, sweeping ratePerSec. rung is
    the int rate, so e4_plateau() works unchanged (plateau over ascending
    rates); callers rename P_plateau -> rate_plateau on output.
    """
    out: dict = {}
    for cdir in _variant_dirs(Path(results_root) / "E4", config):
        for rdir in sorted(cdir.iterdir()):
            mr = _R_DIR_RE.match(rdir.name)
            if not (rdir.is_dir() and mr):
                continue
            rate_val = int(mr.group(1))
            runs = []
            for d in sorted(rdir.iterdir()):
                mrn = _RUN_DIR_RE.match(d.name)
                if d.is_dir() and mrn:
                    runs.append(load_run(d, "E4", cdir.name, rung=rate_val, run_index=int(mrn.group(1))))
            runs.sort(key=lambda r: r.run_index)
            out[(cdir.name, rate_val)] = runs
    return out


def discover_e2(results_root: Path, config: str) -> dict:
    """E2 rho rungs with optional variant suffix, keyed triple."""
    out: dict = {}
    for cdir in _variant_dirs(Path(results_root) / "E2", config):
        for rdir in sorted(cdir.iterdir()):
            mr = _RHO_DIR_RE.match(rdir.name)
            if not (rdir.is_dir() and mr):
                continue
            rho_val = int(mr.group(1))
            variant = mr.group(2) or ""
            runs = []
            for d in sorted(rdir.iterdir()):
                mn = _RUN_DIR_RE.match(d.name)
                if d.is_dir() and mn:
                    runs.append(load_run(d, "E2", cdir.name, rung=rho_val, run_index=int(mn.group(1))))
            runs.sort(key=lambda r: r.run_index)
            out[(cdir.name, rho_val, variant)] = runs
    return out



def dispersion(values: list[float]) -> dict:
    """Sample statistics for n replicate runs: median, mean, sample stdev,
    CoV (stdev/mean), min, max. NaN inputs are dropped. n<2 -> stdev/cov
    are NaN (undefined for a single point). Empty input -> all NaN, n=0.
    """
    vals = [v for v in values if v == v]
    n = len(vals)
    out = {"n": n, "median": math.nan, "mean": math.nan, "stdev": math.nan,
           "cov": math.nan, "min": math.nan, "max": math.nan}
    if not vals:
        return out
    out["mean"] = statistics.fmean(vals)
    out["median"] = statistics.median(vals)
    out["min"], out["max"] = min(vals), max(vals)
    if n >= 2:
        out["stdev"] = statistics.stdev(vals)
        if out["mean"] != 0:
            out["cov"] = out["stdev"] / abs(out["mean"])
    return out



def discover_logdirs_evidence(results_root: Path) -> list[Path]:
    """Partitioning evidence: results/partitioning-id-evidence/*.json
    (standalone, run-independent snapshots) plus legacy evidence/logdirs-*.json
    anywhere under results/."""
    base = Path(results_root)
    if not base.is_dir():
        return []
    found = sorted((base / "partitioning-id-evidence").glob("*.json"))
    found += sorted(base.rglob("evidence/logdirs-*.json"))
    return found


def discover_prom_log_bytes(results_root: Path) -> list[Path]:
    """prom_log_bytes.json inside any collected run (JMX kafka.log Size,
    per broker/topic/partition time series)."""
    base = Path(results_root)
    if not base.is_dir():
        return []
    return sorted(base.rglob("prom/prom_log_bytes.json"))
