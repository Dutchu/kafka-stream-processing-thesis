"""Pure parsing functions for every raw result format used by thesis-v2.

Formats: spec-dashboard.md SS5 (series.json == 1 Hz dashboard series,
ack.json == per-invocation ACK reports, run.json manifest -- all JSON since
the SQLite/JSON dashboard rewrite), spec-infra.md R7 (prom/*.json keys,
standard Prometheus /api/v1/query_range response). Functions here only read
the given path/string; they never plot, aggregate across runs, or write.
"""
from __future__ import annotations

import json
import math
from pathlib import Path


def parse_prom_json(path) -> list[tuple[dict, list[tuple[float, float]]]]:
    """Parse a raw Prometheus HTTP API `query_range` response.

    Expected envelope:
        {"status": "success",
         "data": {"resultType": "matrix",
                   "result": [{"metric": {...labels...},
                               "values": [[ts, "val"], ...]}, ...]}}

    Returns a list of (labels_dict, series) pairs, one per `result` entry,
    where series is a list of (float timestamp, float value) points.
    "NaN" string values are skipped (not converted to float('nan')) so
    downstream min/max/median callers never see NaN. Any malformed or
    unsuccessful response (bad JSON, "status" != "success", missing/empty
    "result", wrong "resultType") is tolerated and yields []; the caller is
    expected to warn, not crash.
    """
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []

    if not isinstance(data, dict) or data.get("status") != "success":
        return []
    payload = data.get("data")
    if not isinstance(payload, dict):
        return []
    result = payload.get("result")
    if not isinstance(result, list) or not result:
        return []

    out: list[tuple[dict, list[tuple[float, float]]]] = []
    for series in result:
        if not isinstance(series, dict):
            continue
        labels = series.get("metric") or {}
        raw_values = series.get("values") or []
        points: list[tuple[float, float]] = []
        for pair in raw_values:
            try:
                ts_raw, val_raw = pair
            except (TypeError, ValueError):
                continue
            if isinstance(val_raw, str) and val_raw.strip().lower() == "nan":
                continue
            try:
                ts = float(ts_raw)
                val = float(val_raw)
            except (TypeError, ValueError):
                continue
            if val != val:
                continue
            points.append((ts, val))
        out.append((labels, points))
    return out


def prom_all_points(path) -> list[tuple[float, float]]:
    """Flatten every (labels, series) pair from one prom/*.json into points
    (used for single-series queries such as prom_handler_idle.json where
    caller wants a plain value series regardless of label set)."""
    pts: list[tuple[float, float]] = []
    for _labels, series in parse_prom_json(path):
        pts.extend(series)
    return pts


def parse_logdirs_snapshot(path) -> list[dict]:
    """kafka-log-dirs.sh JSON (evidence/logdirs-<topic>.json) ->
    [{topic, partition, broker, size_bytes}]. Accepts both the 'brokers'-list
    shape and the dict shape. Unparseable files yield [] (caller warns)."""
    import json as _json
    from pathlib import Path as _Path
    try:
        raw = _Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    start = raw.find("{")
    try:
        doc = _json.loads(raw[start:] if start >= 0 else raw)
    except ValueError:
        return []
    brokers = doc.get("brokers", []) if isinstance(doc, dict) else []
    if isinstance(brokers, dict):
        items = []
        for bid, b in brokers.items():
            for ld in (b.get("logDirs") or []):
                for p in (ld.get("partitions") or []):
                    items.append((bid, p))
    else:
        items = []
        for b in brokers:
            bid = b.get("broker")
            for ld in (b.get("logDirs") or []):
                for p in (ld.get("partitions") or []):
                    items.append((bid, p))
    rows = []
    for bid, p in items:
        name = str(p.get("partition", ""))
        topic, _, part = name.rpartition("-")
        try:
            size = float(p.get("size", float("nan")))
        except (TypeError, ValueError):
            size = float("nan")
        rows.append({"topic": topic, "partition": part, "broker": str(bid),
                     "size_bytes": size, "source": str(path)})
    return rows


def prom_last_by_labels(path, label_keys: tuple) -> list[dict]:
    """Last value of every series in a prom matrix file, with its labels."""
    out = []
    for labels, series in parse_prom_json(path):
        if not series:
            continue
        out.append({"labels": {k: labels.get(k) for k in label_keys},
                    "value": series[-1][1]})
    return out


def prom_series_by_label(path, label_key: str) -> dict:
    """{label_value: [(ts, val), ...]} grouped by one label key (e.g.
    'broker' or 'node'); series without that label key are skipped."""
    out: dict = {}
    for labels, series in parse_prom_json(path):
        key = labels.get(label_key)
        if key is None:
            continue
        out.setdefault(key, []).extend(series)
    return out


def prom_min_of_mean_by_label(path, label_key: str):
    """min-over-labels of the mean-over-time value; NaN if no series.

    Implements the E4 signature statistics (spec-analysis.md SS2): e.g.
    handler_idle_min = "min po brokerach (srednia w oknie prom_handler_idle)".
    """
    by_label = prom_series_by_label(path, label_key)
    means = []
    for _label, series in by_label.items():
        vals = [v for _ts, v in series]
        if vals:
            means.append(sum(vals) / len(vals))
    return min(means) if means else math.nan


def prom_max_of_mean_by_label(path, label_key: str):
    """max-over-labels of the mean-over-time value; NaN if no series."""
    by_label = prom_series_by_label(path, label_key)
    means = []
    for _label, series in by_label.items():
        vals = [v for _ts, v in series]
        if vals:
            means.append(sum(vals) / len(vals))
    return max(means) if means else math.nan


def prom_mean_all(path):
    """Mean of every point across every series in the file; NaN if empty."""
    pts = prom_all_points(path)
    vals = [v for _ts, v in pts]
    return sum(vals) / len(vals) if vals else math.nan


def prom_max_all(path):
    """Max value across every series/point in the file; NaN if empty."""
    pts = prom_all_points(path)
    vals = [v for _ts, v in pts]
    return max(vals) if vals else math.nan


def prom_fraction_above(path, threshold: float) -> float:
    """Fraction of points (across all series) with value > threshold; NaN
    if the file has no points (used for request_queue > 0 duration)."""
    pts = prom_all_points(path)
    if not pts:
        return math.nan
    hits = sum(1 for _ts, v in pts if v > threshold)
    return hits / len(pts)



def parse_run_json(path) -> dict:
    """Load run.json manifest as a plain dict; {} if missing/unparsable
    (caller is expected to warn and fall back to NaN placeholders)."""
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}



def _load_json_rows(path) -> list:
    """Read a JSON file that must contain an array of row objects.

    Missing file, unparsable JSON, or a top-level value that is not a list
    -> [] (caller warns). Non-object elements inside the array are dropped.
    An empty array is a legitimate (empty) result, also [].
    """
    p = Path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict)]


def _to_float(value):
    """JSON scalar -> float; null/missing/non-numeric/blank -> NaN.

    Accepts real JSON numbers (int/float) and, defensively, numeric strings
    (e.g. "12.5") so a collector that stringifies values still parses.
    bool is rejected (JSON true/false are not measurements)."""
    if value is None or isinstance(value, bool):
        return math.nan
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return math.nan
        try:
            return float(s)
        except ValueError:
            return math.nan
    return math.nan


def _to_str(value) -> str:
    """JSON scalar -> str; null -> "" (the former CSV empty-cell convention)."""
    if value is None:
        return ""
    return str(value).strip()



SERIES_COLUMNS = [
    "ts_ms", "run_id", "lambda_leo", "lambda_consumer", "rho", "wq_ms",
    "pred_ack_ms", "pred_e2e_ms", "ack_mean_ms", "ack_p50_ms", "ack_p95_ms",
    "ack_p99_ms", "ack_max_ms", "ack_count", "ack_invocations",
    "e2e_mean_ms", "e2e_p50_ms", "e2e_p99_ms", "e2e_max_ms", "e2e_count",
    "err_ack_ms", "err_e2e_ms", "err_ack_rel", "err_e2e_rel",
    "consumer_lag", "sent", "acked", "failed", "error_types",
    "ca2_est", "cs2_est", "total_msgs_cluster", "bytes_rate",
]

_NUMERIC_SERIES_COLUMNS = set(SERIES_COLUMNS) - {"run_id", "error_types"}


def parse_dashboard_series_json(path) -> list[dict]:
    """series.json (JSON array, one object per 1 Hz tick, keys ==
    SERIES_COLUMNS) -> list of row dicts, numeric columns as float (JSON
    null -> NaN), 'run_id' as str, 'error_types' as the "Type:count;..."
    str (null -> ""). Missing/invalid file or empty array -> [] (caller
    warns). Keys absent from a row are filled (NaN / "") so every row has
    the full SERIES_COLUMNS key set."""
    rows = []
    for raw in _load_json_rows(path):
        row = {}
        for col in SERIES_COLUMNS:
            val = raw.get(col)
            if col in _NUMERIC_SERIES_COLUMNS:
                row[col] = _to_float(val)
            elif col == "error_types" and isinstance(val, dict):
                row[col] = format_error_types(parse_error_types(val))
            else:
                row[col] = _to_str(val)
        rows.append(row)
    return rows



ACK_COLUMNS = [
    "window_end_ms", "run_id", "invocations", "sent", "acked", "failed",
    "bytes", "ack_mean_ms", "ack_p50_ms", "ack_p95_ms", "ack_p99_ms",
    "ack_max_ms", "errors",
]

_NUMERIC_ACK_COLUMNS = set(ACK_COLUMNS) - {"run_id", "errors"}


def parse_ack_reports_json(path) -> list[dict]:
    """ack.json (JSON array of objects, keys == ACK_COLUMNS) -> list of row
    dicts, numeric columns as float (null -> NaN), 'run_id' as str, and
    'errors' normalised to a dict {Type: int count}.

    'errors' in the new contract is a JSON object {Type: count}; the legacy
    "Type:count;..." string (and null / "") is accepted too and parsed with
    parse_error_types(), so both shapes yield the same dict. Missing/invalid
    file or empty array -> [] (caller warns)."""
    rows = []
    for raw in _load_json_rows(path):
        row = {}
        for col in ACK_COLUMNS:
            val = raw.get(col)
            if col in _NUMERIC_ACK_COLUMNS:
                row[col] = _to_float(val)
            elif col == "errors":
                row[col] = parse_error_types(val)
            else:
                row[col] = _to_str(val)
        rows.append(row)
    return rows



def parse_error_types(cell) -> dict:
    """Normalise an error-count field to {str: int}.

    Accepts: a dict {"TimeoutException": 2, "BufferExhausted": 1} (JSON
    object form -- returned as a fresh dict with int counts, non-numeric
    counts -> 0); the legacy string form
    'TimeoutException:2;BufferExhausted:1'; None / "" / {} -> {}."""
    if cell is None:
        return {}
    if isinstance(cell, dict):
        out: dict = {}
        for k, v in cell.items():
            key = str(k)
            try:
                cnt = int(v) if v is not None and not isinstance(v, bool) else 0
            except (TypeError, ValueError):
                cnt = 0
            out[key] = out.get(key, 0) + cnt
        return out
    s = str(cell).strip()
    if not s:
        return {}
    out = {}
    for part in s.split(";"):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            k, _, v = part.rpartition(":")
            try:
                out[k] = out.get(k, 0) + int(v)
            except ValueError:
                out[k] = out.get(k, 0)
        else:
            out[part] = out.get(part, 0)
    return out


def format_error_types(counts: dict) -> str:
    """Inverse of parse_error_types, stable key order (sorted)."""
    return ";".join(f"{k}:{v}" for k, v in sorted(counts.items()))



def parse_env_file(path) -> dict:
    """KEY=VALUE lines (one per line, '#' comments allowed) -> dict[str,str].
    Missing file -> {}."""
    p = Path(path)
    if not p.exists():
        return {}
    out: dict = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"')
    return out
