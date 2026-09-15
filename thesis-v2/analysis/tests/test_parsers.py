"""parsers.py tests for the JSON input contract (series.json / ack.json):
null -> NaN/"" conventions, `errors` as {Type: count} object OR legacy
"Type:count;..." string, empty arrays, malformed files -- plus a check that
the converted fixtures carry the expected shapes.
"""
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import parsers

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "results"


def _write(tmp_path, name, payload):
    p = tmp_path / name
    p.write_text(json.dumps(payload) if not isinstance(payload, str) else payload,
                 encoding="utf-8")
    return p



def test_series_json_numbers_nulls_and_strings(tmp_path):
    p = _write(tmp_path, "series.json", [{
        "ts_ms": 1725000000000, "run_id": "r1", "lambda_leo": 20.5,
        "rho": None, "ack_count": 19, "error_types": None, "ca2_est": "1.25",
    }])
    rows = parsers.parse_dashboard_series_json(p)
    assert len(rows) == 1
    row = rows[0]
    assert set(row.keys()) == set(parsers.SERIES_COLUMNS)
    assert row["ts_ms"] == 1725000000000.0 and isinstance(row["ts_ms"], float)
    assert row["lambda_leo"] == 20.5
    assert row["ack_count"] == 19.0
    assert math.isnan(row["rho"])
    assert math.isnan(row["bytes_rate"])
    assert row["ca2_est"] == 1.25
    assert row["run_id"] == "r1"
    assert row["error_types"] == ""


def test_series_json_error_types_string_kept_and_object_normalised(tmp_path):
    p = _write(tmp_path, "series.json", [
        {"ts_ms": 1, "error_types": "TimeoutException:3;Other:1"},
        {"ts_ms": 2, "error_types": {"TimeoutException": 3, "Other": 1}},
    ])
    rows = parsers.parse_dashboard_series_json(p)
    assert rows[0]["error_types"] == "TimeoutException:3;Other:1"
    assert rows[1]["error_types"] == "Other:1;TimeoutException:3"


def test_series_json_empty_array_missing_and_malformed_give_empty_list(tmp_path):
    assert parsers.parse_dashboard_series_json(_write(tmp_path, "empty.json", [])) == []
    assert parsers.parse_dashboard_series_json(tmp_path / "nope.json") == []
    assert parsers.parse_dashboard_series_json(_write(tmp_path, "bad.json", "{not json")) == []
    assert parsers.parse_dashboard_series_json(_write(tmp_path, "obj.json", {"rows": []})) == []
    rows = parsers.parse_dashboard_series_json(_write(tmp_path, "mixed.json", [1, None, {"ts_ms": 5}]))
    assert len(rows) == 1 and rows[0]["ts_ms"] == 5.0



def test_ack_json_errors_object_is_used_directly(tmp_path):
    p = _write(tmp_path, "ack.json", [{
        "window_end_ms": 1725000015000, "run_id": "r1", "invocations": 10,
        "sent": 100, "acked": 98, "failed": 2, "bytes": 14000,
        "ack_mean_ms": 8.5, "ack_p50_ms": 8.0, "ack_p95_ms": 14.0,
        "ack_p99_ms": 19.0, "ack_max_ms": 39.0,
        "errors": {"TimeoutException": 2},
    }])
    rows = parsers.parse_ack_reports_json(p)
    assert len(rows) == 1
    row = rows[0]
    assert set(row.keys()) == set(parsers.ACK_COLUMNS)
    assert row["window_end_ms"] == 1725000015000.0
    assert row["acked"] == 98.0
    assert row["errors"] == {"TimeoutException": 2}
    assert isinstance(row["errors"]["TimeoutException"], int)


def test_ack_json_errors_legacy_string_is_parsed(tmp_path):
    p = _write(tmp_path, "ack.json", [
        {"window_end_ms": 1, "errors": "TimeoutException:2;BufferExhausted:1"},
        {"window_end_ms": 2, "errors": ""},
        {"window_end_ms": 3, "errors": None},
        {"window_end_ms": 4},
        {"window_end_ms": 5, "errors": {}},
    ])
    rows = parsers.parse_ack_reports_json(p)
    assert rows[0]["errors"] == {"TimeoutException": 2, "BufferExhausted": 1}
    assert rows[1]["errors"] == {}
    assert rows[2]["errors"] == {}
    assert rows[3]["errors"] == {}
    assert rows[4]["errors"] == {}


def test_ack_json_nulls_become_nan(tmp_path):
    p = _write(tmp_path, "ack.json", [{"window_end_ms": 7, "ack_mean_ms": None, "failed": None}])
    row = parsers.parse_ack_reports_json(p)[0]
    assert math.isnan(row["ack_mean_ms"])
    assert math.isnan(row["failed"])
    assert math.isnan(row["ack_p99_ms"])
    assert row["run_id"] == ""


def test_ack_json_empty_array_and_missing_give_empty_list(tmp_path):
    assert parsers.parse_ack_reports_json(_write(tmp_path, "empty.json", [])) == []
    assert parsers.parse_ack_reports_json(tmp_path / "nope.json") == []



def test_parse_error_types_object_and_string_equivalent():
    as_str = parsers.parse_error_types("TimeoutException:2;BufferExhausted:1")
    as_obj = parsers.parse_error_types({"TimeoutException": 2, "BufferExhausted": 1})
    assert as_str == as_obj == {"TimeoutException": 2, "BufferExhausted": 1}
    assert parsers.parse_error_types(None) == {}
    assert parsers.parse_error_types("") == {}
    assert parsers.parse_error_types({}) == {}
    assert parsers.parse_error_types({"A": 2.0, "B": "3", "C": "x"}) == {"A": 2, "B": 3, "C": 0}
    assert parsers.parse_error_types(as_obj) == as_obj
    assert parsers.format_error_types(as_obj) == "BufferExhausted:1;TimeoutException:2"



def test_fixture_json_files_follow_contract():
    e1 = FIXTURES / "E1" / "1b-rf1" / "run1"
    e4 = FIXTURES / "E4" / "1b-rf1" / "P10" / "run1"
    assert not (e1 / "dashboard_1hz.csv").exists()
    assert not (e1 / "ack_reports.csv").exists()

    series_raw = json.loads((e1 / "series.json").read_text(encoding="utf-8"))
    assert isinstance(series_raw, list) and len(series_raw) == 10
    assert list(series_raw[0].keys()) == parsers.SERIES_COLUMNS

    ack_raw = json.loads((e4 / "ack.json").read_text(encoding="utf-8"))
    assert isinstance(ack_raw, list) and len(ack_raw) == 9
    assert list(ack_raw[0].keys()) == parsers.ACK_COLUMNS
    assert ack_raw[0]["errors"] == {"TimeoutException": 20}

    rows = parsers.parse_ack_reports_json(e4 / "ack.json")
    assert rows[0]["errors"] == {"TimeoutException": 20}
    assert rows[0]["ack_mean_ms"] == 10.0
    series = parsers.parse_dashboard_series_json(e4 / "series.json")
    assert series[0]["error_types"] == "TimeoutException:20"
    assert series[0]["lambda_leo"] == 9200.0
