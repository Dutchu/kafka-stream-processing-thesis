"""loaders.py tests: directory discovery (experiment-plan.md SS5.1 layout),
analysis-window slicing, and n=3 dispersion statistics -- against the
synthetic fixtures in tests/fixtures/results/.
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import loaders

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "results"


def test_discover_e1_finds_one_run():
    runs = loaders.discover_e1(FIXTURES, "1b-rf1")
    assert len(runs) == 1
    assert runs[0].exp == "E1"
    assert runs[0].config == "1b-rf1"
    assert runs[0].run_index == 1
    assert runs[0].manifest.get("runId", "").startswith("E1-1b-rf1")


def test_discover_e1_missing_config_returns_empty():
    runs = loaders.discover_e1(FIXTURES, "3b-rf1")
    assert runs == []


def test_discover_e4_finds_two_p_rungs():
    by_p = loaders.discover_e4(FIXTURES, "1b-rf1")
    assert set(by_p.keys()) == {("1b-rf1", 10), ("1b-rf1", 25)}
    assert len(by_p[("1b-rf1", 10)]) == 1
    assert len(by_p[("1b-rf1", 25)]) == 1
    assert by_p[("1b-rf1", 10)][0].rung == 10
    assert by_p[("1b-rf1", 25)][0].rung == 25


def test_discover_e4r_empty_without_r_dirs():
    assert loaders.discover_e4r(FIXTURES, "1b-rf1") == {}


def test_discover_e4r_finds_rate_rung(tmp_path):
    base = tmp_path / "E4" / "1b-rf1-esmall" / "R500" / "run1"
    base.mkdir(parents=True)
    by_r = loaders.discover_e4r(str(tmp_path), "1b-rf1")
    assert set(by_r.keys()) == {("1b-rf1-esmall", 500)}
    assert by_r[("1b-rf1-esmall", 500)][0].rung == 500
    assert by_r[("1b-rf1-esmall", 500)][0].config == "1b-rf1-esmall"


def test_discover_e2_finds_one_rho_rung():
    by_rho = loaders.discover_e2(FIXTURES, "1b-rf1")
    assert set(by_rho.keys()) == {("1b-rf1", 50, "")}
    assert len(by_rho[("1b-rf1", 50, "")]) == 1
    assert by_rho[("1b-rf1", 50, "")][0].rung == 50


def test_discover_e2_variant_suffix(tmp_path):
    base = tmp_path / "E2" / "1b-rf1" / "rho90bp" / "run1"
    base.mkdir(parents=True)
    by_rho = loaders.discover_e2(str(tmp_path), "1b-rf1")
    assert set(by_rho.keys()) == {("1b-rf1", 90, "bp")}
    assert by_rho[("1b-rf1", 90, "bp")][0].rung == 90


def test_loaded_run_has_series_and_ack_rows():
    runs = loaders.discover_e1(FIXTURES, "1b-rf1")
    run = runs[0]
    assert len(run.series) == 10
    assert len(run.ack) == 10
    assert run.complete is True
    assert isinstance(run.series[0]["ts_ms"], float)
    assert run.series[0]["run_id"].startswith("E1-1b-rf1")
    assert run.series[0]["error_types"] == ""
    assert run.ack[0]["errors"] == {}


def test_load_run_missing_series_and_ack_is_incomplete(tmp_path):
    """A run dir with run.json but no series.json/ack.json loads with empty
    containers and complete=False (warning, not crash)."""
    run_dir = tmp_path / "run1"
    run_dir.mkdir()
    (run_dir / "run.json").write_text('{"runId": "x", "startEpochMs": 0, "stopEpochMs": 90000}',
                                      encoding="utf-8")
    run = loaders.load_run(run_dir, "E1", "1b-rf1", run_index=1)
    assert run.series == []
    assert run.ack == []
    assert run.complete is False
    assert loaders.series_in_window(run) == []
    assert loaders.ack_in_window(run) == []


def test_analysis_window_is_start_plus_15s_to_stop():
    runs = loaders.discover_e1(FIXTURES, "1b-rf1")
    run = runs[0]
    win = loaders.analysis_window(run)
    assert win == (1725000000000.0 + 15000.0, 1725000090000.0)


def test_series_in_window_excludes_warmup_row():
    runs = loaders.discover_e1(FIXTURES, "1b-rf1")
    run = runs[0]
    windowed = loaders.series_in_window(run)
    assert all(r["ts_ms"] >= 1725000015000.0 for r in windowed)
    assert 1725000000000.0 not in {r["ts_ms"] for r in windowed}
    assert 1725000005000.0 not in {r["ts_ms"] for r in windowed}
    assert len(windowed) == 8


def test_ack_in_window_excludes_warmup_row():
    runs = loaders.discover_e1(FIXTURES, "1b-rf1")
    run = runs[0]
    windowed = loaders.ack_in_window(run)
    assert all(r["window_end_ms"] >= 1725000015000.0 for r in windowed)
    assert 1725000000000.0 not in {r["window_end_ms"] for r in windowed}
    assert 1725000005000.0 not in {r["window_end_ms"] for r in windowed}
    assert len(windowed) == 8


def test_dispersion_basic_n3():
    d = loaders.dispersion([10.0, 12.0, 11.0])
    assert d["n"] == 3
    assert d["median"] == 11.0
    assert math.isclose(d["mean"], 11.0)
    assert d["min"] == 10.0
    assert d["max"] == 12.0
    assert d["stdev"] > 0
    assert d["cov"] > 0


def test_dispersion_drops_nan():
    d = loaders.dispersion([10.0, math.nan, 12.0])
    assert d["n"] == 2
    assert d["median"] == 11.0


def test_dispersion_empty_is_all_nan():
    d = loaders.dispersion([])
    assert d["n"] == 0
    assert math.isnan(d["median"])
    assert math.isnan(d["mean"])


def test_dispersion_single_value_no_stdev():
    d = loaders.dispersion([5.0])
    assert d["n"] == 1
    assert d["median"] == 5.0
    assert math.isnan(d["stdev"])
    assert math.isnan(d["cov"])
