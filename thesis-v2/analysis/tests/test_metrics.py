"""metrics.py tests: E1/E4/E2/scalability calculations against the
synthetic fixtures (spec-analysis.md SS2) and against hand-built minimal
RunData for the mu_status="client-limited" acceptance criterion.
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import loaders
import metrics
from loaders import RunData

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "results"



def test_e1_per_run_uses_fixture_values():
    run = loaders.discover_e1(FIXTURES, "1b-rf1")[0]
    row = metrics.e1_per_run(run)
    assert row["config"] == "1b-rf1"
    assert row["run"] == 1
    assert row["method"] == "summary_hist_p50"
    assert row["tau_ack_ms"] == 8.0
    assert 21.5 <= row["tau_e2e_ms"] <= 23.5
    assert 0.20 <= row["cs2"] <= 0.23
    assert row["cs2"] != 0.48
    assert 0.9 <= row["ca2"] <= 1.1


def test_e1_per_run_falls_back_to_median_ack_p50_without_manifest_summary():
    """When run.json lacks summary.ackP50, tau_ack_ms falls back to the
    median of ack_p50_ms over the windowed ack.json rows, and method
    records that fallback (spec-analysis.md SS2 / architect decision)."""
    run = loaders.discover_e1(FIXTURES, "1b-rf1")[0]
    original_summary = run.manifest.get("summary")
    patched_summary = dict(original_summary)
    patched_summary.pop("ackP50", None)
    run.manifest = {**run.manifest, "summary": patched_summary}
    try:
        row = metrics.e1_per_run(run)
        assert row["method"] == "median_of_ack_p50"
        assert 7.8 <= row["tau_ack_ms"] <= 8.3
    finally:
        run.manifest = {**run.manifest, "summary": original_summary}


def test_e1_summary_medians_single_run():
    run = loaders.discover_e1(FIXTURES, "1b-rf1")[0]
    row = metrics.e1_per_run(run)
    summary = metrics.e1_summary("1b-rf1", [row])
    assert summary["config"] == "1b-rf1"
    assert summary["tau_ack_med"] == row["tau_ack_ms"]
    assert math.isnan(summary["tau_ack_cov"])


def test_e1_summary_missing_config_is_nan_with_warning():
    summary = metrics.e1_summary("3b-rf1", [])
    assert summary["config"] == "3b-rf1"
    assert math.isnan(summary["tau_ack_med"])



def test_e4_signature_fires_at_p10():
    run = loaders.discover_e4(FIXTURES, "1b-rf1")[("1b-rf1", 10)][0]
    sig = metrics.e4_signature(run)
    assert sig["signature_fired"] is True
    assert sig["binding_resource"] == "handler"


def test_e4_per_run_p10_lambda_and_mbps():
    run = loaders.discover_e4(FIXTURES, "1b-rf1")[("1b-rf1", 10)][0]
    row = metrics.e4_per_run(run)
    assert row["config"] == "1b-rf1"
    assert row["P"] == 10
    assert 9400 <= row["lambda_mean"] <= 9550
    assert row["mbps"] > 1.3
    assert row["failed"] > 0
    assert "TimeoutException" in row["error_types"]
    assert row["signature_fired"] is True


def test_e4_plateau_detects_p10_as_plateau():
    by_p = loaders.discover_e4(FIXTURES, "1b-rf1")
    ladder_rows_by_p = {p: [metrics.e4_per_run(r) for r in runs] for (_c, p), runs in by_p.items()}
    mu_row = metrics.e4_plateau("1b-rf1", ladder_rows_by_p)
    assert mu_row["config"] == "1b-rf1"
    assert mu_row["P_plateau"] == 10
    assert mu_row["mu_status"] == "ok"
    assert 9400 <= mu_row["mu_msgs"] <= 9550
    assert mu_row["binding_resource"] == "handler"
    assert mu_row["p50_growth"] > 1.0


def test_e4_plateau_client_limited_when_signature_never_fires():
    """mu_status='client-limited' acceptance criterion (spec-analysis.md
    SS5) using two synthetic rows where the signature never fires."""
    rows_p10 = [{"lambda_mean": 100.0, "mbps": 0.015, "ack_p50": 10.0,
                "signature_fired": False, "binding_resource": "none"}]
    rows_p25 = [{"lambda_mean": 101.0, "mbps": 0.015, "ack_p50": 15.0,
                "signature_fired": False, "binding_resource": "none"}]
    mu_row = metrics.e4_plateau("synthetic", {10: rows_p10, 25: rows_p25})
    assert mu_row["mu_status"] == "client-limited"


def test_e4_plateau_robust_to_missing_keys():
    """metrics.e4_plateau must not KeyError on rows missing optional keys
    (e.g. 'mbps') -- real data can have NaN/absent fields upstream."""
    rows_p10 = [{"lambda_mean": 100.0, "ack_p50": 10.0, "signature_fired": True,
                "binding_resource": "cpu"}]
    mu_row = metrics.e4_plateau("synthetic-missing-keys", {10: rows_p10})
    assert mu_row["mu_status"] == "ok"
    assert math.isnan(mu_row["mu_mbps"])


def test_e4_plateau_no_data_is_client_limited():
    mu_row = metrics.e4_plateau("empty-config", {})
    assert mu_row["mu_status"] == "client-limited"
    assert math.isnan(mu_row["mu_msgs"])



def test_e2_per_run_rho_and_prediction():
    run = loaders.discover_e2(FIXTURES, "1b-rf1")[("1b-rf1", 50, "")][0]
    row = metrics.e2_per_run(run, mu_msgs=9500.0, tau_ack=8.0, tau_e2e=22.0, ca2=1.0, cs2=0.5)
    assert row["rho_target"] == 50
    assert 0.49 <= row["rho_real"] <= 0.51
    assert row["pred_ack"] > 8.0
    assert row["pred_e2e"] > 22.0
    assert row["consumer_limited"] is False
    assert math.isfinite(row["err_ack"])


def test_e2_per_run_missing_mu_gives_nan_rho():
    run = loaders.discover_e2(FIXTURES, "1b-rf1")[("1b-rf1", 50, "")][0]
    row = metrics.e2_per_run(run, mu_msgs=math.nan, tau_ack=8.0, tau_e2e=22.0, ca2=1.0, cs2=0.5)
    assert math.isnan(row["rho_real"])
    assert math.isnan(row["pred_ack"])


def test_e2_summary_and_kingman_error_row():
    run = loaders.discover_e2(FIXTURES, "1b-rf1")[("1b-rf1", 50, "")][0]
    row = metrics.e2_per_run(run, mu_msgs=9500.0, tau_ack=8.0, tau_e2e=22.0, ca2=1.0, cs2=0.5)
    summary = metrics.e2_summary("1b-rf1", 50, [row])
    assert summary["rho_target"] == 50
    assert summary["rho_real_med"] == row["rho_real"]

    err_row = metrics.kingman_error_row("1b-rf1", {50: [row], 25: [], 75: [], 90: []})
    assert err_row["config"] == "1b-rf1"
    assert math.isfinite(err_row["MAE_ack_ms"])
    assert err_row["best_rho"] == 50
    assert err_row["worst_rho"] == 50


def test_kingman_error_row_no_data():
    err_row = metrics.kingman_error_row("empty", {25: [], 50: [], 75: [], 90: []})
    assert math.isnan(err_row["MAE_ack_ms"])



def test_scalability_rows_partial_data_yields_nan_not_crash():
    mu_by_config = {"1b-rf1": {"mu_msgs": 9500.0, "mu_mbps": 1.4}}
    tau_ack = {"1b-rf1": 8.0}
    tau_e2e = {"1b-rf1": 22.0}
    rows = metrics.scalability_rows(mu_by_config, tau_ack, tau_e2e)
    assert len(rows) == 4
    mu_row = next(r for r in rows if r["metric"] == "mu_msgs")
    assert mu_row["1b-rf1"] == 9500.0
    assert math.isnan(mu_row["3b-rf1"])
    assert math.isnan(mu_row["S_rf1"])


def test_scalability_rows_full_data_computes_speedup():
    mu_by_config = {
        "1b-rf1": {"mu_msgs": 100.0, "mu_mbps": 1.0},
        "3b-rf1": {"mu_msgs": 280.0, "mu_mbps": 2.8},
        "3b-rf3": {"mu_msgs": 150.0, "mu_mbps": 1.5},
    }
    tau_ack = {"1b-rf1": 8.0, "3b-rf1": 8.5, "3b-rf3": 9.0}
    tau_e2e = {"1b-rf1": 22.0, "3b-rf1": 23.0, "3b-rf3": 24.0}
    rows = metrics.scalability_rows(mu_by_config, tau_ack, tau_e2e)
    mu_row = next(r for r in rows if r["metric"] == "mu_msgs")
    assert math.isclose(mu_row["S_rf1"], 2.8)
    assert math.isclose(mu_row["eff_rf1"], 2.8 / 3.0)
    assert math.isclose(mu_row["rf_cost"], 150.0 / 280.0)
    assert math.isclose(mu_row["S_rf3"], 1.5)
