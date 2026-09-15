"""Tests for omb_import.py (E0 perf-test ladder -> plateau mu + OMB profile)."""
import math

import pytest

from omb_import import (
    discover_omb_e2e,
    ladder_rows,
    omb_profile_row,
    omb_profiles_json_entry,
    parse_consumer_summary,
    parse_perf_aggregate,
    plateau_mu,
    throttle_of,
)

SAMPLE = """50000 records sent, 10000.0 records/sec (9.77 MB/sec), 1.0 ms avg latency, 10.0 ms max latency.
50005 records sent, 10001.0 records/sec (9.77 MB/sec), 1.0 ms avg latency, 3.0 ms max latency.
3000000 records sent, 9999.200064 records/sec (9.76 MB/sec), 2.05 ms avg latency, 840.00 ms max latency, 1 ms 50th, 3 ms 95th, 17 ms 99th, 191 ms 99.9th.
"""


def test_parse_aggregate_takes_last_line():
    agg = parse_perf_aggregate(SAMPLE)
    assert agg["records"] == 3000000
    assert agg["rps"] == pytest.approx(9999.200064)
    assert agg["p50_ms"] == 1.0
    assert agg["p99_ms"] == 17.0


def test_parse_aggregate_none_without_final_line():
    assert parse_perf_aggregate("50000 records sent, 1.0 records/sec (1 MB/sec), 1.0 ms avg latency, 2.0 ms max latency.\n") is None


def test_throttle_of():
    assert throttle_of("T10") == 10.0
    assert throttle_of("T200") == 200.0
    assert throttle_of("Tinf") == math.inf
    assert throttle_of("Tinf2x") == math.inf
    assert throttle_of("run1") != throttle_of("run1")


def _by_rung():
    def obs(rps, p50):
        return {"rps": rps, "mbps": rps / 1024, "avg_ms": 5.0,
                "p50_ms": p50, "p95_ms": 3.0, "p99_ms": 100.0, "run": "run1"}
    return {
        "T10": [obs(9999.0, 1.0), obs(9998.0, 1.0), obs(10000.0, 1.0)],
        "T100": [obs(77417.0, 116.0), obs(81000.0, 151.0), obs(76000.0, 131.0)],
        "T200": [obs(79091.0, 150.0), obs(78500.0, 169.0), obs(79500.0, 170.0)],
    }


def test_plateau_detected():
    ladder = ladder_rows("1b-rf1-esmall", _by_rung())
    mu = plateau_mu(ladder)
    assert mu["mu_status"] == "plateau"
    assert mu["mu_msgs"] == pytest.approx(79091.0)
    assert mu["mu_n"] == 3
    assert mu["mu_cov"] == mu["mu_cov"]


def test_plateau_still_scaling():
    by_rung = {"T10": [{"rps": 9999.0, "mbps": 9.7, "avg_ms": 2.0, "p50_ms": 1.0,
                        "p95_ms": 3.0, "p99_ms": 17.0, "run": "run1"}],
               "T25": [{"rps": 24994.0, "mbps": 24.4, "avg_ms": 3.5, "p50_ms": 1.0,
                        "p95_ms": 3.0, "p99_ms": 87.0, "run": "run1"}]}
    mu = plateau_mu(ladder_rows("1b-rf1-esmall", by_rung))
    assert mu["mu_status"] == "still-scaling"
    assert mu["mu_msgs"] != mu["mu_msgs"]


def test_profile_row_has_errors_and_method():
    by_rung = _by_rung()
    ladder = ladder_rows("1b-rf1-esmall", by_rung)
    mu = plateau_mu(ladder)
    row = omb_profile_row("1b-rf1-esmall", by_rung, mu)
    assert row["mu_msgs"] == pytest.approx(79091.0)
    assert row["mu_n"] == 3
    assert row["tau_ack_ms"] == pytest.approx(1.0)
    assert row["tau_ack_sd"] == pytest.approx(0.0)
    assert "plateau" in row["method"]


def test_profiles_json_entry_omits_unknown():
    row = {"config": "1b-rf1-esmall", "mu_msgs": 79091.0, "method": "x"}
    e = omb_profiles_json_entry(row)
    assert e["muMsgs"] == pytest.approx(79091.0)
    assert "tauAckMs" not in e
    assert "tauE2eMs" not in e
    assert e["config"] == "1b-rf1"
    assert e["source"].startswith("OMB(")


def test_parse_consumer_summary_fetch_per_msg():
    text = ("2026-09-10T12:00:00, 2026-09-10T12:01:00, 100.00, 1.67, "
            "102400, 1706.67, 10, 55000, 1.82, 1861.82\n")
    s = parse_consumer_summary(text)
    assert s["nmsg"] == pytest.approx(102400.0)
    assert s["fetch_ms_per_msg"] == pytest.approx(55000.0 / 102400.0)


def test_parse_consumer_summary_none_without_csv():
    assert parse_consumer_summary("no numbers here\n") is None


def test_profile_row_tau_e2e_from_consumer(tmp_path):
    by_rung = {"T10": [{"rps": 9999.0, "mbps": 9.7, "avg_ms": 2.0, "p50_ms": 1.0,
                        "p95_ms": 3.0, "p99_ms": 17.0, "run": "run1"}]}
    mu = {"mu_msgs": 79091.0, "mu_sd": 935.0, "mu_cov": 0.012, "mu_n": 3,
          "rungs": "T100,T200", "mu_status": "plateau"}
    row = omb_profile_row("1b-rf1-esmall", by_rung, mu, {"T10": [2.5, 3.5]})
    assert row["tau_e2e_ms"] == pytest.approx(3.0)
    assert "fetch-proxy" in row["method"]
    row2 = omb_profile_row("1b-rf1-esmall", by_rung, mu, {})
    assert row2["tau_e2e_ms"] != row2["tau_e2e_ms"]
