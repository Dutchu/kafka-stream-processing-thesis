"""Horizontal-scalability figures of merit WITH measurement uncertainty.

Inputs are per-run throughput rows (E4 ladder rows for the plateau rung and
E4 *attempt* rows for irregular campaigns).  For every config we take the
runs of the saturating rung (rate 0, acks=all), compute median / SD / CoV / n
of BOTH witnesses (committed lambda_leo, accepted lambda_jmx), and derive

    S      = mu(3b-rf1) / mu(1b-rf1)          (speed-up, ideal 3)
    eff    = S / 3                            (efficiency, ideal 1)
    rf_cost = mu(3b-rf3) / mu(3b-rf1)         (replication cost, committed)

with relative errors propagated as sqrt(cov_a^2 + cov_b^2) (independent
ratios; conservative first-order propagation).
"""
from __future__ import annotations

import math

from loaders import dispersion

N_BROKERS = 3


def _sat_rows(rows: list[dict]) -> list[dict]:
    """Rows of the saturating rung: unlimited rate and acks=all (default)."""
    out = []
    for r in rows:
        rate = r.get("rate", math.nan)
        if rate == rate and float(rate) != 0.0:
            continue
        if str(r.get("acks", "all")) != "all":
            continue
        out.append(r)
    return out


def mu_summary_row(config: str, rows: list[dict]) -> dict:
    """tab-mu-summary.csv row for one config from its saturating-rung runs."""
    sat = _sat_rows(rows)
    leo = dispersion([r.get("lambda_leo", r.get("lambda_mean", math.nan)) for r in sat])
    jmx = dispersion([r.get("lambda_jmx", math.nan) for r in sat])
    mbps = dispersion([r.get("mbps", r.get("mbps_in", math.nan)) for r in sat])
    acks1 = [r for r in rows if str(r.get("acks", "all")) == "1"]
    acks1_jmx = dispersion([r.get("lambda_jmx", math.nan) for r in acks1])
    acks1_leo = dispersion([r.get("lambda_leo", math.nan) for r in acks1])
    agree = (jmx["median"] / leo["median"] if leo["median"] == leo["median"]
             and jmx["median"] == jmx["median"] and leo["median"] else math.nan)
    return {
        "config": config,
        "n": len(sat),
        "mu_committed": leo["median"],
        "mu_committed_sd": leo["stdev"],
        "mu_committed_cov": leo["cov"],
        "mu_accepted_jmx": jmx["median"],
        "mu_accepted_jmx_sd": jmx["stdev"],
        "witness_ratio": agree,
        "mu_mbps": mbps["median"],
        "acks1_n": len(acks1),
        "acks1_accepted_jmx": acks1_jmx["median"],
        "acks1_committed_leo": acks1_leo["median"],
    }


def _ratio(num: dict, den: dict, num_key="mu_committed", cov_key="mu_committed_cov"):
    a, b = num.get(num_key, math.nan), den.get(num_key, math.nan)
    if not (a == a and b == b) or b == 0:
        return math.nan, math.nan, math.nan
    val = a / b
    ca, cb = num.get(cov_key, math.nan), den.get(cov_key, math.nan)
    ca = 0.0 if ca != ca else ca
    cb = 0.0 if cb != cb else cb
    rel = math.sqrt(ca * ca + cb * cb)
    return val, val * rel, rel


def scalability_error_rows(mu_by_config: dict) -> list[dict]:
    """tab-scalability-err.csv rows: quantity, value, err_abs, err_rel, formula."""
    one = mu_by_config.get("1b-rf1", {})
    rf1 = mu_by_config.get("3b-rf1", {})
    rf3 = mu_by_config.get("3b-rf3", {})
    rows = []
    s, s_abs, s_rel = _ratio(rf1, one)
    rows.append({"quantity": "S_rf1", "value": s, "err_abs": s_abs, "err_rel": s_rel,
                 "formula": "mu(3b-rf1)/mu(1b-rf1)"})
    rows.append({"quantity": "eff_rf1", "value": s / N_BROKERS if s == s else math.nan,
                 "err_abs": s_abs / N_BROKERS if s_abs == s_abs else math.nan, "err_rel": s_rel,
                 "formula": "S_rf1/3"})
    c, c_abs, c_rel = _ratio(rf3, rf1)
    rows.append({"quantity": "rf_cost", "value": c, "err_abs": c_abs, "err_rel": c_rel,
                 "formula": "mu(3b-rf3)/mu(3b-rf1) committed"})
    inv = 1.0 / c if c == c and c else math.nan
    rows.append({"quantity": "rf_penalty", "value": inv,
                 "err_abs": inv * c_rel if inv == inv else math.nan, "err_rel": c_rel,
                 "formula": "mu(3b-rf1)/mu(3b-rf3) committed"})
    a1 = rf3.get("acks1_accepted_jmx", math.nan)
    ratio_acc = a1 / rf1.get("mu_committed", math.nan) if a1 == a1 else math.nan
    rows.append({"quantity": "acks1_accepted_vs_rf1", "value": ratio_acc, "err_abs": math.nan,
                 "err_rel": math.nan, "formula": "accepted(3b-rf3,acks=1,JMX)/mu(3b-rf1)"})
    s3, s3_abs, s3_rel = _ratio(rf3, one)
    rows.append({"quantity": "S_rf3", "value": s3, "err_abs": s3_abs, "err_rel": s3_rel,
                 "formula": "mu(3b-rf3)/mu(1b-rf1) committed"})
    return rows
