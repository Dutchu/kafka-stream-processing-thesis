"""Kingman (VUT) approximation -- the ONLY place in the pipeline that
contains this formula (spec-analysis.md SS2, experiment-plan.md SS1).

    E[W_q] ~= rho/(1-rho) * (ca2+cs2)/2 * tau
    L_pred  = tau + E[W_q]
    err     = L_obs - L_pred
    err_rel = err / L_pred

`tau` here is the service/RTT baseline used as the common multiplier in the
VUT equation (ACK baseline latency from E1, per spec-analysis.md SS2 E2).
Every caller (metrics.py, dashboard reference, tests) must import this
module instead of re-deriving the formula.
"""
from __future__ import annotations

import math

RHO_CLAMP = 0.999


def wq(rho: float, ca2: float, cs2: float, tau: float) -> float:
    """Kingman mean queueing-wait approximation, same units as tau.

    wq(0.5, 1.0, 0.5, 6.0) == 4.5 (acceptance criterion, spec-analysis.md SS5).
    NaN-safe: any NaN input propagates to NaN (never raises).
    """
    if rho != rho or ca2 != ca2 or cs2 != cs2 or tau != tau:
        return math.nan
    if rho < 0.0:
        raise ValueError("rho must be >= 0")
    rho_eff = min(rho, RHO_CLAMP)
    return rho_eff / (1.0 - rho_eff) * (ca2 + cs2) / 2.0 * tau


def pred_latency(tau_component: float, rho: float, ca2: float, cs2: float,
                  tau_wq: float) -> float:
    """L_pred = tau_component + W_q(rho, ca2, cs2, tau_wq).

    `tau_component` is the additive baseline latency for the predicted
    quantity (tau_ack for pred_ack, tau_e2e for pred_e2e); `tau_wq` is the
    service-time multiplier inside W_q, which is always tau_ack (spec-
    analysis.md SS2 E2: "parametry z E1 tej konfiguracji" -- tau_ack is the
    service/RTT time used inside the VUT product for both predictions,
    matching MathEngine.java in spec-dashboard.md R4).
    """
    w = wq(rho, ca2, cs2, tau_wq)
    if tau_component != tau_component:
        return math.nan
    return tau_component + w


def error(observed: float, predicted: float):
    """(err, err_rel) tuple; NaN-safe (NaN in -> NaN out, never raises)."""
    if observed != observed or predicted != predicted:
        return math.nan, math.nan
    err = observed - predicted
    err_rel = math.nan if predicted == 0 else err / predicted
    return err, err_rel
