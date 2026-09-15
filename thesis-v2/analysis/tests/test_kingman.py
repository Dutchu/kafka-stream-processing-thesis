"""kingman.py tests -- the single formula source (spec-analysis.md SS5)."""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import kingman


def test_wq_acceptance_value():
    """Frozen acceptance criterion (spec-analysis.md SS5)."""
    assert kingman.wq(0.5, 1.0, 0.5, 6.0) == 4.5


def test_wq_zero_rho_is_zero():
    assert kingman.wq(0.0, 1.0, 0.5, 6.0) == 0.0


def test_wq_negative_rho_raises():
    try:
        kingman.wq(-0.1, 1.0, 0.5, 6.0)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_wq_rho_ge_one_is_clamped_not_infinite():
    v_at_clamp = kingman.wq(kingman.RHO_CLAMP, 1.0, 0.5, 6.0)
    v_over = kingman.wq(1.5, 1.0, 0.5, 6.0)
    assert v_over == v_at_clamp
    assert math.isfinite(v_over)


def test_wq_nan_safe():
    assert math.isnan(kingman.wq(math.nan, 1.0, 0.5, 6.0))
    assert math.isnan(kingman.wq(0.5, math.nan, 0.5, 6.0))
    assert math.isnan(kingman.wq(0.5, 1.0, math.nan, 6.0))
    assert math.isnan(kingman.wq(0.5, 1.0, 0.5, math.nan))


def test_pred_latency_uses_tau_component_plus_wq():
    pred = kingman.pred_latency(tau_component=8.0, rho=0.5, ca2=1.0, cs2=0.5, tau_wq=6.0)
    assert pred == 8.0 + 4.5


def test_pred_latency_nan_component():
    assert math.isnan(kingman.pred_latency(math.nan, 0.5, 1.0, 0.5, 6.0))


def test_error_basic():
    err, err_rel = kingman.error(observed=20.0, predicted=16.0)
    assert err == 4.0
    assert err_rel == 0.25


def test_error_nan_safe():
    err, err_rel = kingman.error(math.nan, 16.0)
    assert math.isnan(err) and math.isnan(err_rel)
    err, err_rel = kingman.error(20.0, math.nan)
    assert math.isnan(err) and math.isnan(err_rel)


def test_error_zero_predicted_gives_nan_rel_not_crash():
    err, err_rel = kingman.error(5.0, 0.0)
    assert err == 5.0
    assert math.isnan(err_rel)
