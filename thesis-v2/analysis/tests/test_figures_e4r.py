import math

from figures_e4r import ordered_rates, _finite, fig_e4r_saturation


def test_unlimited_is_last_and_missing_cpu_is_not_zero():
    rows = [{'rate': 0, 'cpu_busy_max': math.nan},
            {'rate': 770572, 'cpu_busy_max': None},
            {'rate': 10000, 'cpu_busy_max': .14}]
    assert ordered_rates(rows) == [10000, 770572, 0]
    assert _finite(rows, 'cpu_busy_max', 100) == [14]


def test_render_preserves_individual_runs_and_summary(tmp_path):
    import csv
    rows = [dict(rate=rate, run=i, parallelism=20, lambda_mean=value,
                 ack_p50=30, ack_p99=60, cpu_busy_max=.5,
                 handler_idle_min=math.nan)
            for rate in [0, 10000, 770572]
            for i, value in enumerate([750000, 1300000, 1330000], 1)]
    fig_e4r_saturation('3b-rf1', rows, {}, tmp_path)
    with (tmp_path / 'data-fig-e4r-saturation-3b-rf1.csv').open() as f:
        data = list(csv.DictReader(f))
    assert [int(r['rate']) for r in data] == [10000, 770572, 0]
    assert float(data[0]['lambda_median']) == 1300000
    with (tmp_path / 'data-fig-e4r-runs-3b-rf1.csv').open() as f:
        assert len(list(csv.DictReader(f))) == 9
    assert (tmp_path / 'fig-e4r-saturation-3b-rf1-readable.pdf').stat().st_size > 0


def test_empty_data_renders(tmp_path):
    fig_e4r_saturation('empty', [], {}, tmp_path)
    assert (tmp_path / 'fig-e4r-saturation-empty-readable.pdf').exists()
