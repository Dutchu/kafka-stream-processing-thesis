"""Categorical E4R comparisons; R0 means unlimited, never zero offered load.

Run this module to rebuild only these figures from raw run artifacts, without
rewriting tables, profiles or capacity estimates used by the manuscript.
"""
from __future__ import annotations

import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator

from loaders import dispersion
from style import save_fig, write_csv, warn


def ordered_rates(rows):
    return sorted({r['rate'] for r in rows}, key=lambda rate: (rate == 0, rate))


def _finite(rows, key, scale=1.0, positive=False):
    values = [r.get(key) for r in rows]
    return [v * scale for v in values if isinstance(v, (int, float))
            and math.isfinite(v) and (not positive or v > 0)]


def _points(ax, x, values, marker='o', color='0.15', width=.20):
    """Open markers = individual runs, filled marker + range = summary."""
    if not values:
        return
    offsets = [0] if len(values) == 1 else [
        width * (i / (len(values) - 1) - .5) for i in range(len(values))]
    ax.scatter([x + d for d in offsets], values, marker=marker,
               facecolors='white', edgecolors=color, s=26, zorder=4)
    med = dispersion(values)['median']
    ax.errorbar([x], [med], yerr=[[med - min(values)], [max(values) - med]],
                fmt=marker, color=color, markersize=6, capsize=4,
                linestyle='none', zorder=5)


def fig_e4r_saturation(config: str, ladder_rows: list[dict], mu_row: dict,
                       out_figures) -> None:
    rates = ordered_rates(ladder_rows)
    groups = [[r for r in ladder_rows if r['rate'] == rate] for rate in rates]
    fig, axes = plt.subplots(3, 1, sharex=True, figsize=(6.1, 7.5),
                             layout='constrained')
    a, b, c = axes
    for ax in axes:
        ax.grid(False, axis='x')
        ax.tick_params(labelsize=10)
        ax.margins(x=.18)
        ax.spines[['top', 'right']].set_visible(False)
    a.set_title('a) Osiągnięta przepustowość', loc='left', fontsize=11)
    b.set_title('b) Opóźnienie potwierdzenia ACK', loc='left', fontsize=11)
    c.set_title('c) Zajętość CPU brokerów', loc='left', fontsize=11)
    data_rows, run_rows = [], []
    for i, (rate, rows) in enumerate(zip(rates, groups)):
        _points(a, i, _finite(rows, 'lambda_mean', 1e-6))
        _points(b, i - .18, _finite(rows, 'ack_p50', positive=True),
                marker='s', color='0.15', width=.12)
        _points(b, i + .18, _finite(rows, 'ack_p99', positive=True),
                marker='^', color='0.5', width=.12)
        _points(c, i, _finite(rows, 'cpu_busy_max', 100))
        lam = dispersion(_finite(rows, 'lambda_mean'))
        data_rows.append((rate, lam['median'], lam['min'], lam['max'],
                          *[dispersion(_finite(rows, key))['median'] for key in
                            ('ack_p50', 'ack_p99', 'handler_idle_min', 'cpu_busy_max')]))
        for r in rows:
            run_rows.append((rate, r.get('run'), r.get('parallelism'),
                             *[r.get(k, math.nan) for k in
                               ('lambda_mean', 'ack_p50', 'ack_p99', 'cpu_busy_max')]))
    a.set_ylim(bottom=0)
    a.set_ylabel('Przepustowość [mln msg/s]', fontsize=10)
    a.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g}'.replace('.', ',')))
    b.set_yscale('log')
    b.set_ylabel('ACK [ms, skala log]', fontsize=10)
    b.yaxis.set_major_locator(LogLocator(base=10))
    for marker, color, label in [('s', '0.15', 'p50'), ('^', '0.5', 'p99')]:
        b.plot([], [], marker=marker, linestyle='none', color=color, label=label)
    b.legend(loc='upper left', bbox_to_anchor=(.60, 1.20), ncol=2,
             frameon=False, fontsize=10, handletextpad=.3, columnspacing=.7)
    c.set_ylim(0, 100)
    c.set_yticks([0, 25, 50, 75, 100])
    c.set_ylabel('CPU [%]', fontsize=10)
    c.set_xticks(range(len(rates)), [
        'Bez limitu\n(R0)' if r == 0 else f'{r:,.0f}'.replace(',', ' ')
        for r in rates])
    c.set_xlabel('Zadane tempo [msg/s na inwokację]\nPoziomy obciążenia; odstępy umowne',
                 fontsize=10)
    for ax, keys in [(a, ['lambda_mean']), (b, ['ack_p50', 'ack_p99']),
                     (c, ['cpu_busy_max'])]:
        if not any(_finite(ladder_rows, key, positive=(ax is b)) for key in keys):
            ax.text(.5, .5, 'Brak danych', transform=ax.transAxes, ha='center')
    if not rates:
        warn(f'fig-e4r-saturation-{config}: no E4R ladder rows')
    save_fig(fig, f'fig-e4r-saturation-{config}-readable', out_figures)
    header = ['rate', 'lambda_median', 'lambda_min', 'lambda_max', 'ack_p50_med',
              'ack_p99_med', 'handler_idle_min_med', 'cpu_busy_max_med']
    write_csv(f'data-fig-e4r-saturation-{config}', header, data_rows, out_figures)
    write_csv(f'data-fig-e4r-runs-{config}',
              ['rate', 'run', 'parallelism', 'lambda_mean', 'ack_p50', 'ack_p99',
               'cpu_busy_max'], run_rows, out_figures)


if __name__ == '__main__':
    import argparse
    from loaders import CONFIGS, discover_e4r
    from metrics_e4 import e4_per_run
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--out-figures', type=Path, required=True)
    args = parser.parse_args()
    for config in CONFIGS:
        grouped = {}
        for (variant, rate), runs in discover_e4r(args.results, config).items():
            for run in runs:
                row = e4_per_run(run)
                row['rate'] = rate
                grouped.setdefault(variant, []).append(row)
        if not grouped:
            grouped[config] = []
        for variant, rows in grouped.items():
            fig_e4r_saturation(variant, rows, {}, args.out_figures)
