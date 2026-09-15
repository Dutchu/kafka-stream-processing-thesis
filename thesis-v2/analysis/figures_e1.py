"""E1 run-level latency comparisons with separate ACK and e2e scales."""
from __future__ import annotations

import math

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter

from loaders import CONFIGS, dispersion
from style import save_fig, warn, write_csv


def latency_summary(rows, key):
    values = [r[key] for r in rows if isinstance(r.get(key), (int, float))
              and math.isfinite(r[key])]
    return values, dispersion(values)


def fig_e1_tau(e1_tau_rows: list[dict], out_figures) -> None:
    """Hollow run points; filled median and min-max range, never a CI."""
    configs = list(CONFIGS)
    configs += sorted({r['config'] for r in e1_tau_rows} - set(configs))
    fig, axes = plt.subplots(2, 1, figsize=(6.2, 5.8))
    aggregate_rows, run_rows = [], []
    for config in configs:
        rows = sorted([r for r in e1_tau_rows if r['config'] == config],
                      key=lambda r: r.get('run', 0))
        summaries = [latency_summary(rows, key)[1]
                     for key in ('tau_ack_ms', 'tau_e2e_ms')]
        aggregate_rows.append((config, *[d[k] for d in summaries
                                        for k in ('median', 'min', 'max')]))
        run_rows.extend((config, r.get('run'), r.get('tau_ack_ms', math.nan),
                         r.get('tau_e2e_ms', math.nan)) for r in rows)

    any_point = False
    for ax, key, title in zip(axes, ('tau_ack_ms', 'tau_e2e_ms'),
                              ('(a) Potwierdzenie zapisu — ACK',
                               '(b) Opóźnienie end-to-end — e2e')):
        largest = 0.0
        for i, config in enumerate(configs):
            rows = sorted([r for r in e1_tau_rows if r['config'] == config],
                          key=lambda r: r.get('run', 0))
            values, d = latency_summary(rows, key)
            if not values:
                ax.text(.98, i, 'brak danych', transform=ax.get_yaxis_transform(),
                        ha='right', va='center', fontsize=9)
                continue
            any_point = True
            largest = max(largest, d['max'])
            offsets = [0] if len(values) == 1 else [
                .16 * (j / (len(values) - 1) - .5) for j in range(len(values))]
            ax.scatter(values, [i - .13 + o for o in offsets], s=30,
                       facecolors='white', edgecolors='0.15', zorder=4)
            ax.errorbar(d['median'], i + .17,
                        xerr=[[d['median'] - d['min']], [d['max'] - d['median']]],
                        fmt='D', color='0.15', markersize=4, capsize=3,
                        linewidth=1.4, zorder=3)
            label = f"{d['median']:.2f} ms; n={d['n']}".replace('.', ',')
            ax.text(.98, i + .17, label, transform=ax.get_yaxis_transform(),
                    ha='right', va='center', fontsize=9)
        ax.set_xlim(0, largest * 1.42 if largest > 0 else 1)
        ax.set_ylim(len(configs) - .5, -.5)
        ax.set_yticks(range(len(configs)), configs)
        ax.set_xlabel('Opóźnienie [ms]')
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, pos: f'{v:g}'.replace('.', ',')))
        ax.set_title(title, loc='left', fontsize=10)
        ax.grid(False, axis='y')
        ax.spines[['top', 'right']].set_visible(False)
    if not any_point:
        warn('fig-e1-tau: no E1 data for any config')
    fig.legend(handles=[
        Line2D([], [], marker='o', markerfacecolor='white', color='0.15',
               linestyle='none', label='Pojedynczy bieg'),
        Line2D([], [], marker='D', color='0.15', markersize=4,
               label='Mediana i minimum–maksimum')],
        loc='lower center', ncol=2, frameon=False, fontsize=9)
    fig.tight_layout(rect=(0, .07, 1, 1), h_pad=2)
    save_fig(fig, 'fig-e1-tau', out_figures)
    write_csv('data-fig-e1-tau',
              ['config', 'tau_ack_med', 'tau_ack_min', 'tau_ack_max',
               'tau_e2e_med', 'tau_e2e_min', 'tau_e2e_max'], aggregate_rows, out_figures)
    write_csv('data-fig-e1-tau-runs',
              ['config', 'run', 'tau_ack_ms', 'tau_e2e_ms'], run_rows, out_figures)


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    from loaders import discover_e1
    from metrics_e1 import e1_per_run

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', required=True, type=Path)
    parser.add_argument('--out-figures', required=True, type=Path)
    args = parser.parse_args()
    rows = [e1_per_run(run) for config in CONFIGS
            for run in discover_e1(args.results, config)]
    fig_e1_tau(rows, args.out_figures)
