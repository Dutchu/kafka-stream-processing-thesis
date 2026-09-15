"""RF1 scaling and heterogeneous RF3 attempts, kept as separate populations."""
from __future__ import annotations

import argparse
import math
import statistics
from pathlib import Path

from style import plt, save_fig, write_csv


def finite(value):
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def select_runs(attempts):
    """Only RF1 R0 repeats are pooled. Every RF3 attempt remains separate."""
    reference = {
        cfg: [r for r in attempts.get(cfg, [])
              if finite(r.get('rate')) and float(r['rate']) == 0
              and str(r.get('acks')) == 'all'
              and finite(r.get('P')) and float(r['P']) == 20]
        for cfg in ('1b-rf1', '3b-rf1')
    }
    rf3 = sorted(attempts.get('3b-rf3', []), key=lambda r: (
        str(r.get('acks')) == '1',
        float(r['P']) if finite(r.get('P')) else math.inf,
        float(r['rate']) if finite(r.get('rate')) else math.inf,
        str(r.get('key')), str(r['label']).lower(), r['run']))
    return reference, rf3


def summary(rows, field):
    values = [float(r[field]) for r in rows if finite(r.get(field))]
    if not values:
        return math.nan, math.nan, math.nan
    return statistics.median(values), min(values), max(values)


def _number(value):
    return f'{value / 1000:.0f}'.replace('.', ',') if finite(value) else '—'


def fig_scalability(attempts, out_figures):
    reference, rf3 = select_runs(attempts)
    count = max(1, len(rf3))
    fig, (top, bottom) = plt.subplots(
        2, 1, figsize=(7.0, 3.0 + count * .49), sharex=True,
        gridspec_kw={'height_ratios': [2.2, count + .6]})
    fig.subplots_adjust(left=.26, right=.80, top=.94, bottom=.16, hspace=.45)
    colors = ['0.12', '0.5']
    fields = ['lambda_leo', 'lambda_jmx']
    markers = ['o', 's']
    for ax in (top, bottom):
        ax.spines[['top', 'right', 'left']].set_visible(False)
        ax.tick_params(axis='y', length=0, pad=9)
        ax.grid(axis='y', visible=False)
        ax.text(1.05, 1.02, 'HW', transform=ax.transAxes, fontsize=9, ha='center')
        ax.text(1.23, 1.02, 'JMX', transform=ax.transAxes, fontsize=9, ha='center')

    def draw(ax, y, rows, aggregate):
        mids = []
        for k, field in enumerate(fields):
            mid, lo, hi = summary(rows, field)
            mids.append(mid)
            yy = y + (k - .5) * .18
            if finite(mid):
                ax.errorbar(mid / 1000, yy,
                            xerr=[[max(0, mid-lo)/1000], [max(0, hi-mid)/1000]] if aggregate else None,
                            fmt=markers[k], color=colors[k], ms=5,
                            markerfacecolor=colors[k] if k == 0 else 'white', capsize=3)
            ax.text(1.05 + k*.18, y, _number(mid),
                    transform=ax.get_yaxis_transform(), fontsize=9, ha='center', va='center')
        if all(finite(v) for v in mids):
            ax.plot([m / 1000 for m in mids], [y-.09, y+.09],
                    color='.75', lw=.8, zorder=0)

    top.set_title('A. RF1: mediana i zakres minimum–maksimum', loc='left', pad=25)
    labels = []
    for y, (cfg, rows) in enumerate(reference.items()):
        draw(top, y, rows, True)
        labels.append(f'{cfg} · acks=all\nP20 · r=0 · n={len(rows)}')
    ideal = summary(reference['1b-rf1'], 'lambda_leo')[0] * 3
    if finite(ideal):
        top.plot(ideal / 1000, .66, marker='|', color='black', ms=12)
        top.annotate('3 × wynik 1b', (ideal / 1000, .66), xytext=(-7, -19),
                     textcoords='offset points', ha='right', fontsize=9)
    top.set_yticks(range(2), labels)
    top.set_ylim(1.65, -.55)
    top.tick_params(axis='x', labelbottom=True)

    bottom.set_title('B. RF3: osobne próby, bez wspólnej mediany', loc='left', pad=25)
    labels = []
    for y, row in enumerate(rf3):
        draw(bottom, y, [row], False)
        key = 'unikalny' if row.get('key') == 'unique' else 'strefy'
        rate = f'{float(row["rate"]):,.0f}'.replace(',', ' ') if finite(row.get('rate')) else '?'
        labels.append(f'P{row["P"]} · r={rate} · acks={row["acks"]}\n'
                      f'klucz {key} · bieg {row["run"]}')
    bottom.set_yticks(range(len(rf3)), labels)
    bottom.set_ylim(count-.5, -.65)
    if not rf3:
        bottom.text(.5, .5, 'Brak danych RF3', transform=bottom.transAxes, ha='center')
    bottom.set_xlabel('Przepustowość [tys. msg/s]\nKolumny HW i JMX w tej samej jednostce', labelpad=10)
    all_rows = [r for rows in reference.values() for r in rows] + rf3
    values = [float(r[f])/1000 for r in all_rows for f in fields if finite(r.get(f))]
    if finite(ideal):
        values.append(ideal/1000)
    bottom.set_xlim(0, max(values, default=1)*1.10)
    from matplotlib.lines import Line2D
    fig.legend(handles=[
        Line2D([], [], marker='o', color=colors[0], ls='', label='HW: zatwierdzone'),
        Line2D([], [], marker='s', color=colors[1], markerfacecolor='white', ls='', label='JMX: przyjęte przez liderów')],
        loc='lower center', bbox_to_anchor=(.53, .01), ncol=2, frameon=False)
    save_fig(fig, 'fig-scalability', out_figures)
    header = ['config', 'label', 'run', 'P', 'rate', 'acks', 'key',
              'lambda_leo', 'lambda_jmx', 'source_relative_to_results']
    write_csv('data-fig-scalability', header, [
        [r.get(k, '') for k in header[:-1]] +
        [f'E4/{r["config"]}/{r["label"]}/run{r["run"]}'] for r in all_rows], out_figures)


if __name__ == '__main__':
    from e4_witness import attempts_rows
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--out-figures', type=Path, required=True)
    args = parser.parse_args()
    fig_scalability({c: attempts_rows(args.results, c)
                     for c in ('1b-rf1', '3b-rf1', '3b-rf3')}, args.out_figures)
