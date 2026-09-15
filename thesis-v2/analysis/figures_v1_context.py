"""Redraw archived v1 figure data; no experiments or model refitting."""
import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt
from style import save_fig, write_csv


def read_data(folder, name):
    with (folder / name).open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        return reader.fieldnames, list(reader)


def render(source, output):
    specs = [
        ('pacelc-latency', 'e3'), ('partition-availability', 'e5'),
        ('drain', 'e6'), ('evt-ccdf', 'e7')]
    for suffix, exp in specs:
        fields, rows = read_data(source, f'data-fig-{suffix}.csv')
        fig, ax = plt.subplots(figsize=(6.2, 3.7))
        styles = [('o', '-', '0.1'), ('s', '--', '0.45'), ('^', ':', '0.65')]
        if exp == 'e3':
            configs = ['fire', 'pael', 'pcec']
            for k, percentile in enumerate(['p50', 'p95', 'p99']):
                selected = {r['config']: r for r in rows if r['percentile'] == percentile}
                values = [float(selected[c]['median_ms']) for c in configs]
                lo = [values[i] - float(selected[c]['min_ms']) for i, c in enumerate(configs)]
                hi = [float(selected[c]['max_ms']) - values[i] for i, c in enumerate(configs)]
                marker, line, color = styles[k]
                ax.errorbar([i+(k-1)*.17 for i in range(3)], values, yerr=[lo, hi],
                            fmt=marker, color=color, capsize=3, label=percentile)
            ax.set_xticks(range(3), ['acks=0', 'acks=1', 'acks=all'])
            ax.set_ylabel('Opóźnienie publikacji [ms]')
            ax.set_ylim(bottom=0)
        elif exp == 'e5':
            for k, config in enumerate(sorted({r['config'] for r in rows})):
                selected = [r for r in rows if r['config'] == config]
                marker, line, color = styles[k % len(styles)]
                ax.plot([float(r['t_s']) for r in selected],
                        [float(r['throughput_mb_per_sec']) for r in selected],
                        linestyle=line, color=color, label={'pael':'acks=1', 'pcec':'acks=all'}.get(config, config))
            ax.set_xlabel('Czas od początku wysyłania [s]')
            ax.set_ylabel('Przepustowość [MB/s]')
            ax.set_ylim(bottom=0)
        elif exp == 'e6':
            selected_rows = [r for r in rows if r['source'] == 'perf-deficit'
                             and r['t_s'] and r['value']]
            for k, config in enumerate(sorted({r['config'] for r in selected_rows})):
                selected = [r for r in selected_rows if r['config'] == config]
                marker, line, color = styles[k % len(styles)]
                ax.plot([float(r['t_s']) for r in selected],
                        [float(r['value']) / 1e6 for r in selected],
                        linestyle=line, color=color, label='Wariant ' + config.replace('.', ','))
            ax.set_xlabel('Czas od początku wysyłania [s]')
            ax.set_ylabel('Deficyt realizacji tempa [MB]')
        else:
            percentiles = ['p50_ms', 'p95_ms', 'p99_ms', 'p999_ms']
            for k, config in enumerate(sorted({r['config'] for r in rows})):
                selected = {r['percentile']: r for r in rows if r['config'] == config}
                marker, line, color = styles[k % len(styles)]
                ax.plot(range(4), [float(selected[p]['latency_ms']) for p in percentiles],
                        marker=marker, linestyle=line, color=color,
                        label={'mixed':'Rozmiary mieszane', 'uniform':'Rozmiar jednolity'}.get(config, config))
            ax.set_xticks(range(4), ['p50', 'p95', 'p99', 'p99,9'])
            ax.set_ylabel('Opóźnienie publikacji [ms]')
            ax.set_xlabel('Percentyl (punkty zagregowane)')
            ax.set_ylim(bottom=0)
        ax.spines[['top','right']].set_visible(False)
        ax.legend(loc='upper center', bbox_to_anchor=(.5,-.2), ncol=3, frameon=False)
        fig.tight_layout()
        name = f'fig-v1-context-{exp}'
        save_fig(fig, name, output)
        write_csv('data-' + name, fields, [[r[f] for f in fields] for r in rows], output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--out-figures', type=Path, required=True)
    args = parser.parse_args()
    render(args.source, args.out_figures)
