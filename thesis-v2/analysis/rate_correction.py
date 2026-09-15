"""Reconstruct Rate correction from nominal E2 runs; keep rho90bp held out."""
from __future__ import annotations

import argparse
import csv
import math
import statistics
from pathlib import Path

from loaders import discover_e2, series_in_window, analysis_window
from style import plt, save_fig, write_csv

CONFIGS = ('1b-rf1', '3b-rf1')


def span(values):
    vals = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    return (statistics.median(vals), min(vals), max(vals)) if vals else (math.nan,)*3


def estimate(rows, target, mu, parallelism):
    """Per-rung empirical envelope, conditional on fixed mu, not a confidence interval."""
    eta, eta_lo, eta_hi = span(r['eta'] for r in rows if r['variant'] == '')
    nominal = target*mu/parallelism
    if not math.isfinite(eta) or eta_lo <= 0:
        raise ValueError('Positive nominal-run eta observations are required')
    return dict(eta=eta, eta_min=eta_lo, eta_max=eta_hi,
                rate_nominal=nominal, rate_corrected=nominal/eta,
                rate_min=nominal/eta_hi, rate_max=nominal/eta_lo)


def analyze_correction(results_root, mu_by_config, discovered=None):
    if discovered is None:
        discovered = {}
        for cfg in CONFIGS:
            discovered.update(discover_e2(Path(results_root), cfg))
    raw, summaries = [], []
    for cfg in CONFIGS:
        mu = mu_by_config.get(cfg, {}).get('mu_msgs', math.nan)
        if not math.isfinite(mu) or mu <= 0:
            continue
        for (c, target, variant), runs in sorted(discovered.items()):
            if c != cfg or variant not in ('', 'bp'):
                continue
            for run in runs:
                m = run.manifest
                p, rate = m.get('parallelism'), m.get('ratePerSec')
                points = [r['lambda_leo'] for r in series_in_window(run)
                          if math.isfinite(r.get('lambda_leo', math.nan))]
                if not p or not rate or not points:
                    continue
                lam = statistics.fmean(points)
                win = analysis_window(run)
                raw.append(dict(config=cfg, rho_target=target, variant=variant,
                                run=run.run_index, P=p, rate=rate, mu=mu,
                                lambda_mean=lam, rho_real=lam/mu, eta=lam/(p*rate),
                                start_ms=m['startEpochMs'], stop_ms=m['stopEpochMs'],
                                analysis_start_ms=win[0], analysis_stop_ms=win[1],
                                samples=len(points),
                                source=str(run.path.relative_to(results_root)).replace('\\', '/')))
        for target in (25, 50, 75, 90):
            cal = [r for r in raw if r['config']==cfg and r['rho_target']==target and r['variant']=='']
            test = [r for r in raw if r['config']==cfg and r['rho_target']==target and r['variant']=='bp']
            if not cal:
                continue
            if len({r['P'] for r in cal + test}) != 1:
                raise ValueError('Correction comparison requires matching parallelism')
            if test and max(r['stop_ms'] for r in cal) >= min(r['start_ms'] for r in test):
                raise ValueError('Calibration must precede held-out validation')
            p = cal[0]['P']
            s = dict(config=cfg, rho_target=target, mu=mu, P=p, n=len(cal),
                     **estimate(cal, target/100, mu, p))
            s['rho_nom_med'], s['rho_nom_min'], s['rho_nom_max'] = span(r['rho_real'] for r in cal)
            s['bp_n'] = len(test)
            s['bp_rate'], _, _ = span(r['rate'] for r in test)
            if test and len({r['rate'] for r in test}) != 1:
                raise ValueError('Different validation Rates require separate comparison rows')
            s['bp_rho_med'], s['bp_rho_min'], s['bp_rho_max'] = span(r['rho_real'] for r in test)
            s['eta_setting'] = target/100*mu/(p*s['bp_rate']) if test else math.nan
            for suffix, eta_key in [('', 'eta'), ('_min','eta_min'), ('_max','eta_max')]:
                s['bp_rho_pred'+suffix] = p*s['bp_rate']*s[eta_key]/mu if test else math.nan
            summaries.append(s)
    return raw, summaries


def figures(summaries, out_figures):
    for kind in ('rate', 'rho'):
        fig, axes = plt.subplots(2, 1, figsize=(6.3, 6.5), sharex=True)
        fig.subplots_adjust(hspace=.43, bottom=.20, top=.94, left=.13, right=.97)
        for ax, cfg in zip(axes, CONFIGS):
            rows = [r for r in summaries if r['config']==cfg]
            ax.set_title(cfg, loc='left')
            ax.spines[['top','right']].set_visible(False)
            x = [r['rho_target']/100 for r in rows]
            if not rows:
                ax.text(.5,.5,'Brak danych',transform=ax.transAxes,ha='center')
                continue
            if kind == 'rate':
                ax.plot(x, [r['rate_nominal']/1000 for r in rows], '--', color='.4', label='Rate nominalny')
                ax.fill_between(x, [r['rate_min']/1000 for r in rows], [r['rate_max']/1000 for r in rows],
                                color='.82', label='Zakres z rozrzutu η')
                ax.plot(x, [r['rate_corrected']/1000 for r in rows], '-o', color='.1', label='Rate po korekcie')
                for r in rows:
                    if r['bp_n']:
                        ax.plot(r['rho_target']/100, r['bp_rate']/1000, 'D', color='black',
                                markerfacecolor='white', ms=7, label='Rate użyty w rho90bp', zorder=5)
                        ax.text(.03,.90, f"rho90bp: {r['bp_rate']:,.0f} msg/s".replace(',', ' '),
                                transform=ax.transAxes)
                ax.set_ylabel('Rate na inwokację\n[tys. msg/s]')
                ax.set_ylim(bottom=0)
            else:
                ax.plot([.2,1], [.2,1], '--', color='.5', label='Osiągnięcie celu')
                mid=[r['rho_nom_med'] for r in rows]
                ax.errorbar(x, mid, yerr=[[m-r['rho_nom_min'] for m,r in zip(mid,rows)],
                                         [r['rho_nom_max']-m for m,r in zip(mid,rows)]],
                            fmt='o-', color='.25', capsize=3, label='Biegi nominalne: mediana i zakres')
                for r in rows:
                    if r['bp_n']:
                        ax.fill_between([.87,.93], [r['bp_rho_pred_min']]*2, [r['bp_rho_pred_max']]*2,
                                        color='.75', label='Przewidywane ρ dla Rate z rho90bp')
                        ax.plot(.9,r['bp_rho_pred'],'_',color='black',ms=12)
                        ax.errorbar(.9,r['bp_rho_med'],yerr=[[r['bp_rho_med']-r['bp_rho_min']],
                                                         [r['bp_rho_max']-r['bp_rho_med']]],
                                    fmt='D',color='black',markerfacecolor='white',capsize=4,
                                    label='rho90bp: mediana i zakres',zorder=5)
                        ax.text(.03,.90,f"rho90bp: ρ = {r['bp_rho_med']:.3f}".replace('.',','),
                                transform=ax.transAxes)
                ax.set_ylabel('Osiągnięte wykorzystanie ρ')
                ax.set_ylim(.1,1.03)
                from matplotlib.ticker import FuncFormatter
                ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:.1f}'.replace('.',',')))
            ax.set_xticks([.25,.5,.75,.9],['0,25','0,50','0,75','0,90'])
            ax.set_xlim(.20,.97)
        axes[-1].set_xlabel('Docelowe wykorzystanie ρ')
        handles, labels = axes[0].get_legend_handles_labels()
        if handles:
            fig.legend(handles,labels,loc='lower center',bbox_to_anchor=(.5,.01),ncol=1,frameon=False)
        save_fig(fig, 'fig-rate-correction' if kind=='rate' else 'fig-rate-validation', out_figures)


def produce(results_root, out_tables, out_figures, mu_by_config, discovered=None):
    raw, summaries = analyze_correction(Path(results_root), mu_by_config, discovered)
    for name, rows, out in [('tab-rate-correction', summaries, out_tables),
                            ('data-fig-rate-correction-runs', raw, out_figures),
                            ('data-fig-rate-correction', summaries, out_figures)]:
        if rows:
            header=list(rows[0])
            write_csv(name,header,[[round(r[k],6) if isinstance(r[k],float) and math.isfinite(r[k])
                                    else '' if isinstance(r[k],float) and not math.isfinite(r[k])
                                    else r[k] for k in header] for r in rows],out)
    figures(summaries, out_figures)
    return raw, summaries


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results',type=Path,required=True)
    parser.add_argument('--tables',type=Path,required=True)
    parser.add_argument('--figures',type=Path,required=True)
    args=parser.parse_args()
    with (args.tables/'tab-mu-summary.csv').open(encoding='utf-8',newline='') as f:
        mu={r['config']:{'mu_msgs':float(r['mu_committed'])} for r in csv.DictReader(f) if r['mu_committed']}
    produce(args.results,args.tables,args.figures,mu)
    import values_tex
    values_tex.write_values_tex(args.tables)
