"""Every frozen figure (spec-analysis.md SS3) -> --out-figures, PDF+PNG,
each with a twin data-fig-*.csv (plotted data) in the same directory.

All figures ALWAYS get written, even with no/partial data (an empty axes
with a "no data" annotation is acceptable; the acceptance criterion is
file existence, spec-analysis.md SS4/SS5), so W6 LaTeX guards never see a
missing figure file.
"""
from __future__ import annotations

import math

import matplotlib.pyplot as plt

import kingman
from loaders import CONFIGS, dispersion
from style import save_fig, write_csv, config_style, place_legend, warn


def _no_data(ax, msg="brak danych"):
    ax.text(0.5, 0.5, msg, ha="center", va="center", transform=ax.transAxes,
            fontsize=9, color="0.4")



def _rho_curve(ca2, cs2, tau_ack, tau_e2e, n=96):
    rhos = [i / (n - 1) * 0.95 for i in range(n)]
    pred_ack = [kingman.pred_latency(tau_ack, r, ca2, cs2, tau_ack) for r in rhos]
    pred_e2e = [kingman.pred_latency(tau_e2e, r, ca2, cs2, tau_ack) for r in rhos]
    return rhos, pred_ack, pred_e2e


def fig_kingman_config(config: str, profile: dict, e2_summary_rows: list[dict],
                       out_figures) -> None:
    """fig-kingman-<config>: pred_ack(rho)/pred_e2e(rho) curve + measured
    points (medians, min-max bars) with rho_real labels."""
    fig, ax = plt.subplots()
    ca2 = profile.get("ca2", math.nan)
    cs2 = profile.get("cs2", math.nan)
    tau_ack = profile.get("tauAckMs", math.nan)
    tau_e2e = profile.get("tauE2eMs", math.nan)

    data_rows = []
    have_curve = all(v == v for v in (ca2, cs2, tau_ack, tau_e2e))
    if have_curve:
        rhos, pred_ack, pred_e2e = _rho_curve(ca2, cs2, tau_ack, tau_e2e)
        ax.plot(rhos, pred_ack, "-", color="0.1", label="pred ACK")
        ax.plot(rhos, pred_e2e, "--", color="0.5", label="pred e2e")
        data_rows = list(zip(rhos, pred_ack, pred_e2e))
    else:
        warn(f"fig-kingman-{config}: profile incomplete (ca2/cs2/tau missing); curve omitted")
        _no_data(ax)

    for r in e2_summary_rows:
        rho = r.get("rho_real_med")
        if rho == rho:
            ax.scatter([rho], [r.get("ack_mean_med")], marker="o", color="0.1", zorder=5)
            ax.scatter([rho], [r.get("e2e_mean_med")], marker="s", color="0.5", zorder=5)
            ax.annotate(f"{rho:.2f}", (rho, r.get("ack_mean_med", math.nan)),
                        textcoords="offset points", xytext=(4, 4), fontsize=7)

    ax.set_xlabel(r"$\rho$")
    ax.set_ylabel("opoznienie (ms, log)")
    ax.set_yscale("log")
    ax.set_title(f"Kingman: {config}")
    place_legend(ax)
    save_fig(fig, f"fig-kingman-{config}", out_figures)

    header = ["rho", "pred_ack_ms", "pred_e2e_ms"]
    write_csv(f"data-fig-kingman-{config}", header, data_rows, out_figures)


def fig_kingman_all(profiles_by_config: dict, out_figures) -> None:
    """fig-kingman-all: 3 configs' ACK prediction curves overlaid."""
    fig, ax = plt.subplots()
    data_rows = []
    any_curve = False
    for config in CONFIGS:
        profile = profiles_by_config.get(config, {})
        ca2 = profile.get("ca2", math.nan)
        cs2 = profile.get("cs2", math.nan)
        tau_ack = profile.get("tauAckMs", math.nan)
        style = config_style(config)
        if all(v == v for v in (ca2, cs2, tau_ack)):
            rhos, pred_ack, _pred_e2e = _rho_curve(ca2, cs2, tau_ack, tau_ack)
            ax.plot(rhos, pred_ack, linestyle=style["linestyle"],
                    color=style["color"], marker="", label=config)
            for rho, pa in zip(rhos, pred_ack):
                data_rows.append((config, rho, pa))
            any_curve = True
        else:
            warn(f"fig-kingman-all: profile incomplete for config={config}; curve omitted")
    if not any_curve:
        _no_data(ax)
    ax.set_xlabel(r"$\rho$")
    ax.set_ylabel("pred ACK (ms)")
    ax.set_title("Kingman: porownanie konfiguracji")
    place_legend(ax)
    save_fig(fig, "fig-kingman-all", out_figures)
    write_csv("data-fig-kingman-all", ["config", "rho", "pred_ack_ms"], data_rows, out_figures)


def fig_kingman_error(kingman_error_by_config: dict, e2_summary_rows_all: list[dict],
                      out_figures) -> None:
    """fig-kingman-error: bars of relative error per rung x config."""
    fig, ax = plt.subplots()
    rhos = [25, 50, 75, 90]
    width = 0.8 / max(len(CONFIGS), 1)
    data_rows = []
    any_bar = False
    for i, config in enumerate(CONFIGS):
        vals = []
        for rho in rhos:
            match = [r for r in e2_summary_rows_all
                     if r.get("config") == config and r.get("rho_target") == rho]
            v = match[0].get("err_ack_rel_med") if match else math.nan
            vals.append(v)
            data_rows.append((config, rho, v))
        xs = [x + i * width for x in range(len(rhos))]
        plot_vals = [0.0 if v != v else v for v in vals]
        if any(v == v for v in vals):
            any_bar = True
        style = config_style(config)
        ax.bar(xs, plot_vals, width=width, color=style["color"], label=config)
    if not any_bar:
        _no_data(ax)
    ax.set_xticks([x + width for x in range(len(rhos))])
    ax.set_xticklabels([f"{r/100:.2f}" for r in rhos])
    ax.set_xlabel(r"$\rho$ target")
    ax.set_ylabel(r"$\varepsilon_{rel}$ ACK")
    ax.set_title("Blad wzgledny Kingmana")
    place_legend(ax)
    save_fig(fig, "fig-kingman-error", out_figures)
    write_csv("data-fig-kingman-error", ["config", "rho_target", "err_ack_rel_med"], data_rows, out_figures)



def fig_e4_saturation(config: str, ladder_rows: list[dict], mu_row: dict,
                      out_figures) -> None:
    """fig-e4-saturation-<config>: lambda(P) + ack p50/p99(P, log) with
    P_plateau marked, plus handler_idle_min/cpu_busy_max sub-panel."""
    ps = sorted({r["P"] for r in ladder_rows})
    lam_med, lam_min, lam_max = [], [], []
    p50_med, p99_med = [], []
    handler_med, cpu_med = [], []
    data_rows = []
    for p in ps:
        rows = [r for r in ladder_rows if r["P"] == p]
        lam = dispersion([r["lambda_mean"] for r in rows])
        p50 = dispersion([r["ack_p50"] for r in rows])["median"]
        p99 = dispersion([r["ack_p99"] for r in rows])["median"]
        handler = dispersion([r["handler_idle_min"] for r in rows])["median"]
        cpu = dispersion([r["cpu_busy_max"] for r in rows])["median"]
        lam_med.append(lam["median"]); lam_min.append(lam["min"]); lam_max.append(lam["max"])
        p50_med.append(p50); p99_med.append(p99)
        handler_med.append(handler); cpu_med.append(cpu)
        data_rows.append((p, lam["median"], lam["min"], lam["max"], p50, p99, handler, cpu))

    fig, (ax1, ax3) = plt.subplots(2, 1, sharex=True, height_ratios=[3, 1])
    if ps:
        ax1.errorbar(ps, lam_med, yerr=[
            [m - lo for m, lo in zip(lam_med, lam_min)],
            [hi - m for m, hi in zip(lam_med, lam_max)],
        ], fmt="o-", color="0.1", label=r"$\lambda$ (msg/s)")
        ax2 = ax1.twinx()
        ax2.plot(ps, p50_med, "s--", color="0.4", label="ACK p50")
        ax2.plot(ps, p99_med, "^:", color="0.6", label="ACK p99")
        ax2.set_yscale("log")
        ax2.set_ylabel("ACK p50/p99 (ms, log)")
        p_plateau = mu_row.get("P_plateau") if mu_row else None
        if p_plateau == p_plateau and p_plateau in ps:
            ax1.axvline(p_plateau, color="0.3", linestyle=":", linewidth=1)
        ax3.plot(ps, handler_med, "o-", color="0.1", label="handler_idle_min")
        ax3.plot(ps, cpu_med, "s--", color="0.5", label="cpu_busy_max")
        place_legend(ax3, ncol=2)
    else:
        _no_data(ax1)
        warn(f"fig-e4-saturation-{config}: no E4 ladder rows")
    ax1.set_ylabel(r"$\lambda$ (msg/s)")
    ax3.set_xlabel("P (rownoleglosc)")
    ax1.set_title(f"Saturacja E4: {config}")
    save_fig(fig, f"fig-e4-saturation-{config}", out_figures)

    header = ["P", "lambda_median", "lambda_min", "lambda_max", "ack_p50_med",
              "ack_p99_med", "handler_idle_min_med", "cpu_busy_max_med"]
    write_csv(f"data-fig-e4-saturation-{config}", header, data_rows, out_figures)


from figures_e4r import fig_e4r_saturation


def fig_omb_vs_functions_ladder(omb_config_hw: str, omb_ladder: list[dict],
                                fn_rows: list[dict], out_figures) -> None:
    """fig-omb-vs-functions-ladder: offered (x) vs achieved (y) per source.

    OMB points: thr_target -> rps_med (+-rps_sd, finite throttles only).
    Functions points: rate*parallelism -> lambda_mean, grouped per rate with
    median/min/max whiskers. Diagonal y=x = ideal. NOTE in title: payloads
    differ (OMB ~1KB vs functions ~150B) — msg/s comparable, MB/s is not.
    """
    fig, ax = plt.subplots()
    data_rows = []
    omb_pts = [(r["thr_target"], r["rps_med"], r["rps_sd"], r["rung"])
               for r in omb_ladder
               if r.get("thr_target") == r.get("thr_target")
               and r.get("thr_target") != math.inf and (r.get("n") or 0) > 0]
    fn_by_rate: dict = {}
    for r in fn_rows:
        try:
            offered = float(r.get("rate", math.nan)) * float(r.get("parallelism", math.nan))
        except (TypeError, ValueError):
            continue
        if offered == offered:
            fn_by_rate.setdefault(offered, []).append(r.get("lambda_mean", math.nan))
    if omb_pts:
        xs = [p[0] for p in omb_pts]
        ys = [p[1] for p in omb_pts]
        es = [p[2] if p[2] == p[2] else 0.0 for p in omb_pts]
        ax.errorbar(xs, ys, yerr=es, fmt="o-", color="0.1", label="OMB (1 generator)")
        for x, y, _e, rung in omb_pts:
            data_rows.append(("OMB", rung, x, y))
    fn_xs = sorted(fn_by_rate)
    if fn_xs:
        fmed = [dispersion(fn_by_rate[x])["median"] for x in fn_xs]
        flo = [dispersion(fn_by_rate[x])["min"] for x in fn_xs]
        fhi = [dispersion(fn_by_rate[x])["max"] for x in fn_xs]
        ax.errorbar(fn_xs, fmed,
                    yerr=[[m - lo for m, lo in zip(fmed, flo)],
                          [hi - m for m, hi in zip(fmed, fhi)]],
                    fmt="s--", color="0.5", label="Funkcje (P=20, tuned)")
        for x in fn_xs:
            d = dispersion(fn_by_rate[x])
            data_rows.append(("funkcje", "", x, d["median"]))
    if not omb_pts and not fn_xs:
        _no_data(ax)
        warn(f"fig-omb-vs-functions-ladder-{omb_config_hw}: no ladder data")
    else:
        lo = min([p[0] for p in omb_pts] + fn_xs)
        hi = max([p[0] for p in omb_pts] + fn_xs)
        ax.plot([lo, hi], [lo, hi], ":", color="0.7", linewidth=1, label="ideal (y=x)")
        ax.set_xscale("log")
        ax.set_yscale("log")
    ax.set_xlabel("oferowane (msg/s, log)")
    ax.set_ylabel("dowiezione (msg/s, log)")
    ax.set_title(f"OMB vs funkcje: {omb_config_hw} (OMB ~1KB, funkcje ~150B)")
    place_legend(ax)
    save_fig(fig, f"fig-omb-vs-functions-ladder-{omb_config_hw}", out_figures)
    write_csv(f"data-fig-omb-vs-functions-ladder-{omb_config_hw}",
              ["source", "rung", "offered", "achieved"], data_rows, out_figures)



def fig_e4_timeseries(config: str, p_plateau, series_rows: list[dict], out_figures) -> None:
    """fig-e4-timeseries-<config>-P<Pplateau>: 1Hz trace of one plateau run
    (lambda, ack p50, consumer_lag) as evidence of p50/queue growth."""
    name_p = p_plateau if p_plateau == p_plateau and p_plateau is not None else "NA"
    fig, ax1 = plt.subplots()
    data_rows = []
    if series_rows:
        t0 = series_rows[0].get("ts_ms", 0.0)
        ts_s = [(r.get("ts_ms", math.nan) - t0) / 1000.0 for r in series_rows]
        lam = [r.get("lambda_leo", math.nan) for r in series_rows]
        p50 = [r.get("ack_p50_ms", math.nan) for r in series_rows]
        lag = [r.get("consumer_lag", math.nan) for r in series_rows]
        ax1.plot(ts_s, lam, "-", color="0.1", label=r"$\lambda$ (msg/s)")
        ax2 = ax1.twinx()
        ax2.plot(ts_s, p50, "--", color="0.4", label="ACK p50 (ms)")
        ax2.plot(ts_s, lag, ":", color="0.6", label="lag (msgs)")
        data_rows = list(zip(ts_s, lam, p50, lag))
        place_legend(ax1, ncol=1)
    else:
        _no_data(ax1)
        warn(f"fig-e4-timeseries-{config}-P{name_p}: no series rows available")
    ax1.set_xlabel("t (s od poczatku okna)")
    ax1.set_ylabel(r"$\lambda$ (msg/s)")
    ax1.set_title(f"Przebieg 1 Hz na plateau: {config}, P={name_p}")
    save_fig(fig, f"fig-e4-timeseries-{config}-P{name_p}", out_figures)
    write_csv(f"data-fig-e4-timeseries-{config}-P{name_p}",
              ["t_s", "lambda_leo", "ack_p50_ms", "consumer_lag"], data_rows, out_figures)



from figures_scalability import fig_scalability



from figures_e1 import fig_e1_tau
