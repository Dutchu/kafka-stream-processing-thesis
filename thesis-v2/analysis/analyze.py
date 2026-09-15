#!/usr/bin/env python3
"""thesis-v2 analysis pipeline entry point (spec-analysis.md SS4).

    python analyze.py --results DIR --out-tables DIR --out-figures DIR --profiles PATH

Reads results/<EXP>/<CONFIG>/[P<P>|rho<R>/]run<N>/ (experiment-plan.md
SS5.1), computes E1/E4/E2/scalability metrics (metrics.py, formula in
kingman.py), and writes EVERY frozen table (tables.py) and figure
(figures.py), plus analysis/output/profiles.json and
analysis/output/summary.md. Works on incomplete data: a missing
config/rung yields NaN rows/placeholders with a WARNING, never a crash,
and every file from spec-analysis.md SS3 is still produced.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import calibration
import e4_witness
import figures
import metrics
import scaling
import rate_correction
import tables
import tables_extra
import values_tex
from analyze_v1 import run_v1_supplement
from loaders import (
    CONFIGS, E4_RUNGS, E2_RUNGS, discover_e1, discover_e4, discover_e4r, discover_e2,
)
from omb_import import (
    discover_omb, discover_omb_e2e, ladder_rows as omb_ladder_rows,
    omb_profile_row, omb_profiles_json_entry, plateau_mu,
)
from style import WARNINGS, warn


def _analyze(results_root: Path, out_tables: Path, out_figures: Path,
             profiles_path: Path, summary_path: Path, v1_root: "Path | None" = None) -> dict:
    results_root = Path(results_root)
    out_tables = Path(out_tables)
    out_figures = Path(out_figures)
    profiles_path = Path(profiles_path)
    summary_path = Path(summary_path)

    e1_runs_all = []
    for c in CONFIGS:
        e1_runs_all.extend(discover_e1(results_root, c))
    e1_hw_cfgs = sorted({r.config for r in e1_runs_all})
    for cfg in CONFIGS:
        if not any(r.config == cfg or r.config.startswith(cfg + "-") for r in e1_runs_all):
            warn(f"E1: no runs found for config={cfg}")
    e1_tau_rows = [metrics.e1_per_run(run) for run in e1_runs_all]
    e1_summary_rows = [metrics.e1_summary(chw, e1_tau_rows) for chw in e1_hw_cfgs]
    tables.write_tab_e1_tau(e1_tau_rows, out_tables)
    tables.write_tab_e1_summary(e1_summary_rows, out_tables)
    e1_summary_by_config = {r["config"]: r for r in e1_summary_rows}

    calib_by_config, calib_rows = calibration.resolve(results_root, e1_summary_by_config)
    tables_extra.write_tab_calibration(calib_rows, out_tables)

    e4r_by_key: dict = {}
    for cfg in CONFIGS:
        e4r_by_key.update(discover_e4r(results_root, cfg))
    e4r_hw_cfgs = sorted({chw for (chw, _r) in e4r_by_key})
    e4r_ladder_all = []
    e4r_mu_rows = []
    e4r_ladder_by_config: dict = {}
    for chw in e4r_hw_cfgs:
        ladder_by_rate = {}
        for (c2, rate), runs in sorted(e4r_by_key.items()):
            if c2 != chw:
                continue
            rows = [metrics.e4_per_run(run) for run in runs]
            for r, run in zip(rows, runs):
                r["rate"] = rate
                r["lambda_jmx"] = e4_witness.lambda_jmx(run)
            ladder_by_rate[rate] = rows
            e4r_ladder_all.extend(rows)
        e4r_ladder_by_config[chw] = [r for rows in ladder_by_rate.values() for r in rows]
        mu = metrics.e4_plateau(chw, ladder_by_rate)
        mu["rate_plateau"] = mu.pop("P_plateau", math.nan)
        e4r_mu_rows.append(mu)
    tables.write_tab_e4r_ladder(e4r_ladder_all, out_tables)
    tables.write_tab_e4r_mu(e4r_mu_rows, out_tables)
    e4r_mu_by_config = {r["config"]: r for r in e4r_mu_rows}

    e4_by_key: dict = {}
    for cfg in CONFIGS:
        e4_by_key.update(discover_e4(results_root, cfg))
    e4_hw_cfgs = sorted({chw for (chw, _p) in e4_by_key})
    for cfg in CONFIGS:
        if not any(chw == cfg or chw.startswith(cfg + "-") for chw in e4_hw_cfgs) \
                and not any(chw == cfg or chw.startswith(cfg + "-") for chw in e4r_hw_cfgs):
            warn(f"E4: no runs found for config={cfg}")
    e4_ladder_rows_by_config = {}
    e4_mu_rows = []
    for chw in e4_hw_cfgs:
        ladder_rows_by_p = {}
        all_ladder_rows = []
        for (c2, p_val), runs in sorted(e4_by_key.items()):
            if c2 != chw:
                continue
            rows = [metrics.e4_per_run(run) for run in runs]
            for r, run in zip(rows, runs):
                r["lambda_jmx"] = e4_witness.lambda_jmx(run)
            ladder_rows_by_p[p_val] = rows
            all_ladder_rows.extend(rows)
        e4_ladder_rows_by_config[chw] = all_ladder_rows
        if ladder_rows_by_p:
            e4_mu_rows.append(metrics.e4_plateau(chw, ladder_rows_by_p))

    all_e4_ladder_rows = [r for rows in e4_ladder_rows_by_config.values() for r in rows]
    tables.write_tab_e4_ladder(all_e4_ladder_rows, out_tables)
    tables.write_tab_e4_mu(e4_mu_rows, out_tables)
    e4_mu_by_config = {r["config"]: r for r in e4_mu_rows}

    attempt_cfgs = sorted({d.name for d in (results_root / "E4").iterdir() if d.is_dir()}
                          if (results_root / "E4").is_dir() else set())
    mu_summary_by_config = {}
    attempts_by_config = {}
    for chw in attempt_cfgs:
        att_rows = e4_witness.attempts_rows(results_root, chw)
        attempts_by_config[chw] = att_rows
        tables_extra.write_tab_e4_attempts(chw, att_rows, out_tables)
        mu_summary_by_config[chw] = scaling.mu_summary_row(chw, att_rows)
    tables_extra.write_tab_mu_summary(mu_summary_by_config, out_tables)
    tables_extra.write_tab_scalability_err(scaling.scalability_error_rows(mu_summary_by_config), out_tables)

    def _mu_with_r0_fallback(cfg: str, field: str) -> float:
        """mu from the P-ladder, else the R0-rate fallback (repeatable path,
        no manual PUTs). Single place — every consumer (profiles, scalability,
        E2) reads through here so no fourth copy rots."""
        v = e4_mu_by_config.get(cfg, {}).get(field, math.nan)
        if v != v:
            v = e4r_mu_by_config.get(cfg, {}).get(field, math.nan)
        return v

    mu_by_config = {}
    for c in CONFIGS:
        mu_by_config[c] = {"mu_msgs": _mu_with_r0_fallback(c, "mu_msgs"),
                           "mu_mbps": _mu_with_r0_fallback(c, "mu_mbps")}
    tau_ack_by_config = {c: calib_by_config.get(c, {}).get("tau_ack_med", math.nan) for c in CONFIGS}
    tau_e2e_by_config = {c: calib_by_config.get(c, {}).get("tau_e2e_med", math.nan) for c in CONFIGS}
    scalability_rows = metrics.scalability_rows(mu_by_config, tau_ack_by_config, tau_e2e_by_config)
    tables.write_tab_scalability(scalability_rows, out_tables)

    e2_by_key: dict = {}
    for cfg in CONFIGS:
        e2_by_key.update(discover_e2(results_root, cfg))
    e2_hw_cfgs = sorted({chw for (chw, _r, _v) in e2_by_key})
    e2_ladder_rows_by_config = {}
    e2_summary_rows_all = []
    e2bp_ladder_rows_all = []
    e2bp_summary_rows_all = []
    kingman_error_rows = []
    for chw in e2_hw_cfgs:
        mu_msgs = _mu_with_r0_fallback(chw, "mu_msgs")
        tau_ack = calib_by_config.get(chw, {}).get("tau_ack_med", math.nan)
        tau_e2e = calib_by_config.get(chw, {}).get("tau_e2e_med", math.nan)
        ca2 = calib_by_config.get(chw, {}).get("ca2_med", math.nan)
        cs2 = calib_by_config.get(chw, {}).get("cs2_med", math.nan)

        all_rungs_rows = {}
        e2_ladder_rows = []
        for rho in E2_RUNGS:
            runs = e2_by_key.get((chw, rho, ""), [])
            rows = [metrics.e2_per_run(run, mu_msgs, tau_ack, tau_e2e, ca2, cs2) for run in runs]
            all_rungs_rows[rho] = rows
            e2_ladder_rows.extend(rows)
            e2_summary_rows_all.append(metrics.e2_summary(chw, rho, rows))
        for (c2, rho, variant), runs in sorted(e2_by_key.items()):
            if c2 != chw or not variant:
                continue
            rows = [metrics.e2_per_run(run, mu_msgs, tau_ack, tau_e2e, ca2, cs2) for run in runs]
            for r in rows:
                r["variant"] = variant
                r["config"] = chw
            e2bp_ladder_rows_all.extend(rows)
            summary = metrics.e2_summary(chw, rho, rows)
            summary["variant"] = variant
            e2bp_summary_rows_all.append(summary)
        e2_ladder_rows_by_config[chw] = e2_ladder_rows
        tables.write_tab_e2_ladder(chw, e2_ladder_rows, out_tables)
        kingman_error_rows.append(metrics.kingman_error_row(chw, all_rungs_rows))
    for cfg in CONFIGS:
        if cfg not in e2_hw_cfgs and not any(c.startswith(cfg + "-") for c in e2_hw_cfgs):
            warn(f"E2: no runs found for config={cfg}")

    tables.write_tab_e2_summary(e2_summary_rows_all, out_tables)
    tables.write_tab_e2bp(e2bp_ladder_rows_all, e2bp_summary_rows_all, out_tables)
    tables.write_tab_kingman_error(kingman_error_rows, out_tables)
    rate_correction.produce(results_root, out_tables, out_figures, mu_by_config, e2_by_key)

    omb_profile_rows = []
    omb_base = results_root / "OMB-reference"
    omb_cfgs = sorted(d.name for d in omb_base.iterdir()) if omb_base.is_dir() else []
    for config_hw in omb_cfgs:
        by_rung = discover_omb(results_root, config_hw)
        if not by_rung:
            continue
        ladder = omb_ladder_rows(config_hw, by_rung)
        mu = plateau_mu(ladder)
        e2e = discover_omb_e2e(results_root, config_hw)
        omb_profile_rows.append(omb_profile_row(config_hw, by_rung, mu, e2e))
        base = config_hw.split("-es")[0].split("-emicro")[0]
        fn_rows = [r for r in e4r_ladder_all
                   if r.get("config") == base or str(r.get("config", "")).startswith(base + "-")]
        figures.fig_omb_vs_functions_ladder(config_hw, ladder, fn_rows, out_figures)
    tables.write_tab_omb_profile(omb_profile_rows, out_tables)

    from loaders import discover_logdirs_evidence, discover_prom_log_bytes
    from parsers import parse_logdirs_snapshot, prom_last_by_labels
    part_detail: list = []
    for ev in discover_logdirs_evidence(results_root):
        rows = parse_logdirs_snapshot(ev)
        if not rows:
            warn(f"partition-usage: unparseable evidence {ev}")
        part_detail.extend(rows)
    for prom_file in discover_prom_log_bytes(results_root):
        for entry in prom_last_by_labels(prom_file, ("broker", "topic", "partition")):
            lbl = entry["labels"]
            try:
                size = float(entry["value"])
            except (TypeError, ValueError):
                continue
            if size != size:
                continue
            part_detail.append({"topic": lbl.get("topic", "?"),
                                "partition": lbl.get("partition", "?"),
                                "broker": lbl.get("broker", "?"),
                                "size_bytes": size,
                                "source": str(prom_file)})
    _part_detail, _part_summary = metrics.partition_imbalance(part_detail)
    tables.write_tab_partition_usage(_part_detail, _part_summary, out_tables)

    census_rows = []
    time_budget_rows = []
    for chw in e1_hw_cfgs:
        chw_runs = [r for r in e1_runs_all if r.config == chw]
        census_rows.append(metrics.run_census_row("E1", chw, "", chw_runs))
        time_budget_rows.append(metrics.time_budget_row("E1", chw, chw_runs))

    for (chw, p_val), runs in sorted(e4_by_key.items()):
        census_rows.append(metrics.run_census_row("E4", chw, f"P{p_val}", runs))
    for (chw, rate), runs in sorted(e4r_by_key.items()):
        census_rows.append(metrics.run_census_row("E4", chw, f"R{rate}", runs))
    regular_labels = {f"R{rate}" for (_c, rate) in e4r_by_key} | {f"P{p}" for (_c, p) in e4_by_key}
    for chw in attempt_cfgs:
        att_runs = e4_witness.discover_attempts(results_root, chw)
        by_label: dict = {}
        for run in att_runs:
            if str(run.rung) in regular_labels:
                continue
            by_label.setdefault(str(run.rung), []).append(run)
        for label, runs in sorted(by_label.items()):
            census_rows.append(metrics.run_census_row("E4", chw, label, runs))
    for chw in sorted(set(e4_hw_cfgs) | set(e4r_hw_cfgs) | set(attempt_cfgs)):
        chw_runs = [r for (c2, _p), rs in e4_by_key.items() if c2 == chw for r in rs]
        chw_runs += [r for (c2, _r), rs in e4r_by_key.items() if c2 == chw for r in rs]
        if not chw_runs:
            chw_runs = e4_witness.discover_attempts(results_root, chw)
        time_budget_rows.append(metrics.time_budget_row("E4", chw, chw_runs))

    for (chw, rho, variant), runs in sorted(e2_by_key.items()):
        census_rows.append(metrics.run_census_row("E2", chw, f"rho{rho}{variant}", runs))
    for chw in e2_hw_cfgs:
        chw_runs = [r for (c2, _r, _v), rs in e2_by_key.items() if c2 == chw for r in rs]
        time_budget_rows.append(metrics.time_budget_row("E2", chw, chw_runs))

    tables.write_tab_run_census(census_rows, out_tables)
    tables.write_tab_time_budget(time_budget_rows, out_tables)

    e2_summary_by_config = {c: [r for r in e2_summary_rows_all if r["config"] == c] for c in CONFIGS}
    profiles_by_config = {}
    for cfg in CONFIGS:
        s = calib_by_config.get(cfg, {})
        mu_msgs = _mu_with_r0_fallback(cfg, "mu_msgs")
        profiles_by_config[cfg] = {
            "muMsgs": mu_msgs,
            "tauAckMs": s.get("tau_ack_med", math.nan),
            "tauE2eMs": s.get("tau_e2e_med", math.nan),
            "ca2": s.get("ca2_med", math.nan),
            "cs2": s.get("cs2_med", math.nan),
        }

    for cfg in CONFIGS:
        figures.fig_kingman_config(cfg, profiles_by_config[cfg], e2_summary_by_config[cfg], out_figures)
    figures.fig_kingman_all(profiles_by_config, out_figures)
    figures.fig_kingman_error({r["config"]: r for r in kingman_error_rows}, e2_summary_rows_all, out_figures)

    for cfg in CONFIGS:
        p_rows = e4_ladder_rows_by_config.get(cfg, [])
        r_rows = e4r_ladder_by_config.get(cfg, [])
        if p_rows or not r_rows:
            figures.fig_e4_saturation(cfg, p_rows,
                                      e4_mu_by_config.get(cfg, {}), out_figures)
        if r_rows or not p_rows:
            figures.fig_e4r_saturation(cfg, r_rows,
                                       e4r_mu_by_config.get(cfg, {}), out_figures)

    for chw in e4_hw_cfgs:
        mu_row = e4_mu_by_config.get(chw, {})
        p_plateau = mu_row.get("P_plateau")
        series_rows = []
        if p_plateau == p_plateau and p_plateau is not None:
            runs = e4_by_key.get((chw, int(p_plateau)), [])
            if runs:
                from loaders import series_in_window
                series_rows = series_in_window(runs[0])
            else:
                warn(f"fig-e4-timeseries-{chw}: no run directory for plateau P={p_plateau}")
        else:
            warn(f"fig-e4-timeseries-{chw}: no plateau P determined (mu_status=client-limited or no data)")
        figures.fig_e4_timeseries(chw, p_plateau, series_rows, out_figures)

    figures.fig_scalability(attempts_by_config, out_figures)
    figures.fig_e1_tau(e1_tau_rows, out_figures)

    def _nn(v):
        return None if v is None or v != v else v

    profiles_out = {}
    for cfg in CONFIGS:
        p = profiles_by_config[cfg]
        if any(v != v for v in p.values()):
            warn(f"profiles.json: config={cfg} has incomplete profile (NaN field present)")
        entry = {"source": "E1/E4 pipeline (analyze.py)"}
        for py_key, java_key in (("muMsgs", "muMsgs"), ("tauAckMs", "tauAckMs"),
                                 ("tauE2eMs", "tauE2eMs"), ("ca2", "ca2"), ("cs2", "cs2")):
            v = _nn(p[py_key])
            if v is not None:
                entry[java_key] = v
        profiles_out[cfg] = entry
    if omb_profile_rows:
        profiles_out["OMB"] = omb_profiles_json_entry(omb_profile_rows[0])
    else:
        warn("profiles.json: no OMB ladder data, OMB entry skipped")
    profiles_path.parent.mkdir(parents=True, exist_ok=True)
    profiles_path.write_text(json.dumps(profiles_out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    run_v1_supplement(v1_root, e2_summary_rows_all, out_tables, out_figures)
    from analyze_v1 import run_omb_evidence
    run_omb_evidence(results_root, out_tables)

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    _write_summary_md(summary_path, e1_summary_rows, e4_mu_rows, scalability_rows,
                      kingman_error_rows, e2_summary_rows_all)

    n_values = values_tex.write_values_tex(out_tables)

    return {
        "values": n_values,
        "e1_tau_rows": len(e1_tau_rows),
        "e4_ladder_rows": len(all_e4_ladder_rows),
        "e2_summary_rows": len(e2_summary_rows_all),
        "warnings": len(WARNINGS),
    }


def _fmt2(v):
    return "N/A" if v is None or v != v else f"{v:.2f}"


def _write_summary_md(summary_path: Path, e1_summary_rows, e4_mu_rows,
                      scalability_rows, kingman_error_rows, e2_summary_rows_all) -> None:
    lines = ["# Podsumowanie analizy (thesis-v2)", ""]
    lines.append("Zestawienie liczbowe generowane automatycznie przez `analyze.py`; "
                 "zrodlo danych do rozdzialu 5 manuskryptu (W6).")
    lines.append("")
    lines.append("## E1 -- kalibracja tau i c^2")
    for r in e1_summary_rows:
        lines.append(f"- **{r['config']}**: tau_ack = {_fmt2(r['tau_ack_med'])} ms "
                     f"(CoV {_fmt2(r['tau_ack_cov'])}), tau_e2e = {_fmt2(r['tau_e2e_med'])} ms, "
                     f"c_s^2 = {_fmt2(r['cs2_med'])}, c_a^2 = {_fmt2(r['ca2_med'])}.")
    lines.append("")
    lines.append("## E4 -- plateau mu i sygnatura saturacji")
    for r in e4_mu_rows:
        lines.append(f"- **{r['config']}**: P_plateau = {r.get('P_plateau')}, "
                     f"mu = {_fmt2(r['mu_msgs'])} msg/s ({_fmt2(r['mu_mbps'])} MB/s), "
                     f"binding_resource = {r.get('binding_resource')}, "
                     f"mu_status = {r.get('mu_status')}.")
    lines.append("")
    lines.append("## Skalowalnosc")
    for r in scalability_rows:
        lines.append(f"- {r['metric']}: 1b-rf1={_fmt2(r['1b-rf1'])}, "
                     f"3b-rf1={_fmt2(r['3b-rf1'])} (S={_fmt2(r['S_rf1'])}, "
                     f"eff={_fmt2(r['eff_rf1'])}), 3b-rf3={_fmt2(r['3b-rf3'])} "
                     f"(rf_cost={_fmt2(r['rf_cost'])}).")
    lines.append("")
    lines.append("## Blad Kingmana (MAE/MAPE)")
    for r in kingman_error_rows:
        lines.append(f"- **{r['config']}**: MAE_ack = {_fmt2(r['MAE_ack_ms'])} ms, "
                     f"MAPE_ack = {_fmt2(r['MAPE_ack'])}, MAE_e2e = {_fmt2(r['MAE_e2e_ms'])} ms, "
                     f"MAPE_e2e = {_fmt2(r['MAPE_e2e'])}, "
                     f"best_rho = {r.get('best_rho')}, worst_rho = {r.get('worst_rho')}.")
    lines.append("")
    lines.append("## Drabinka E2 (mediany)")
    for r in e2_summary_rows_all:
        lines.append(f"- {r['config']} rho={r['rho_target']}: rho_real = "
                     f"{_fmt2(r['rho_real_med'])}, ack_mean = {_fmt2(r['ack_mean_med'])} ms "
                     f"(pred {_fmt2(r['pred_ack'])} ms), e2e_mean = {_fmt2(r['e2e_mean_med'])} ms "
                     f"(pred {_fmt2(r['pred_e2e'])} ms).")
    lines.append("")
    if WARNINGS:
        lines.append("## Ostrzezenia pipeline'u")
        for w in WARNINGS:
            lines.append(f"- {w}")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, help="results/ root (experiment-plan.md SS5.1 layout)")
    parser.add_argument("--out-tables", required=True, help="output dir for CSV tables (paper/tables)")
    parser.add_argument("--out-figures", required=True, help="output dir for figures (paper/figures)")
    parser.add_argument("--profiles", required=True, help="output path for profiles.json")
    parser.add_argument("--v1-results", default=None,
                        help="results/ root of the first campaign (3x e2-standard-4); "
                             "enables the v1 supplement tables (default: skipped)")
    args = parser.parse_args(argv)

    summary_path = Path(args.profiles).resolve().parent / "summary.md"

    stats = _analyze(Path(args.results), Path(args.out_tables), Path(args.out_figures),
                     Path(args.profiles), summary_path,
                     Path(args.v1_results) if args.v1_results else None)

    print(f"OK: e1_tau_rows={stats['e1_tau_rows']} e4_ladder_rows={stats['e4_ladder_rows']} "
         f"e2_summary_rows={stats['e2_summary_rows']} values={stats['values']} "
         f"warnings={stats['warnings']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
