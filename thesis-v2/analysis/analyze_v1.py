"""v1 supplement step of the pipeline: tables + the v1-vs-v2 error figure.

Kept out of analyze.py so the entry point stays small.  Always emits the
frozen v1 tables (header-only when ``v1_root`` is None or empty) so the
manuscript's conditional includes see a file and can show a placeholder.
"""
from __future__ import annotations

import math
from pathlib import Path

import matplotlib.pyplot as plt

import tables_v1
import v1_import
from style import place_legend, save_fig, warn, write_csv


def run_v1_supplement(v1_root: "Path | None", v2_e2_summary_rows: list[dict],
                      out_tables: Path, out_figures: Path, v2_config: str = "3b-rf1") -> None:
    if v1_root is None or not Path(v1_root).is_dir():
        warn("v1 supplement: --v1-results not given or missing; v1 tables emitted empty")
        e1_rows, e4_rows, ladder = [], [], []
    else:
        e1_rows = v1_import.v1_e1_rows(Path(v1_root))
        e4_rows = v1_import.v1_e4_rows(Path(v1_root))
        ladder = v1_import.v1_e2_ladder_rows(Path(v1_root))
    summary = v1_import.v1_e2_summary_rows(ladder)
    cmp_rows = v1_import.e2_error_comparison_rows(summary, v2_e2_summary_rows, v2_config)
    tables_v1.write_tab_v1_e1(e1_rows, out_tables)
    tables_v1.write_tab_v1_e4(e4_rows, out_tables)
    tables_v1.write_tab_v1_e2(ladder, summary, out_tables)
    tables_v1.write_tab_e2_error_cmp(cmp_rows, out_tables)
    fig_e2_error_v1_vs_v2(cmp_rows, out_figures)


def run_omb_evidence(results_root: Path, out_tables: Path) -> None:
    """Client-bound evidence of the OMB reference ladder (thesis-v2 results)."""
    import omb_evidence
    tables_v1.write_tab_omb_evidence(omb_evidence.omb_evidence_rows(Path(results_root)), out_tables)


def fig_e2_error_v1_vs_v2(rows: list[dict], out_figures: Path) -> None:
    """Relative prediction error of the mean publish/ACK latency per rung:
    v1 (OMB, one VM, 3x e2-standard-4, tau=6/mu=98 MB/s) vs v2 (functions)."""
    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    sources = sorted({r["source"] for r in rows})
    if not rows or not sources:
        ax.text(0.5, 0.5, "brak danych", ha="center", va="center", transform=ax.transAxes)
        ax.set_axis_off()
        save_fig(fig, "fig-e2-error-v1-vs-v2", out_figures)
        write_csv("data-fig-e2-error-v1-vs-v2", ["source", "rho_target", "err_rel"], [], out_figures)
        return
    rungs = sorted({int(r["rho_target"]) for r in rows})
    width = 0.8 / max(1, len(sources))
    data_rows = []
    for i, src in enumerate(sources):
        ys = []
        for rung in rungs:
            match = [r for r in rows if r["source"] == src and int(r["rho_target"]) == rung]
            v = match[0]["err_rel"] if match else math.nan
            ys.append(v)
            data_rows.append([src, rung, "" if v != v else f"{v:.3f}"])
        xs = [k + (i - (len(sources) - 1) / 2) * width for k in range(len(rungs))]
        ax.bar(xs, [0 if y != y else y * 100 for y in ys], width=width * 0.95, label=src,
               hatch=["", "//", "..", "xx"][i % 4], edgecolor="black", linewidth=0.6)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(range(len(rungs)))
    ax.set_xticklabels([f"$\\rho_{{cel}}$={r/100:.2f}" for r in rungs])
    ax.set_ylabel("błąd względny predykcji [%]")
    ax.set_yscale("symlog", linthresh=50)
    ax.grid(axis="y", linewidth=0.4, alpha=0.6)
    place_legend(ax, ncol=1)
    save_fig(fig, "fig-e2-error-v1-vs-v2", out_figures)
    write_csv("data-fig-e2-error-v1-vs-v2", ["source", "rho_target", "err_rel"], data_rows, out_figures)
