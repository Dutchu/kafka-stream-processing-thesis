"""Thesis-v2 print style (rcParams), figure/CSV saving, and the WARNINGS log.

Adapted from `C:\\thesis\\analysis\\style.py` (read-only pattern): same
rcParams and save_fig/write_csv semantics, but output directories are
resolved at call time from CLI arguments (--out-tables/--out-figures)
instead of fixed ROOT-relative paths, because thesis-v2/analyze.py must
support fixtures and any results/output layout the CLI is pointed at.
"""
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

WARNINGS: list[str] = []


def warn(msg: str) -> None:
    WARNINGS.append(msg)
    print(f"WARNING: {msg}")


plt.rcParams.update({
    "font.size": 9,
    "axes.titlesize": 9,
    "axes.labelsize": 9,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "figure.figsize": (5.6, 3.4),
    "savefig.bbox": "tight",
    "axes.grid": True,
    "grid.linestyle": ":",
    "grid.color": "0.8",
})

CONFIG_ORDER = ["1b-rf1", "3b-rf1", "3b-rf3"]
CONFIG_STYLE = {
    "1b-rf1": {"marker": "o", "linestyle": "-", "color": "0.05"},
    "3b-rf1": {"marker": "s", "linestyle": "--", "color": "0.35"},
    "3b-rf3": {"marker": "^", "linestyle": "-.", "color": "0.6"},
}


def config_style(config: str) -> dict:
    return CONFIG_STYLE.get(config, {"marker": "x", "linestyle": ":", "color": "0.5"})


def save_fig(fig, name: str, figdir: Path) -> None:
    figdir = Path(figdir)
    figdir.mkdir(parents=True, exist_ok=True)
    fig.savefig(figdir / f"{name}.pdf")
    fig.savefig(figdir / f"{name}.png", dpi=200)
    plt.close(fig)


def write_csv(name: str, header: list, rows: list, outdir: Path) -> None:
    """Single CSV emitter used for BOTH LaTeX tables (paper/tables) and
    figure-data twins (paper/figures/data-fig-*.csv). Headers without
    spaces, ',' separator, '.' decimal point (writer just formats floats
    the normal Python way, which already uses '.').
    """
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    with open(outdir / f"{name}.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def place_legend(ax, ncol: int = 2, max_entries=None):
    """House legend: below the axes, never on data. Copied pattern from
    the original style.py (deduplicates by label, optional cap)."""
    handles, labels = ax.get_legend_handles_labels()
    seen: set = set()
    hh = [(h, lab) for h, lab in zip(handles, labels)
          if lab not in seen and not seen.add(lab)]
    if max_entries is not None:
        hh = hh[:max_entries]
    if not hh:
        return None
    h2, l2 = zip(*hh)
    return ax.legend(h2, l2, loc="upper center",
                      bbox_to_anchor=(0.5, -0.20), ncol=ncol, fontsize=9,
                      frameon=False)
