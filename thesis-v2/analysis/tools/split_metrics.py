"""One-off refactoring helper: split metrics.py (720 lines) into four modules
(<600 lines each, author's rule) plus a thin facade that re-exports everything
so existing imports (`import metrics; metrics.e1_per_run`) keep working.

Usage: python split_metrics.py  (run from analysis/)
"""
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "metrics.py"
OUT = SRC.parent

HEADER_IMPORTS = '''from __future__ import annotations

import math
import statistics

import kingman
from loaders import (
    CONFIGS, ack_in_window, series_in_window, dispersion,
)
from parsers import parse_error_types, format_error_types, prom_min_of_mean_by_label, \\
    prom_max_of_mean_by_label, prom_mean_all, prom_fraction_above
from style import warn
'''

HELPERS = '''

def _col_mean(rows: list[dict], col: str) -> float:
    vals = [r[col] for r in rows if col in r and r[col] == r[col]]
    return sum(vals) / len(vals) if vals else math.nan


def _col_median(rows: list[dict], col: str) -> float:
    vals = [r[col] for r in rows if col in r and r[col] == r[col]]
    return dispersion(vals)["median"]


def _weighted_mean(rows: list[dict], val_col: str, weight_col: str) -> float:
    pairs = [(r[val_col], r[weight_col]) for r in rows
             if r.get(val_col) == r.get(val_col) and r.get(weight_col) == r.get(weight_col)
             and r.get(weight_col, 0) > 0]
    total_w = sum(w for _v, w in pairs)
    if total_w <= 0:
        return math.nan
    return sum(v * w for v, w in pairs) / total_w
'''

PARTS = [
    ("metrics_e1", "E1 calibration metrics: tau_ack / tau_e2e / c_s^2 / c_a^2 per run and per config.", 41, 153),
    ("metrics_e4", "E4 saturation metrics: broker signature, per-run ladder rows, plateau mu.", 156, 383),
    ("metrics_e2", "E2 rho-ladder metrics: per-run Kingman prediction/error, rung summary, MAE/MAPE.", 385, 563),
    ("metrics_misc", "Scalability rows, run census, time budget, partition imbalance.", 565, 720),
]


def main():
    lines = SRC.read_text(encoding="utf-8").splitlines()
    names = []
    for mod, doc, a, b in PARTS:
        body = "\n".join(lines[a - 1:b]).rstrip() + "\n"
        text = f'"""{doc}\n\nSplit out of the former metrics.py (facade kept for imports).\n"""\n' \
               + HEADER_IMPORTS + HELPERS + "\n\n" + body
        (OUT / f"{mod}.py").write_text(text, encoding="utf-8")
        names.append(mod)
    facade = ('"""Facade over the metrics_* modules (kept so `import metrics` keeps working).\n\n'
              'Real code lives in metrics_e1.py, metrics_e4.py, metrics_e2.py, metrics_misc.py\n'
              '(author\'s rule: no module above 600 lines).\n"""\n'
              'from __future__ import annotations\n\n')
    for mod in names:
        facade += f"from {mod} import *  # noqa: F401,F403\n"
    facade += ("from metrics_e1 import _col_mean, _col_median, _weighted_mean  # noqa: F401\n"
               "from metrics_e4 import NIC_LIMIT_BPS, DISK_LIMIT_BPS, REQUEST_QUEUE_DURATION_THRESHOLD, "
               "_binding_resource  # noqa: F401\n")
    SRC.write_text(facade, encoding="utf-8")
    for mod in names:
        n = len((OUT / f"{mod}.py").read_text(encoding="utf-8").splitlines())
        print(mod, n, "lines")


if __name__ == "__main__":
    main()
