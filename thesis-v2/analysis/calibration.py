"""Calibration parameters actually USED by the Kingman model, per config.

The E1 triple is the default source of tau_ack / tau_e2e / c_a^2 / c_s^2.
``calibration_overrides.json`` may point a config at a different low-load run
(e.g. when E1 was measured with a producer configuration that differs from the
E2/E4 campaign).  This module resolves the used values, keeps the E1 values for
comparison, and emits the rows of ``tab-calibration.csv`` so the manuscript can
show both side by side with the documented reason.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import metrics
from loaders import load_run
from style import warn

OVERRIDES_FILE = Path(__file__).resolve().parent / "calibration_overrides.json"

PARAMS = (("tau_ack_ms", "tau_ack_med"), ("tau_e2e_ms", "tau_e2e_med"),
          ("cs2", "cs2_med"), ("ca2", "ca2_med"))


def load_overrides(path: Path = OVERRIDES_FILE) -> dict:
    """{config: {"run": "E4/1b-rf1/R500/run1", "reason": "..."}} (comment keys dropped)."""
    if not path.is_file():
        if path == OVERRIDES_FILE:
            try:
                from calibration_defaults import OVERRIDES
            except ImportError:
                return {}
            return {k: v for k, v in OVERRIDES.items()
                    if not k.startswith("_") and isinstance(v, dict)}
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as e:  # noqa: BLE001
        warn(f"calibration: unparsable {path}: {e}")
        return {}
    return {k: v for k, v in data.items() if not k.startswith("_") and isinstance(v, dict)}


def _override_row(results_root: Path, config: str, spec: dict) -> dict | None:
    run_dir = results_root / spec.get("run", "")
    if not run_dir.is_dir():
        warn(f"calibration: override run for {config} missing: {run_dir}")
        return None
    run = load_run(run_dir, "CAL", config, rung="override", run_index=1)
    row = metrics.e1_per_run(run)
    row["source_run"] = spec.get("run", "")
    row["reason"] = spec.get("reason", "")
    return row


def resolve(results_root: Path, e1_summary_by_config: dict,
            overrides: dict | None = None) -> tuple[dict, list[dict]]:
    """Return (used_by_config, calibration_table_rows).

    used_by_config[config] has the same keys as an e1_summary row
    (tau_ack_med, tau_e2e_med, cs2_med, ca2_med) plus ``source`` ("E1" or the
    override run path).  The table rows list every parameter of every config:
    config, parameter, e1_value, used_value, source, reason.
    """
    overrides = load_overrides() if overrides is None else overrides
    used: dict = {}
    rows: list[dict] = []
    configs = sorted(set(e1_summary_by_config) | set(overrides))
    for cfg in configs:
        e1 = e1_summary_by_config.get(cfg, {})
        ov_row = _override_row(Path(results_root), cfg, overrides[cfg]) if cfg in overrides else None
        entry = {"config": cfg, "source": "E1"}
        for run_key, sum_key in PARAMS:
            e1_val = e1.get(sum_key, math.nan)
            if ov_row is not None and ov_row.get(run_key, math.nan) == ov_row.get(run_key, math.nan):
                used_val = ov_row[run_key]
                source = ov_row["source_run"]
                reason = ov_row["reason"]
            else:
                used_val = e1_val
                source = "E1"
                reason = ""
            entry[sum_key] = used_val
            rows.append({"config": cfg, "parameter": run_key, "e1_value": e1_val,
                         "used_value": used_val, "source": source, "reason": reason})
        if ov_row is not None:
            entry["source"] = ov_row["source_run"]
        used[cfg] = entry
    return used, rows
