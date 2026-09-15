#!/usr/bin/env python3
"""One-off converter: legacy CSV run artifacts -> the JSON input contract.

    python tools/convert_csv_to_json.py <results_root> [--delete-csv]

For every run directory below <results_root> that still holds the legacy
`dashboard_1hz.csv` / `ack_reports.csv`, writes the equivalent
`series.json` / `ack.json` next to them:

* numbers stay numbers (int cells -> JSON int, decimal cells -> JSON float;
  the numeric *value* is identical -- `9200.0` and `9200` compare equal),
* empty CSV cells -> JSON null,
* `run_id` stays a string; `error_types` (series) stays the
  "Type:count;..." string ("" when empty),
* `errors` (ack) becomes a JSON object {Type: count} ({} when empty).

The pipeline itself (parsers.py) reads ONLY the JSON files; this script is
the bridge for result directories collected before the dashboard rewrite
(and is how tests/fixtures were migrated). Pure stdlib.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from parsers import ACK_COLUMNS, SERIES_COLUMNS, parse_error_types  # noqa: E402

_SERIES_STR = {"run_id", "error_types"}
_ACK_STR = {"run_id"}


def _cell_to_json(cell: str):
    """CSV cell -> JSON scalar: "" -> None, "12" -> 12, "12.5" -> 12.5."""
    s = (cell or "").strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        return s


def _convert_rows(csv_path: Path, columns: list[str], str_cols: set[str],
                  error_obj_col: "str | None") -> list[dict]:
    rows: list[dict] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        for raw in csv.DictReader(fh):
            row: dict = {}
            for col in columns:
                cell = raw.get(col, "")
                if col == error_obj_col:
                    row[col] = parse_error_types(cell)
                elif col in str_cols:
                    row[col] = (cell or "").strip()
                else:
                    row[col] = _cell_to_json(cell)
            rows.append(row)
    return rows


def _dump(path: Path, rows: list[dict]) -> None:
    path.write_text(json.dumps(rows, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")


def convert_run_dir(run_dir: Path, delete_csv: bool = False) -> list[Path]:
    """Convert one run directory in place; returns the JSON paths written."""
    written: list[Path] = []
    series_csv = run_dir / "dashboard_1hz.csv"
    ack_csv = run_dir / "ack_reports.csv"
    if series_csv.exists():
        out = run_dir / "series.json"
        _dump(out, _convert_rows(series_csv, SERIES_COLUMNS, _SERIES_STR, None))
        written.append(out)
        if delete_csv:
            series_csv.unlink()
    if ack_csv.exists():
        out = run_dir / "ack.json"
        _dump(out, _convert_rows(ack_csv, ACK_COLUMNS, _ACK_STR, "errors"))
        written.append(out)
        if delete_csv:
            ack_csv.unlink()
    return written


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results_root", help="results/ root (or any directory tree containing run dirs)")
    ap.add_argument("--delete-csv", action="store_true", help="remove the legacy CSV files after conversion")
    args = ap.parse_args(argv)

    root = Path(args.results_root)
    run_dirs = sorted({p.parent for p in root.rglob("dashboard_1hz.csv")} |
                      {p.parent for p in root.rglob("ack_reports.csv")})
    total = 0
    for d in run_dirs:
        for out in convert_run_dir(d, delete_csv=args.delete_csv):
            print(f"wrote {out}")
            total += 1
    print(f"OK: {total} JSON file(s) written in {len(run_dirs)} run dir(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
