"""Growth rate of a summed Prometheus gauge (e.g. leaders' LEO-LSO) inside a run window.

Usage: python peek_prom_rate.py <run_dir> <prom_file> [label_filter=value]
Prints mean of per-step delta/dt of the summed series plus start/end totals.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from parsers import parse_prom_json  # noqa: E402


def main():
    run_dir = Path(sys.argv[1])
    fname = sys.argv[2]
    flt = sys.argv[3].split("=") if len(sys.argv) > 3 else None
    m = json.load(open(run_dir / "run.json", encoding="utf-8"))
    start = m["startEpochMs"] / 1000 + 15
    stop = m["stopEpochMs"] / 1000
    series = parse_prom_json(run_dir / "prom" / fname)
    per_ts = {}
    labels_seen = set()
    for labels, pts in series:
        if flt and labels.get(flt[0]) != flt[1]:
            continue
        labels_seen.add(tuple(sorted(labels.items())))
        for ts, v in pts:
            if start <= ts <= stop:
                per_ts[ts] = per_ts.get(ts, 0.0) + v
    ts_sorted = sorted(per_ts)
    if len(ts_sorted) < 2:
        print("not enough points", len(ts_sorted), "series", len(labels_seen))
        return
    t0, t1 = ts_sorted[0], ts_sorted[-1]
    total_rate = (per_ts[t1] - per_ts[t0]) / (t1 - t0)
    print(f"series={len(labels_seen)} points={len(ts_sorted)} window={t1 - t0:.0f}s")
    print(f"sum start={per_ts[t0]:.0f} end={per_ts[t1]:.0f} rate={total_rate:.0f}/s")
    for lab in sorted(labels_seen)[:12]:
        print("  ", dict(lab))


if __name__ == "__main__":
    main()
