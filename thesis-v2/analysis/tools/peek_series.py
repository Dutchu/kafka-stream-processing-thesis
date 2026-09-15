"""Peek at a run's 1 Hz series: tick count, lambda_leo distribution, admin errors.

Usage: python peek_series.py <run_dir>
"""
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from parsers import parse_dashboard_series_json  # noqa: E402


def main():
    run_dir = Path(sys.argv[1])
    rows = parse_dashboard_series_json(run_dir / "series.json")
    lam = [r.get("lambda_leo") for r in rows]
    lam = [v for v in lam if v == v]
    print("ticks", len(rows), "lambda_leo n", len(lam))
    if lam:
        qs = statistics.quantiles(lam, n=10)
        print("lambda_leo min/med/max", round(min(lam)), round(statistics.median(lam)), round(max(lam)))
        print("deciles", [round(q) for q in qs])
    keys = rows[0].keys() if rows else []
    print("columns", sorted(keys))
    for col in ("lambda_consumer", "consumer_lag", "total_msgs_cluster", "sent", "acked"):
        vals = [r.get(col) for r in rows if r.get(col) == r.get(col)]
        if vals:
            print(col, "first/last", vals[0], vals[-1])


if __name__ == "__main__":
    main()
