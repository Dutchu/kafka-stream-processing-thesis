"""Print the `summary` block of a run manifest.

Usage: python peek_summary.py <run_dir>
"""
import json
import sys
from pathlib import Path


def main():
    m = json.load(open(Path(sys.argv[1]) / "run.json", encoding="utf-8"))
    print(json.dumps(m.get("summary"), indent=1, ensure_ascii=False)[:3000])
    print("params:", json.dumps(m.get("params"), ensure_ascii=False)[:500])
    print("profile:", json.dumps(m.get("profile"), ensure_ascii=False)[:500])


if __name__ == "__main__":
    main()
