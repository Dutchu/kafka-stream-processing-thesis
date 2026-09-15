"""Quick inspection of run manifests: timestamps, params, key summary fields.

Usage: python inspect_runs.py <results_dir> [glob ...]
"""
import datetime
import glob
import json
import sys


def main():
    root = sys.argv[1]
    pats = sys.argv[2:] or ["E1/*/run*", "E4/*/*/run*", "E2/*/*/run*", "FREE/*/*"]
    for pat in pats:
        for d in sorted(glob.glob(f"{root}/{pat}")):
            try:
                m = json.load(open(f"{d}/run.json", encoding="utf-8"))
            except Exception as e:  # noqa: BLE001
                print(d, "no run.json", e)
                continue
            s = m.get("startEpochMs")
            ts = datetime.datetime.utcfromtimestamp(s / 1000).strftime("%m-%d %H:%M") if s else "?"
            p = m.get("params") or {}
            summ = m.get("summary") or {}
            print(
                f"{d.replace(root + '/', ''):45s} {ts} P={p.get('parallelism', m.get('parallelism'))} "
                f"rate={p.get('ratePerSec', m.get('ratePerSec'))} dur={p.get('durationSec', m.get('durationSec'))} "
                f"lam={summ.get('lambdaMean')} ackP50={summ.get('ackP50')} e2eP50={summ.get('e2eP50')} "
                f"bytes={summ.get('avgRecordBytes')} failed={summ.get('failed')} lagMax={summ.get('consumerLagMax')}"
            )


if __name__ == "__main__":
    main()
