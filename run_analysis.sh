#!/bin/bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
if [[ -z "${PY:-}" ]]; then
  if [[ -x ./.venv/bin/python3 ]]; then
    PY=./.venv/bin/python3
  elif [[ -x ./.venv/Scripts/python.exe ]]; then
    PY=./.venv/Scripts/python.exe
  else
    PY=python3
  fi
fi
if ! "$PY" -c 'import numpy, pandas, matplotlib'; then
  printf 'Brak interpretera lub zaleznosci. Wybierz PY i zainstaluj thesis-v2/analysis/requirements.txt w tym srodowisku.\n' >&2
  exit 1
fi
mkdir -p thesis-v2/analysis/output
printf 'Python: %s\nPelny log: thesis-v2/analysis/output/run-analysis.log\n' "$PY"
"$PY" -u thesis-v2/analysis/analyze.py \
  --results thesis-v2/results \
  --out-tables thesis-v2/paper/tables \
  --out-figures thesis-v2/paper/figures \
  --profiles thesis-v2/analysis/output/profiles.json \
  --v1-results results \
  2>&1 | tee thesis-v2/analysis/output/run-analysis.log
