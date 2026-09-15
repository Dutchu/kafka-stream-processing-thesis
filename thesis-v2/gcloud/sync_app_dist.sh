#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SRC_BRANCH="${1:-main}"
WORKTREE="/tmp/thesis-appdist"

cd "$REPO_ROOT"

echo "SRC_BRANCH=$SRC_BRANCH  WORKTREE=$WORKTREE"
git rev-parse --verify "$SRC_BRANCH" >/dev/null 2>&1 \
  || { echo "ERROR: brak lokalnego brancha '$SRC_BRANCH'." >&2; exit 1; }

if [[ ! -d "$WORKTREE" ]]; then
  echo "Tworze worktree dla app-dist ..."
  git worktree add "$WORKTREE" app-dist
fi

cd "$WORKTREE"
git checkout "$SRC_BRANCH" -- thesis-v2/kafka-app thesis-v2/ansible thesis-v2/config thesis-v2/analysis/output/profiles.json thesis-v2/gcloud/function_url.env .github

if git diff --cached --quiet; then
  echo "app-dist juz aktualny — nic do pushowania."
  exit 0
fi

git -c user.name="${GIT_AUTHOR_NAME:-thesis}" -c user.email="${GIT_AUTHOR_EMAIL:-thesis@localhost}" \
  commit -m "app-dist: sync from ${SRC_BRANCH} $(date -u +%Y%m%dT%H%M%SZ)"

if git push origin app-dist; then
  echo "OK: app-dist zsynchronizowany i spushowany (push na app-dist odpala CI)."
else
  echo "ERROR: git push nieudany (patrz wyzej)." >&2
  exit 1
fi
