#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SRC_BRANCH="${1:-main}"
WORKTREE="/tmp/thesis-appfront"

cd "$REPO_ROOT"

echo "SRC_BRANCH=$SRC_BRANCH  WORKTREE=$WORKTREE"
git rev-parse --verify "$SRC_BRANCH" >/dev/null 2>&1 \
  || { echo "ERROR: brak lokalnego brancha '$SRC_BRANCH'." >&2; exit 1; }

if [[ ! -d "$WORKTREE" ]]; then
  echo "Tworze worktree dla app-front ..."
  git worktree add "$WORKTREE" app-front || {
    echo "ERROR: nie udalo sie zalozyc worktree. Jesli galaz app-front nie istnieje," >&2
    echo "       zaloz ja raz recznie: git checkout --orphan app-front && git rm -rf . &&" >&2
    echo "       git checkout $SRC_BRANCH -- thesis-v2/kafka-app/dashboard-front .github &&" >&2
    echo "       git commit -m 'app-front: init' && git push -u origin app-front && git checkout $SRC_BRANCH" >&2
    exit 1
  }
fi

cd "$WORKTREE"
git checkout "$SRC_BRANCH" -- thesis-v2/kafka-app/dashboard-front .github

if git diff --cached --quiet; then
  echo "app-front juz aktualny — nic do pushowania."
  exit 0
fi

git -c user.name="${GIT_AUTHOR_NAME:-thesis}" -c user.email="${GIT_AUTHOR_EMAIL:-thesis@localhost}" \
  commit -m "app-front: sync from ${SRC_BRANCH} $(date -u +%Y%m%dT%H%M%SZ)"

if git push origin app-front; then
  echo "OK: app-front zsynchronizowany i spushowany (push na app-front odpala hot-reload CI)."
else
  echo "ERROR: git push nieudany (patrz wyzej)." >&2
  exit 1
fi
