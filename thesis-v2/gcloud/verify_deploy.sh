#!/bin/bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster_hosts_v2.env
source "$SCRIPT_DIR/cluster_hosts_v2.env"

EXPECT_BACK="${1:--}"
EXPECT_FRONT="${2:--}"
FAIL=0

VERSION="$(timeout 90 gcloud compute ssh "${SSH_USER}@${LOAD_INSTANCE}" --zone="$GCP_ZONE" \
  --command="curl -sf --max-time 15 localhost:8080/api/version" 2>/dev/null)" || {
  echo "ERROR: dashboard nie odpowiada na /api/version (tunel? serwis?)." >&2
  exit 1
}

get() { printf '%s' "$VERSION" | grep -oE "\"$1\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" | head -n1 | sed -E 's/.*:"([^"]*)"/\1/'; }

BACK="$(get backend)"; FRONT="$(get frontend)"; CONF="$(get config)"
echo "dashboard reports: backend=$BACK frontend=$FRONT config=$CONF"

if [[ "$EXPECT_BACK" != "-" ]]; then
  if [[ "$BACK" == "$EXPECT_BACK" ]]; then echo "OK: backend $BACK"; else echo "DRYF: backend jest $BACK, oczekiwano $EXPECT_BACK"; FAIL=1; fi
fi
if [[ "$EXPECT_FRONT" != "-" ]]; then
  if [[ "$FRONT" == "$EXPECT_FRONT" ]]; then echo "OK: frontend $FRONT"; else echo "DRYF: frontend jest $FRONT, oczekiwano $EXPECT_FRONT"; FAIL=1; fi
fi
if [[ "$BACK" == "unknown" || "$FRONT" == "unknown" ]]; then
  echo "UWAGA: 'unknown' = stempel nie istnieje (deploy sprzed stemplej epoki albo agent nie dobiegł)."
fi
exit $FAIL
