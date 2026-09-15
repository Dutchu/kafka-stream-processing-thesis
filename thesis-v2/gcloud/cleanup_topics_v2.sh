#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster_hosts_v2.env
source "$SCRIPT_DIR/cluster_hosts_v2.env"

TOPICS="weather-rain weather-temp weather-wind control-metrics"
POLL_TIMEOUT_S=60

REMOTE_CMD=$(cat << EOF
set -uo pipefail
BOOTSTRAP="${BOOTSTRAP}"
KAFKA_BIN="${KAFKA_BIN}"
TOPICS="${TOPICS}"
TIMEOUT=${POLL_TIMEOUT_S}

list_existing() {
  "\$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "\$BOOTSTRAP" --list 2>/dev/null || true
}

EXISTING="\$(list_existing)"
ANY_FOUND=0
for topic in \$TOPICS; do
  if echo "\$EXISTING" | grep -qx "\$topic"; then
    ANY_FOUND=1
  fi
done

if [[ "\$ANY_FOUND" -eq 0 ]]; then
  echo "No weather-*/control-metrics topics found; nothing to clean up."
  exit 0
fi

echo "Deleting topics: \$TOPICS"
for topic in \$TOPICS; do
  if echo "\$EXISTING" | grep -qx "\$topic"; then
    "\$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "\$BOOTSTRAP" --delete --topic "\$topic" 2>&1 || true
  fi
done

echo "Polling for deletion to take effect (timeout \${TIMEOUT}s)..."
ELAPSED=0
while true; do
  REMAINING=""
  CUR="\$(list_existing)"
  for topic in \$TOPICS; do
    if echo "\$CUR" | grep -qx "\$topic"; then
      REMAINING="\$REMAINING \$topic"
    fi
  done
  if [[ -z "\$REMAINING" ]]; then
    echo "All matched topics confirmed gone after \${ELAPSED}s."
    exit 0
  fi
  if (( ELAPSED >= TIMEOUT )); then
    echo "TIMEOUT_SURVIVORS_BEGIN"
    echo "\$REMAINING"
    echo "TIMEOUT_SURVIVORS_END"
    exit 1
  fi
  sleep 1
  ELAPSED=\$(( ELAPSED + 1 ))
done
EOF
)

echo "Czyszczenie tematow weather-*/control-metrics na $BROKER1_INSTANCE (timeout: ${POLL_TIMEOUT_S}s) ..."

SSH_OUT=""
SSH_RC=0
SSH_OUT="$(gcloud compute ssh "${SSH_USER}@${BROKER1_INSTANCE}" --zone="$GCP_ZONE" --command="$REMOTE_CMD" 2>&1)" || SSH_RC=$?

printf '%s\n' "$SSH_OUT"

if [[ "$SSH_RC" -ne 0 ]]; then
  if printf '%s\n' "$SSH_OUT" | grep -q 'TIMEOUT_SURVIVORS_BEGIN'; then
    SURVIVORS="$(printf '%s\n' "$SSH_OUT" | sed -n '/TIMEOUT_SURVIVORS_BEGIN/,/TIMEOUT_SURVIVORS_END/p' | sed '1d;$d')"
    echo "##############################################################" >&2
    echo "OSTRZEZENIE: czyszczenie tematow przekroczylo timeout ${POLL_TIMEOUT_S}s." >&2
    echo "             Tematy, ktore NIE zniknely:" >&2
    echo "$SURVIVORS" >&2
    echo "             Sprawdz logi brokera recznie przed ponownym create_topics.sh." >&2
    echo "##############################################################" >&2
  else
    echo "##############################################################" >&2
    echo "OSTRZEZENIE: polaczenie SSH do $BROKER1_INSTANCE nie powiodlo sie." >&2
    echo "##############################################################" >&2
  fi
  exit 1
fi

exit 0
