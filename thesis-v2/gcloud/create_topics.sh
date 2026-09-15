#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster_hosts_v2.env
source "$SCRIPT_DIR/cluster_hosts_v2.env"

usage() {
  echo "Usage: $0 <1b-rf1|3b-rf1|3b-rf3>" >&2
  echo "  Tworzy weather-rain/temp/wind (6 partycji) i control-metrics (1 partycja)" >&2
  echo "  z RF/min.insync.replicas zgodnymi z CONFIG. Idempotentne." >&2
  exit 1
}

[[ $# -eq 1 ]] || usage
CONFIG="$1"

case "$CONFIG" in
  1b-rf1)
    RF=1
    MIN_ISR=1
    BOOT="$BOOTSTRAP_1B"
    ;;
  3b-rf1)
    RF=1
    MIN_ISR=1
    BOOT="$BOOTSTRAP"
    ;;
  3b-rf3)
    RF=3
    MIN_ISR=2
    BOOT="$BOOTSTRAP"
    ;;
  *)
    echo "ERROR: CONFIG nieznany: '$CONFIG' (oczekiwano 1b-rf1|3b-rf1|3b-rf3)" >&2
    usage
    ;;
esac

RETENTION_MS=86400000

echo "CONFIG=$CONFIG  RF=$RF  min.insync.replicas=$MIN_ISR  bootstrap=$BOOT"

TOPIC_SPECS="weather-rain:6 weather-temp:6 weather-wind:6 control-metrics:1"

REMOTE_CMD=$(cat << EOF
set -uo pipefail
BOOT="${BOOT}"
KAFKA_BIN="${KAFKA_BIN}"
RF=${RF}
MIN_ISR=${MIN_ISR}
RETENTION_MS=${RETENTION_MS}
SPECS="${TOPIC_SPECS}"

FAIL=0
for spec in \$SPECS; do
  topic="\${spec%%:*}"
  parts="\${spec##*:}"

  DESC="\$("\$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "\$BOOT" --describe --topic "\$topic" 2>&1)"
  if echo "\$DESC" | grep -q "^Topic: \$topic"; then
    EXIST_RF="\$(echo "\$DESC" | head -n 1 | grep -oE 'ReplicationFactor:[ ]*[0-9]+' | grep -oE '[0-9]+' | head -n 1)"
    if [[ "\$EXIST_RF" == "\$RF" ]]; then
      echo "OK: \$topic already exists with matching RF=\$EXIST_RF"
    else
      echo "ERROR: \$topic exists with RF=\$EXIST_RF but CONFIG expects RF=\$RF." >&2
      echo "       Uruchom cleanup_topics_v2.sh, potem create_topics.sh ponownie." >&2
      FAIL=1
    fi
    continue
  fi

  if "\$KAFKA_BIN/kafka-topics.sh" --create \
      --bootstrap-server "\$BOOT" \
      --topic "\$topic" \
      --partitions "\$parts" \
      --replication-factor "\$RF" \
      --config "min.insync.replicas=\$MIN_ISR" \
      --config "retention.ms=\$RETENTION_MS" 2>&1; then
    echo "OK: \$topic created (partitions=\$parts RF=\$RF)"
  else
    echo "ERROR: failed to create topic \$topic" >&2
    FAIL=1
  fi
done

exit \$FAIL
EOF
)

echo "Tworze/weryfikuje tematy na $BROKER1_INSTANCE (SSH) ..."
if ! gcloud compute ssh "${SSH_USER}@${BROKER1_INSTANCE}" --zone="$GCP_ZONE" --command="$REMOTE_CMD"; then
  echo "ERROR: create_topics.sh nie powiodl sie (patrz komunikaty wyzej)." >&2
  exit 1
fi

echo "Zakonczono create_topics.sh dla CONFIG=$CONFIG."
