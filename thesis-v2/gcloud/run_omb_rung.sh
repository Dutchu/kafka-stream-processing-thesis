#!/bin/bash
set -euo pipefail

CONFIG="${1:?config required}"; RUNG="${2:?rung required}"
RUN_N="${3:?run number required}"; THROTTLE="${4:--1}"; ACKS="${5:-all}"
HW="${HW:?set HW=esmall (or es4/emicro) explicitly}"
SSH_USER="${SSH_USER:-blaszczyk17}"
GCP_ZONE="${GCP_ZONE:-europe-central2-a}"
PROJECT_ID="${PROJECT_ID:-thesis-kafka-proj}"
LOAD_INSTANCE="${LOAD_INSTANCE:-kafka-load}"
TOPIC="${TOPIC:-perf-topic}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN_DIR="$REPO_ROOT/thesis-v2/results/OMB-reference/$CONFIG-$HW/$RUNG/run$RUN_N"
mkdir -p "$RUN_DIR"

echo "### preflight: $TOPIC shape"
if ! gcloud compute ssh "$SSH_USER@$LOAD_INSTANCE" --tunnel-through-iap --zone="$GCP_ZONE" \
  --project="$PROJECT_ID" --command="/opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.0.0.6:9092 --describe --topic $TOPIC" 2>&1 | tee "$RUN_DIR/preflight-topic.txt" | grep "PartitionCount: 30" > /dev/null; then
  echo "ERROR: $TOPIC missing or wrong shape (want 30 partitions, RF per round). Recreate it on kafka-1, then rerun." >&2
  exit 3
fi

DURATION_SEC="${DURATION_SEC:-120}"
if [[ -n "${NUM_RECORDS:-}" ]]; then
  RECORDS="$NUM_RECORDS"
elif [[ "$THROTTLE" == "-1" ]]; then
  RECORDS=$(( 100000 * DURATION_SEC ))
else
  RECORDS=$(( THROTTLE * DURATION_SEC ))
  if [[ "$RECORDS" -lt 3000000 ]]; then RECORDS=3000000; fi
fi

echo "### OMB rung $CONFIG/$RUNG/run$RUN_N: throughput=$THROTTLE acks=$ACKS -> $RUN_DIR"

RUN_START=$(date +%s)
run_consumer() {
  gcloud compute ssh "$SSH_USER@$LOAD_INSTANCE" --tunnel-through-iap --zone="$GCP_ZONE" \
    --project="$PROJECT_ID" --command="cd /opt/kafka/bin && ./kafka-consumer-perf-test.sh \
    --topic $TOPIC \
    --messages $RECORDS \
    --threads 1 \
    --bootstrap-server 10.0.0.6:9092" 2>&1
}
run_producer() {
  local cid="$1"
  local batch="${BATCH_SIZE:-16384}"
  local linger="${LINGER_MS:-0}"
  gcloud compute ssh "$SSH_USER@$LOAD_INSTANCE" --tunnel-through-iap --zone="$GCP_ZONE" \
    --project="$PROJECT_ID" --command="cd /opt/kafka/bin && ./kafka-producer-perf-test.sh \
    --topic $TOPIC \
    --num-records $RECORDS \
    --record-size 1024 \
    --throughput $THROTTLE \
    --producer-props bootstrap.servers=10.0.0.6:9092 acks=$ACKS client.id=$cid batch.size=$batch linger.ms=$linger" 2>&1
}
if [[ "${DUAL:-0}" == "1" ]]; then
  if [[ "${CONSUMER:-0}" == "1" ]]; then
    run_consumer > "$RUN_DIR/perf-consumer.txt" 2>&1 &
    PID_C=$!
  fi
  run_producer "omb-$CONFIG-$RUNG-a" > "$RUN_DIR/perf-a.txt" 2>&1 &
  PID_A=$!
  run_producer "omb-$CONFIG-$RUNG-b" > "$RUN_DIR/perf-b.txt" 2>&1 &
  PID_B=$!
  wait $PID_A; RC_A=$?
  wait $PID_B; RC_B=$?
  { echo "=== PRODUCER A (rc=$RC_A) ==="; tail -1 "$RUN_DIR/perf-a.txt"; echo "=== PRODUCER B (rc=$RC_B) ==="; tail -1 "$RUN_DIR/perf-b.txt"; } | tee "$RUN_DIR/perf.txt"
  if [[ "${CONSUMER:-0}" == "1" ]]; then
    wait $PID_C; RC_C=$?
    echo "=== CONSUMER (rc=$RC_C) ==="; tail -3 "$RUN_DIR/perf-consumer.txt"
  fi
else
  if [[ "${CONSUMER:-0}" == "1" ]]; then
    run_consumer > "$RUN_DIR/perf-consumer.txt" 2>&1 &
    PID_C=$!
  fi
  run_producer "omb-$CONFIG-$RUNG" 2>&1 | tee "$RUN_DIR/perf.txt"
  if [[ "${CONSUMER:-0}" == "1" ]]; then
    wait $PID_C; RC_C=$?
    echo "=== CONSUMER (rc=$RC_C) ==="; tail -3 "$RUN_DIR/perf-consumer.txt"
  fi
fi
RUN_END=$(date +%s)

{
  echo "export RUN_START=$RUN_START"
  echo "export RUN_END=$RUN_END"
} > "$RUN_DIR/run_window.env"

echo "### perf.txt saved ($(wc -l < "$RUN_DIR/perf.txt") lines). Collecting prom/ ..."
bash "$REPO_ROOT/gcloud/collect_jmx_artifacts.sh" "$RUN_DIR" \
  || echo "WARNING: prom/ collection failed — perf artifact is safe in $RUN_DIR"

echo "### done: $RUN_DIR"
