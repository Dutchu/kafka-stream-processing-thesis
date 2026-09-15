#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster_hosts_v2.env
source "$SCRIPT_DIR/cluster_hosts_v2.env"

usage() {
  echo "Usage: $0 <run_dir> <run_id>" >&2
  echo "  <run_dir>  results/<EXP>/<CONFIG>/[.../]run<N>/ (tworzony jesli brak)" >&2
  echo "  <run_id>   identyfikator biegu (pole runId z odpowiedzi POST /api/run)" >&2
  exit 1
}

[[ $# -eq 2 ]] || usage
RUN_DIR_ARG="$1"
RUN_ID="$2"

mkdir -p "$RUN_DIR_ARG"
RUN_DIR="$(cd "$RUN_DIR_ARG" && pwd)"

COLLECT_VIA_SSH="${COLLECT_VIA_SSH:-0}"

echo "run_dir=$RUN_DIR  run_id=$RUN_ID  DASH_URL=$DASH_URL  PROM_URL=$PROM_URL  COLLECT_VIA_SSH=$COLLECT_VIA_SSH"

fetch_dash() {
  local api_path="$1"
  local out_file="$2"
  local url="${DASH_URL}${api_path}"

  if [[ "$COLLECT_VIA_SSH" -eq 1 ]]; then
    if gcloud compute ssh "${SSH_USER}@${LOAD_INSTANCE}" --zone="$GCP_ZONE" \
        --command="curl -sf --max-time 30 '${url}'" > "$out_file" 2>/dev/null; then
      [[ -s "$out_file" ]] && return 0
    fi
    return 1
  else
    curl -sf --max-time 30 "$url" -o "$out_file" 2>/dev/null && [[ -s "$out_file" ]]
  fi
}

DASH_WARN=0

echo "Pobieram manifest -> run.json ..."
if fetch_dash "/api/runs/${RUN_ID}/manifest" "$RUN_DIR/run.json"; then
  echo "  OK: run.json ($(wc -c < "$RUN_DIR/run.json") bytes)"
else
  echo "WARNING: nie udalo sie pobrac manifestu (/api/runs/${RUN_ID}/manifest)." >&2
  DASH_WARN=$((DASH_WARN + 1))
fi

echo "Pobieram serie 1Hz -> series.json ..."
if fetch_dash "/api/runs/${RUN_ID}/series" "$RUN_DIR/series.json"; then
  echo "  OK: series.json ($(wc -c < "$RUN_DIR/series.json") bytes)"
else
  echo "WARNING: nie udalo sie pobrac serii (/api/runs/${RUN_ID}/series)." >&2
  DASH_WARN=$((DASH_WARN + 1))
fi

echo "Pobieram raporty ACK -> ack.json ..."
if fetch_dash "/api/runs/${RUN_ID}/ack" "$RUN_DIR/ack.json"; then
  echo "  OK: ack.json ($(wc -c < "$RUN_DIR/ack.json") bytes)"
else
  echo "WARNING: nie udalo sie pobrac raportow ACK (/api/runs/${RUN_ID}/ack)." >&2
  DASH_WARN=$((DASH_WARN + 1))
fi

START_EPOCH_S=""
STOP_EPOCH_S=""
if [[ -s "$RUN_DIR/run.json" ]]; then
  START_MS="$(grep -oE '"startEpochMs"[[:space:]]*:[[:space:]]*[0-9]+' "$RUN_DIR/run.json" | head -n1 | grep -oE '[0-9]+$' || true)"
  STOP_MS="$(grep -oE '"stopEpochMs"[[:space:]]*:[[:space:]]*[0-9]+' "$RUN_DIR/run.json" | head -n1 | grep -oE '[0-9]+$' || true)"
  if [[ -n "$START_MS" ]]; then START_EPOCH_S=$(( START_MS / 1000 )); fi
  if [[ -n "$STOP_MS" ]]; then STOP_EPOCH_S=$(( STOP_MS / 1000 )); fi
fi

if [[ -z "$START_EPOCH_S" || -z "$STOP_EPOCH_S" ]]; then
  echo "ERROR: brak startEpochMs/stopEpochMs w $RUN_DIR/run.json (manifest niedostepny/niekompletny)." >&2
  echo "       Zapytania Prometheusa (czesc B) wymagaja tego okna; przerywam." >&2
  exit 1
fi

QSTART=$(( START_EPOCH_S - 30 ))
QEND=$(( STOP_EPOCH_S + 60 ))
STEP="5s"

{
  echo "export QSTART=${QSTART}"
  echo "export QEND=${QEND}"
  echo "export STEP=${STEP}"
  echo "export START_EPOCH_S=${START_EPOCH_S}"
  echo "export STOP_EPOCH_S=${STOP_EPOCH_S}"
} > "$RUN_DIR/window.env"
echo "Okno zapytan: QSTART=$QSTART QEND=$QEND STEP=$STEP -> $RUN_DIR/window.env"

FUNCTION_URL_VAL=""
if [[ -f "$SCRIPT_DIR/function_url.env" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/function_url.env"
  FUNCTION_URL_VAL="${FUNCTION_URL:-}"
fi

CLUSTER_ID_VAL=""
QUORUM_SIZE_VAL=""
CFG_OUT=""
if CFG_OUT="$(gcloud compute ssh "${SSH_USER}@${BROKER1_INSTANCE}" --zone="$GCP_ZONE" \
    --command="cat /etc/kafka-thesis-v2.env 2>/dev/null || true" 2>/dev/null)"; then
  CLUSTER_ID_VAL="$(printf '%s\n' "$CFG_OUT" | grep -oE '^CLUSTER_ID=.*' | cut -d= -f2- || true)"
  QUORUM_SIZE_VAL="$(printf '%s\n' "$CFG_OUT" | grep -oE '^QUORUM_SIZE=.*' | cut -d= -f2- || true)"
fi
if [[ -z "$CLUSTER_ID_VAL" ]]; then
  echo "WARNING: nie udalo sie odczytac /etc/kafka-thesis-v2.env z ${BROKER1_INSTANCE} (CLUSTER_ID/QUORUM_SIZE puste w config.env)." >&2
fi

CONFIG_VAL="${CONFIG:-unknown}"
{
  echo "CONFIG=${CONFIG_VAL}"
  echo "CLUSTER_ID=${CLUSTER_ID_VAL}"
  echo "QUORUM_SIZE=${QUORUM_SIZE_VAL}"
  echo "FUNCTION_URL=${FUNCTION_URL_VAL}"
  echo "COLLECTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "RUN_ID=${RUN_ID}"
} > "$RUN_DIR/config.env"
echo "config.env zapisany -> $RUN_DIR/config.env"

PROM_DIR="$RUN_DIR/prom"
mkdir -p "$PROM_DIR"

PROMQL_msgs_in_broker='sum by (broker) (rate(kafka_server_brokertopicmetrics_messagesin_total[30s]))'
PROMQL_msgs_in_topic='sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_topic_total[30s]))'
PROMQL_bytes_in_broker='sum by (broker) (rate(kafka_server_brokertopicmetrics_bytesin_total[30s]))'
PROMQL_log_size='sum by (broker, topic) (kafka_log_logendoffset - kafka_log_logstartoffset)'
PROMQL_log_bytes='sum by (broker, topic, partition) (kafka_log_Size)'
PROMQL_leaders='sum by (broker, topic) ((kafka_log_logendoffset - kafka_log_logstartoffset) and on (broker, topic, partition) (kafka_cluster_partition_replicascount > 0))'
PROMQL_handler_idle='kafka_server_requesthandleravgidlepercent'
PROMQL_netproc_idle='kafka_network_networkprocessoravgidlepercent'
PROMQL_request_queue='kafka_network_requestchannel_requestqueuesize'
PROMQL_cpu_broker='1 - avg by (node)(rate(node_cpu_seconds_total{mode="idle",node=~"kafka-.*"}[1m]))'
PROMQL_nic_rx='sum by (node)(rate(node_network_receive_bytes_total{node=~"kafka-.*",device!~"lo"}[30s]))'
PROMQL_nic_tx='sum by (node)(rate(node_network_transmit_bytes_total{node=~"kafka-.*",device!~"lo"}[30s]))'
PROMQL_disk_write='sum by (node)(rate(node_disk_written_bytes_total{node=~"kafka-.*"}[30s]))'
PROMQL_consumer_host='1 - avg(rate(node_cpu_seconds_total{mode="idle",node="kafka-load"}[1m]))'
PROMQL_kingman='{__name__=~"thesis_kingman_.*"}'
PROMQL_produce_latency='kafka_network_requestmetrics_totaltimems{request="Produce"}'

REMOTE_CMD=$(cat << EOF
set -uo pipefail
rm -rf /tmp/promcollect_v2.* 2>/dev/null || true
RDIR=\$(mktemp -d /tmp/promcollect_v2.XXXXXX)

PROMQL_msgs_in_broker='${PROMQL_msgs_in_broker}'
PROMQL_msgs_in_topic='${PROMQL_msgs_in_topic}'
PROMQL_bytes_in_broker='${PROMQL_bytes_in_broker}'
PROMQL_log_size='${PROMQL_log_size}'
PROMQL_log_bytes='${PROMQL_log_bytes}'
PROMQL_leaders='${PROMQL_leaders}'
PROMQL_handler_idle='${PROMQL_handler_idle}'
PROMQL_netproc_idle='${PROMQL_netproc_idle}'
PROMQL_request_queue='${PROMQL_request_queue}'
PROMQL_cpu_broker='${PROMQL_cpu_broker}'
PROMQL_nic_rx='${PROMQL_nic_rx}'
PROMQL_nic_tx='${PROMQL_nic_tx}'
PROMQL_disk_write='${PROMQL_disk_write}'
PROMQL_consumer_host='${PROMQL_consumer_host}'
PROMQL_kingman='${PROMQL_kingman}'
PROMQL_produce_latency='${PROMQL_produce_latency}'

run_query() {
  # \$1=PromQL  \$2=nazwa pliku wyjsciowego (wzgledem \$RDIR)
  curl -sG http://localhost:9090/api/v1/query_range \
    --data-urlencode "query=\$1" \
    --data-urlencode "start=${QSTART}" \
    --data-urlencode "end=${QEND}" \
    --data-urlencode "step=${STEP}" \
    -o "\${RDIR}/\$2" || true
}

run_query "\$PROMQL_msgs_in_broker"   "prom_msgs_in_broker.json"
run_query "\$PROMQL_msgs_in_topic"    "prom_msgs_in_topic.json"
run_query "\$PROMQL_bytes_in_broker"  "prom_bytes_in_broker.json"
run_query "\$PROMQL_log_size"         "prom_log_size.json"
run_query "\$PROMQL_log_bytes"        "prom_log_bytes.json"
run_query "\$PROMQL_leaders"          "prom_leaders.json"
run_query "\$PROMQL_handler_idle"     "prom_handler_idle.json"
run_query "\$PROMQL_netproc_idle"     "prom_netproc_idle.json"
run_query "\$PROMQL_request_queue"    "prom_request_queue.json"
run_query "\$PROMQL_cpu_broker"       "prom_cpu_broker.json"
run_query "\$PROMQL_nic_rx"           "prom_nic_rx.json"
run_query "\$PROMQL_nic_tx"           "prom_nic_tx.json"
run_query "\$PROMQL_disk_write"       "prom_disk_write.json"
run_query "\$PROMQL_consumer_host"    "prom_consumer_host.json"
run_query "\$PROMQL_kingman"          "prom_kingman.json"
run_query "\$PROMQL_produce_latency"  "prom_produce_latency.json"

echo "\$RDIR"
EOF
)

echo "Zbieram 15 artefaktow Prometheus (jeden SSH round-trip do $MON_INSTANCE) ..."
SSH_OUT=""
if ! SSH_OUT="$(gcloud compute ssh "${SSH_USER}@${MON_INSTANCE}" --zone="$GCP_ZONE" --command="$REMOTE_CMD")"; then
  echo "ERROR: gcloud compute ssh do $MON_INSTANCE nie powiodl sie; brak artefaktow Prometheus dla $RUN_DIR." >&2
  exit 1
fi

RDIR_REMOTE="$(printf '%s\n' "$SSH_OUT" | tail -n 1)"
if [[ ! "$RDIR_REMOTE" =~ ^/tmp/promcollect_v2\. ]]; then
  echo "ERROR: nie udalo sie ustalic zdalnego katalogu tymczasowego." >&2
  echo "       Ostatnia linia SSH: '$RDIR_REMOTE'" >&2
  exit 1
fi
echo "Zdalny katalog kolekcji: $RDIR_REMOTE"

FETCH_TMP="$RUN_DIR/.prom_fetch_tmp.$$"
rm -rf "$FETCH_TMP"
mkdir -p "$FETCH_TMP"

if ! gcloud compute scp --recurse --zone="$GCP_ZONE" \
     "${SSH_USER}@${MON_INSTANCE}:${RDIR_REMOTE}" "$FETCH_TMP"; then
  echo "ERROR: gcloud compute scp --recurse nie powiodl sie ($RDIR_REMOTE)." >&2
  rm -rf "$FETCH_TMP"
  exit 1
fi

FETCHED_SUBDIR="$(find "$FETCH_TMP" -mindepth 1 -maxdepth 1 -type d -print | head -n 1)"
if [[ -z "$FETCHED_SUBDIR" ]]; then
  echo "ERROR: nieoczekiwana struktura po scp; brak podkatalogu w $FETCH_TMP." >&2
  rm -rf "$FETCH_TMP"
  exit 1
fi

mkdir -p "$PROM_DIR"
mv "$FETCHED_SUBDIR"/* "$PROM_DIR"/ 2>/dev/null || true
rm -rf "$FETCH_TMP"

echo "Pobrane artefakty -> $PROM_DIR"

check_one() {
  local out_name="$1"
  local out_path="$PROM_DIR/$out_name"
  if [[ ! -f "$out_path" ]]; then
    echo "WARNING: $out_name nie zostal pobrany." >&2
    return 1
  fi
  if [[ ! -s "$out_path" ]]; then
    echo "WARNING: $out_name jest pusty." >&2
    return 1
  fi
  if ! grep -q '"status":"success"' "$out_path"; then
    echo "WARNING: $out_name nie zawiera \"status\":\"success\" (blad API Prometheusa lub odpowiedz nie-JSON)." >&2
    return 1
  fi
  echo "  OK: $out_name ($(wc -c < "$out_path") bytes)"
  return 0
}

FAIL_COUNT=0
TOTAL=16
for f in prom_msgs_in_broker.json prom_msgs_in_topic.json prom_bytes_in_broker.json \
         prom_log_size.json prom_log_bytes.json prom_leaders.json prom_handler_idle.json prom_netproc_idle.json \
         prom_request_queue.json prom_cpu_broker.json prom_nic_rx.json prom_nic_tx.json \
         prom_disk_write.json prom_consumer_host.json prom_kingman.json prom_produce_latency.json; do
  check_one "$f" || FAIL_COUNT=$(( FAIL_COUNT + 1 ))
done

OK_COUNT=$(( TOTAL - FAIL_COUNT ))
echo "Collection summary: $OK_COUNT/$TOTAL Prometheus queries succeeded -> $PROM_DIR"
if [[ "$DASH_WARN" -gt 0 ]]; then
  echo "Dashboard API: $DASH_WARN/3 plikow (manifest/series/ack) nieudanych — patrz OSTRZEZENIA wyzej."
fi

if [[ "$FAIL_COUNT" -eq "$TOTAL" ]]; then
  echo "ERROR: wszystkie $TOTAL zapytan Prometheus nieudane; brak uzytecznych dowodow JMX/Prometheus dla $RUN_DIR." >&2
  exit 1
fi

echo "Zakonczono collect_run_artifacts.sh dla run_id=$RUN_ID -> $RUN_DIR"
exit 0
