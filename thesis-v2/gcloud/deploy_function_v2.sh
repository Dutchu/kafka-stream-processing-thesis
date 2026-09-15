#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster_hosts_v2.env
source "$SCRIPT_DIR/cluster_hosts_v2.env"

usage() {
  echo "Usage: $0 <1b-rf1|3b-rf1|3b-rf3> [--reconfigure-only]" >&2
  exit 1
}

[[ $# -ge 1 ]] || usage
CONFIG="$1"; shift || true
RECONFIGURE_ONLY=0
if [[ $# -ge 1 ]]; then
  case "$1" in
    --reconfigure-only) RECONFIGURE_ONLY=1; shift ;;
    *) echo "ERROR: nieznany argument '$1'" >&2; usage ;;
  esac
fi

case "$CONFIG" in
  1b-rf1) KAFKA_BROKERS="$BOOTSTRAP_1B" ;;
  3b-rf1|3b-rf3) KAFKA_BROKERS="$BOOTSTRAP" ;;
  *) echo "ERROR: CONFIG nieznany: '$CONFIG' (oczekiwano 1b-rf1|3b-rf1|3b-rf3)" >&2; usage ;;
esac

REGION="$GCP_REGION"
SERVICE_NAME="hexweather-producer-v2"
FUNCTION_SOURCE="$SCRIPT_DIR/../kafka-app/function"
VPC_NAME="kafka-thesis-vpc"
SERVERLESS_SUBNET="kafka-serverless-subnet"
FALLBACK_CONNECTOR="kafka-conn-waw"
FUNCTION_URL_ENV="$SCRIPT_DIR/function_url.env"

echo "CONFIG=$CONFIG  KAFKA_BROKERS=$KAFKA_BROKERS  RECONFIGURE_ONLY=$RECONFIGURE_ONLY"

if [[ "$RECONFIGURE_ONLY" -eq 1 ]]; then
  echo "Tryb --reconfigure-only: aktualizuje wylacznie KAFKA_BROKERS na istniejacej usludze $SERVICE_NAME ..."
  if ! gcloud run services update "$SERVICE_NAME" \
      --region "$REGION" \
      --update-env-vars "KAFKA_BROKERS=${KAFKA_BROKERS}"; then
    echo "ERROR: aktualizacja KAFKA_BROKERS na $SERVICE_NAME nie powiodla sie." >&2
    echo "       Sprawdz, czy usluga istnieje: gcloud run services describe $SERVICE_NAME --region $REGION" >&2
    exit 1
  fi
  echo "OK: KAFKA_BROKERS zaktualizowany na $SERVICE_NAME (region $REGION)."
else
  echo "Krok 1/3: gcloud functions deploy --gen2 (bez --vpc-connector) ..."
  [[ -d "$FUNCTION_SOURCE" ]] || {
    echo "ERROR: brak katalogu zrodlowego funkcji: $FUNCTION_SOURCE" >&2
    echo "       (oczekiwano thesis-v2/kafka-app/function/, dostarczanego przez workstream W2)" >&2
    exit 1
  }

  if ! gcloud functions deploy "$SERVICE_NAME" \
      --gen2 \
      --region "$REGION" \
      --runtime java21 \
      --source "$FUNCTION_SOURCE" \
      --entry-point com.thesis.kafka.WeatherFunction \
      --trigger-http \
      --allow-unauthenticated \
      --memory 1Gi \
      --cpu 1 \
      --concurrency 1 \
      --timeout 600s \
      --max-instances 100 \
      --set-env-vars "^~^KAFKA_BROKERS=${KAFKA_BROKERS}~KAFKA_ACKS=${KAFKA_ACKS:-all}~CONTROL_TOPIC=control-metrics"; then
    echo "ERROR: gcloud functions deploy nie powiodl sie dla $SERVICE_NAME." >&2
    exit 1
  fi
  echo "OK: $SERVICE_NAME wdrozona (gen2, region $REGION)."

  echo ""
  echo "Krok 2/3: gcloud run services update -- Direct VPC egress ..."
  UPDATE_OUT=""
  UPDATE_RC=0
  UPDATE_OUT="$(gcloud run services update "$SERVICE_NAME" \
      --region "$REGION" \
      --network "$VPC_NAME" \
      --subnet "$SERVERLESS_SUBNET" \
      --vpc-egress private-ranges-only 2>&1)" || UPDATE_RC=$?

  printf '%s\n' "$UPDATE_OUT"

  if [[ "$UPDATE_RC" -ne 0 ]]; then
    if printf '%s' "$UPDATE_OUT" | grep -qiE 'unrecognized arguments|invalid choice|unknown flag|--network|--subnet|--vpc-egress'; then
      echo "" >&2
      echo "##############################################################" >&2
      echo "BLAD: flaga Direct VPC egress (--network/--subnet/--vpc-egress)" >&2
      echo "      nie jest wspierana przez zainstalowana wersje gcloud." >&2
      echo "      Fallback (connector istniejacy, nietykniety): uruchom recznie:" >&2
      echo "" >&2
      echo "gcloud run services update ${SERVICE_NAME} --region ${REGION} \\" >&2
      echo "    --vpc-connector ${FALLBACK_CONNECTOR} \\" >&2
      echo "    --vpc-egress private-ranges-only" >&2
      echo "" >&2
      echo "Autor decyduje, czy uzyc fallbacku (wspoldzieli connector z v1)." >&2
      echo "##############################################################" >&2
      exit 2
    fi
    echo "ERROR: gcloud run services update nie powiodl sie z innego powodu (patrz wyzej)." >&2
    exit 1
  fi
  echo "OK: Direct VPC egress skonfigurowany ($VPC_NAME / $SERVERLESS_SUBNET)."
fi

echo ""
echo "Krok 3/3: odczyt URL uslugi ..."
FUNCTION_URL="$(gcloud functions describe "$SERVICE_NAME" --gen2 --region "$REGION" --format='value(serviceConfig.uri)')"
if [[ -z "$FUNCTION_URL" ]]; then
  echo "ERROR: nie udalo sie odczytac serviceConfig.uri dla $SERVICE_NAME." >&2
  exit 1
fi

{
  echo "# function_url.env — wygenerowany przez deploy_function_v2.sh $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "export FUNCTION_URL=\"${FUNCTION_URL}\""
} > "$FUNCTION_URL_ENV"

echo "FUNCTION_URL zapisany do $FUNCTION_URL_ENV: $FUNCTION_URL"
echo ""
echo "UWAGA: ten skrypt nigdy nie modyfikuje ani nie usuwa funkcji"
echo "'hexweather-producer' (v1) ani connectora '${FALLBACK_CONNECTOR}'."
