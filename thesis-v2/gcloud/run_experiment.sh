#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=cluster_hosts_v2.env
source "$SCRIPT_DIR/cluster_hosts_v2.env"

usage() {
  echo "Usage: $0 <E1|E4|E4R|E2> <CONFIG> [--runs 3] [--start 1] [--mu <msg/s>] [--P <n>]" >&2
  echo "  <CONFIG>  1b-rf1 | 3b-rf1 | 3b-rf3" >&2
  echo "  HW env (optional): es4 | esmall | emicro — empty (default) = plain <CONFIG>" >&2
  echo "  --mu      wymagane dla E2, chyba ze analysis/output/profiles.json ma muMsgs dla CONFIG" >&2
  echo "  --P       tylko E4R: parallelism (default 20)" >&2
  exit 1
}

[[ $# -ge 2 ]] || usage
EXP="$1"; shift
CONFIG="$1"; shift
HW="${HW:-}"
CFG_HW="${CONFIG}${HW:+-$HW}"

RUNS=3
START=1
MU=""
E4R_P=20
while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)   [[ $# -ge 2 ]] || usage; RUNS="$2"; shift 2 ;;
    --runs=*) RUNS="${1#--runs=}"; shift ;;
    --start)   [[ $# -ge 2 ]] || usage; START="$2"; shift 2 ;;
    --start=*) START="${1#--start=}"; shift ;;
    --mu)   [[ $# -ge 2 ]] || usage; MU="$2"; shift 2 ;;
    --mu=*) MU="${1#--mu=}"; shift ;;
    --P)   [[ $# -ge 2 ]] || usage; E4R_P="$2"; shift 2 ;;
    --P=*) E4R_P="${1#--P=}"; shift ;;
    -h|--help) usage ;;
    *) echo "ERROR: nieznany argument '$1'" >&2; usage ;;
  esac
done

case "$EXP" in E1|E4|E4R|E2) ;; *) echo "ERROR: EXP musi byc E1|E4|E4R|E2 (otrzymano '$EXP')" >&2; usage ;; esac
case "$CONFIG" in 1b-rf1|3b-rf1|3b-rf3) ;; *) echo "ERROR: CONFIG nieznany '$CONFIG'" >&2; usage ;; esac
if [[ -n "$HW" ]]; then
  case "$HW" in es4|esmall|emicro) ;; *) echo "ERROR: HW musi byc es4|esmall|emicro (otrzymano '$HW')" >&2; usage ;; esac
fi
[[ "$RUNS"  =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: --runs musi byc dodatnia liczba calkowita" >&2; exit 1; }
[[ "$START" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: --start musi byc dodatnia liczba calkowita" >&2; exit 1; }
[[ "$E4R_P" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: --P musi byc dodatnia liczba calkowita" >&2; exit 1; }

TOPICS_JSON='["weather-rain","weather-temp","weather-wind"]'
if [[ -n "${TOPICS:-}" ]]; then TOPICS_JSON="$TOPICS"; fi
LOG_DIR="$REPO_ROOT/results/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/${EXP}_${CONFIG}-${HW}_$(date -u +%Y%m%d).log"

log() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$LOG_FILE" >&2
}

log "=== run_experiment.sh start: EXP=$EXP CONFIG=$CONFIG RUNS=$RUNS START=$START MU=${MU:-<none>} ==="

DASH_VIA_SSH="${DASH_VIA_SSH:-1}"

api_post() {
  if [[ "$DASH_VIA_SSH" -eq 1 ]]; then
    timeout 90 gcloud compute ssh "${SSH_USER}@${LOAD_INSTANCE}" --zone="$GCP_ZONE" \
      --command="curl -sf --max-time 15 -X POST 'localhost:8080/api/run' -H 'Content-Type: application/json' -d '$1'"
  else
    curl -sf --max-time 15 -X POST "${DASH_URL}/api/run" \
      -H 'Content-Type: application/json' -d "$1"
  fi
}

api_get() {
  if [[ "$DASH_VIA_SSH" -eq 1 ]]; then
    timeout 90 gcloud compute ssh "${SSH_USER}@${LOAD_INSTANCE}" --zone="$GCP_ZONE" \
      --command="curl -sf --max-time 15 'localhost:8080$1'"
  else
    curl -sf --max-time 15 "${DASH_URL}$1"
  fi
}

post_run() {
  local body="$1"
  local resp
  if ! resp="$(api_post "$body")"; then
    return 1
  fi
  printf '%s' "$resp" | grep -oE '"runId"[[:space:]]*:[[:space:]]*"[^"]+"' | head -n1 | sed -E 's/.*:"([^"]+)"/\1/'
}

get_status() {
  local run_id="$1"
  api_get "/api/runs/${run_id}/summary" 2>/dev/null \
    | grep -oE '"status"[[:space:]]*:[[:space:]]*"[^"]+"' | head -n1 | sed -E 's/.*:"([^"]+)"/\1/'
}

get_field_num() {
  local run_id="$1" field="$2"
  api_get "/api/runs/${run_id}/summary" 2>/dev/null \
    | grep -oE "\"${field}\"[[:space:]]*:[[:space:]]*[-0-9.eE]+" | head -n1 | grep -oE '[-0-9.eE]+$'
}

get_field_bool() {
  local run_id="$1" field="$2"
  api_get "/api/runs/${run_id}/summary" 2>/dev/null \
    | grep -oE "\"${field}\"[[:space:]]*:[[:space:]]*(true|false)" | head -n1 | grep -oE '(true|false)$'
}

wait_finished() {
  local run_id="$1"
  local elapsed=0
  local timeout=900
  while true; do
    local st
    st="$(get_status "$run_id" || true)"
    if [[ "$st" == "finished" || "$st" == "aborted" || "$st" == "failed" ]]; then
      echo "$st"
      return 0
    fi
    if (( elapsed >= timeout )); then
      echo "timeout"
      return 1
    fi
    sleep 5
    elapsed=$(( elapsed + 5 ))
  done
}

run_one() {
  local body="$1"
  local run_dir="$2"
  mkdir -p "$run_dir"

  log "POST /api/run -> $body"
  local run_id
  if ! run_id="$(post_run "$body")" || [[ -z "$run_id" ]]; then
    log "FAILED: POST /api/run nie zwrocil runId dla $run_dir"
    return 1
  fi
  log "runId=$run_id -> $run_dir"

  local final_status
  final_status="$(wait_finished "$run_id")"
  log "status koncowy runId=$run_id: $final_status"
  if [[ "$final_status" != "finished" ]]; then
    log "FAILED: bieg $run_id nie zakonczyl sie stanem 'finished' (status=$final_status) -> $run_dir"
  fi

  if ! COLLECT_VIA_SSH="$DASH_VIA_SSH" bash "$SCRIPT_DIR/collect_run_artifacts.sh" "$run_dir" "$run_id"; then
    log "WARNING: collect_run_artifacts.sh zwrocil blad dla $run_id -> $run_dir (bieg NIE oznaczony FAILED z tego powodu, patrz osobno)"
  fi

  log "Przerwa 15s po biegu $run_id ..."
  sleep 15

  echo "$run_id"
}

run_e1() {
  local run
  for (( i=0; i<RUNS; i++ )); do
    run=$(( START + i ))
    local body
    body=$(cat << JSON
{"exp":"E1","config":"${CONFIG}","label":"e1","parallelism":1,"ratePerSec":20,"durationSec":90,"topics":${TOPICS_JSON},"runIndex":${run}}
JSON
)
    run_one "$body" "$REPO_ROOT/results/E1/${CFG_HW}/run${run}" || true
  done
}

run_e4() {
  local ladder=(10 25 50 100)
  local prev_lambda=""
  local prev2_lambda=""
  local stop_ladder=0

  for P in "${ladder[@]}"; do
    if [[ "$stop_ladder" -eq 1 ]]; then
      log "Drabinka E4 zatrzymana przed P=$P (plateau + sygnatura na poprzednich 2 szczeblach)."
      break
    fi

    local last_run_id=""
    for (( i=0; i<RUNS; i++ )); do
      local run=$(( START + i ))
      local run_dir="$REPO_ROOT/results/E4/${CFG_HW}/P${P}/run${run}"
      local body
      body=$(cat << JSON
{"exp":"E4","config":"${CONFIG}","label":"p${P}","parallelism":${P},"ratePerSec":0,"durationSec":120,"topics":${TOPICS_JSON},"runIndex":${run}}
JSON
)
      last_run_id="$(run_one "$body" "$run_dir" || true)"
    done

    if [[ "$P" != "100" ]]; then
      log "Miedzy-P: cleanup_topics_v2.sh && create_topics.sh (E4 dysk) ..."
      if ! bash "$SCRIPT_DIR/cleanup_topics_v2.sh"; then
        log "WARNING: cleanup_topics_v2.sh nieudany miedzy P=$P a nastepnym szczeblem."
      fi
      if ! bash "$SCRIPT_DIR/create_topics.sh" "$CONFIG"; then
        log "WARNING: create_topics.sh nieudany po cleanup miedzy szczeblami E4."
      fi
      log "Przerwa 20s (miedzy-P) ..."
      sleep 20
    fi

    if [[ -n "$last_run_id" ]]; then
      local lambda fired
      lambda="$(get_field_num "$last_run_id" "lambdaMean" || true)"
      fired="$(get_field_bool "$last_run_id" "fired" || true)"
      log "P=$P: lambdaMean=${lambda:-<brak>} saturationSignature.fired=${fired:-<brak>}"

      if [[ -n "$lambda" && -n "$prev_lambda" && -n "$prev2_lambda" ]]; then
        local grew_pct
        grew_pct=$(awk -v a="$lambda" -v b="$prev_lambda" 'BEGIN{ if (b==0) {print 999} else {print ((a-b)/b)*100} }')
        local grew_pct_prev
        grew_pct_prev=$(awk -v a="$prev_lambda" -v b="$prev2_lambda" 'BEGIN{ if (b==0) {print 999} else {print ((a-b)/b)*100} }')
        local plateau
        plateau=$(awk -v g1="$grew_pct" -v g2="$grew_pct_prev" 'BEGIN{ print (g1<=5 && g2<=5) ? 1 : 0 }')

        if [[ "$plateau" -eq 1 && "$fired" == "true" ]]; then
          log "Sygnatura saturacji + plateau (<=5% wzrostu przez 2 kolejne P) wykryte na P=$P -> zatrzymuje drabinke po tym szczeblu."
          stop_ladder=1
        fi
      fi
      prev2_lambda="$prev_lambda"
      prev_lambda="$lambda"
    fi
  done
}

run_e4r() {
  local rates=(${E4R_RATES:-500 1000 2000 4000 8000 16000 0})
  local prev_lambda=""
  local prev2_lambda=""
  local stop_ladder=0

  for RATE in "${rates[@]}"; do
    if [[ "$stop_ladder" -eq 1 ]]; then
      log "Drabinka E4R zatrzymana przed rate=$RATE (plateau + sygnatura na poprzednich 2 szczeblach)."
      break
    fi

    local last_run_id=""
    for (( i=0; i<RUNS; i++ )); do
      local run=$(( START + i ))
      local run_dir="$REPO_ROOT/results/E4/${CFG_HW}/R${RATE}/run${run}"
      local body
      body=$(cat << JSON
{"exp":"E4","config":"${CONFIG}","label":"r${RATE}","parallelism":${E4R_P},"ratePerSec":${RATE},"durationSec":120,"topics":${TOPICS_JSON},"runIndex":${run}}
JSON
)
      last_run_id="$(run_one "$body" "$run_dir" || true)"
    done

    if [[ "$RATE" != "0" ]]; then
      log "Miedzy-rate: cleanup_topics_v2.sh && create_topics.sh (E4 dysk) ..."
      if ! bash "$SCRIPT_DIR/cleanup_topics_v2.sh"; then
        log "WARNING: cleanup_topics_v2.sh nieudany miedzy rate=$RATE a nastepnym szczeblem."
      fi
      if ! bash "$SCRIPT_DIR/create_topics.sh" "$CONFIG"; then
        log "WARNING: create_topics.sh nieudany po cleanup miedzy szczeblami E4R."
      fi
      log "Przerwa 20s (miedzy-rate) ..."
      sleep 20
    fi

    if [[ -n "$last_run_id" ]]; then
      local lambda fired
      lambda="$(get_field_num "$last_run_id" "lambdaMean" || true)"
      fired="$(get_field_bool "$last_run_id" "fired" || true)"
      log "rate=$RATE: lambdaMean=${lambda:-<brak>} saturationSignature.fired=${fired:-<brak>}"

      if [[ -n "$lambda" && -n "$prev_lambda" && -n "$prev2_lambda" ]]; then
        local grew_pct
        grew_pct=$(awk -v a="$lambda" -v b="$prev_lambda" 'BEGIN{ if (b==0) {print 999} else {print ((a-b)/b)*100} }')
        local grew_pct_prev
        grew_pct_prev=$(awk -v a="$prev_lambda" -v b="$prev2_lambda" 'BEGIN{ if (b==0) {print 999} else {print ((a-b)/b)*100} }')
        local plateau
        plateau=$(awk -v g1="$grew_pct" -v g2="$grew_pct_prev" 'BEGIN{ print (g1<=5 && g2<=5) ? 1 : 0 }')

        if [[ "$plateau" -eq 1 && "$fired" == "true" ]]; then
          log "Sygnatura saturacji + plateau (<=5% wzrostu przez 2 kolejne rate) wykryte na rate=$RATE -> zatrzymuje drabinke po tym szczeblu."
          stop_ladder=1
        fi
      fi
      prev2_lambda="$prev_lambda"
      prev_lambda="$lambda"
    fi
  done
}

resolve_mu() {
  if [[ -n "$MU" ]]; then
    echo "$MU"
    return 0
  fi
  local r0vals=""
  for rf in "$REPO_ROOT"/results/E4/"${CFG_HW}"/R0/run*/run.json; do
    [[ -f "$rf" ]] || continue
    local lam
    lam="$(grep -oE '"lambdaMean"[[:space:]]*:[[:space:]]*[-0-9.eE]+' "$rf" | head -n1 | grep -oE '[-0-9.eE]+$')"
    [[ -n "$lam" ]] || continue
    r0vals="$r0vals $lam"
  done
  if [[ -n "$r0vals" ]]; then
    # shellcheck disable=SC2086
    local med
    med="$(printf '%s\n' $r0vals | sort -n | awk '{a[NR]=$1} END{ if (NR%2==1) print a[(NR+1)/2]; else print (a[NR/2]+a[NR/2+1])/2 }')"
    if [[ -n "$med" ]]; then
      log "E2: mu = mediana R0 (osiagniete) = $med msg/s"
      echo "$med"
      return 0
    fi
  fi
  local profiles="$REPO_ROOT/analysis/output/profiles.json"
  if [[ -f "$profiles" ]]; then
    local block
    block="$(awk -v cfg="\"${CONFIG}\"" 'index($0,cfg){f=1} f{print; if (/}/&&f && NR>1) exit}' "$profiles")"
    local mu
    mu="$(printf '%s' "$block" | grep -oE '"muMsgs"[[:space:]]*:[[:space:]]*[0-9.]+' | head -n1 | grep -oE '[0-9.]+$')"
    if [[ -n "$mu" ]]; then
      echo "$mu"
      return 0
    fi
  fi
  return 1
}

run_e2() {
  local mu
  if ! mu="$(resolve_mu)"; then
    log "ERROR: E2 wymaga --mu <msg/s> lub mediany R0 z dysku lub $REPO_ROOT/analysis/output/profiles.json z muMsgs dla CONFIG=$CONFIG."
    exit 1
  fi
  local eff="${EFF:-1}"
  log "E2: mu=$mu msg/s (CONFIG=$CONFIG), EFF=$eff"

  local rhos=(0.25 0.5 0.75 0.9)
  local labels=(25 50 75 90)
  for idx in "${!rhos[@]}"; do
    local rho="${rhos[$idx]}"
    local label="${labels[$idx]}"
    local rate
    rate=$(awk -v r="$rho" -v m="$mu" -v e="$eff" 'BEGIN{ printf "%d", (r*m/20/e)+0.5 }')

    for (( i=0; i<RUNS; i++ )); do
      local run=$(( START + i ))
      local run_dir="$REPO_ROOT/results/E2/${CFG_HW}/rho${label}/run${run}"
      local body
      body=$(cat << JSON
{"exp":"E2","config":"${CONFIG}","label":"rho${label}","parallelism":20,"ratePerSec":${rate},"durationSec":120,"topics":${TOPICS_JSON},"intensity":${rho},"runIndex":${run}}
JSON
)
      run_one "$body" "$run_dir" || true
    done
  done
}

case "$EXP" in
  E1) run_e1 ;;
  E4) run_e4 ;;
  E4R) run_e4r ;;
  E2) run_e2 ;;
esac

log "=== run_experiment.sh zakonczony: EXP=$EXP CONFIG=$CONFIG (log: $LOG_FILE) ==="
