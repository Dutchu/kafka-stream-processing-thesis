#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster_hosts_v2.env
source "$SCRIPT_DIR/cluster_hosts_v2.env"

usage() {
  echo "Usage: $0 <numer> [<numer> ...]" >&2
  echo "  <numer>   jeden lub wiecej numerow brokera z {1,2,3} (np. 2 3)" >&2
  echo "Przyklad: $0 2 3   # uruchamia kafka-2 i kafka-3 (Faza B)" >&2
  exit 1
}

[[ $# -ge 1 ]] || usage

INSTANCES=()
for n in "$@"; do
  case "$n" in
    1) INSTANCES+=("$BROKER1_INSTANCE") ;;
    2) INSTANCES+=("$BROKER2_INSTANCE") ;;
    3) INSTANCES+=("$BROKER3_INSTANCE") ;;
    *) echo "ERROR: numer brokera musi byc 1, 2 lub 3 (otrzymano '$n')" >&2; usage ;;
  esac
done

echo "Uruchamiam VM: ${INSTANCES[*]} (strefa $GCP_ZONE) ..."
gcloud compute instances start "${INSTANCES[@]}" --zone "$GCP_ZONE"

SSH_RETRIES=10
SSH_DELAY_S=10

for inst in "${INSTANCES[@]}"; do
  echo "--- Czekam na SSH: $inst ---"
  ok=0
  for (( i=1; i<=SSH_RETRIES; i++ )); do
    if gcloud compute ssh "${SSH_USER}@${inst}" --zone "$GCP_ZONE" --command="echo ssh_ok" &>/dev/null; then
      ok=1
      break
    fi
    echo "  proba $i/$SSH_RETRIES nieudana, czekam ${SSH_DELAY_S}s..."
    sleep "$SSH_DELAY_S"
  done
  if [[ "$ok" -ne 1 ]]; then
    echo "OSTRZEZENIE: $inst nie odpowiedzial po SSH po $((SSH_RETRIES*SSH_DELAY_S))s." >&2
    echo "             Sprawdz recznie: gcloud compute ssh ${SSH_USER}@${inst} --zone $GCP_ZONE" >&2
    continue
  fi

  STATUS="$(gcloud compute ssh "${SSH_USER}@${inst}" --zone "$GCP_ZONE" \
    --command="systemctl is-active kafka 2>&1 || true" 2>/dev/null | tail -n 1 | tr -d '[:space:]')"
  echo "  $inst: systemctl is-active kafka -> ${STATUS:-brak_odpowiedzi}"
done

echo ""
echo "OCZEKIWANY WYNIK: SSH dostepny na kazdym z ${INSTANCES[*]}."
echo "  - jesli nastepnym krokiem jest 'kraft_reconfigure quorum_size=3':"
echo "    stan kafka PRZED tym playbookiem jest bez znaczenia (playbook sam"
echo "    zatrzyma/sformatuje/uruchomi usluge)."
echo "  - jesli VM byly juz wczesniej sformatowane w trybie 3 i tylko"
echo "    restartujesz je bez zmiany kworum: oczekiwane 'active'."
echo "Ten skrypt NIE startuje ani nie zatrzymuje uslugi kafka — patrz"
echo "ansible/kraft_reconfigure.yml (R1)."
