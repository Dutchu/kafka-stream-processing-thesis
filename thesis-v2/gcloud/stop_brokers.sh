#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cluster_hosts_v2.env
source "$SCRIPT_DIR/cluster_hosts_v2.env"

usage() {
  echo "Usage: $0 <numer> [<numer> ...]" >&2
  echo "  <numer>   jeden lub wiecej numerow brokera z {1,2,3} (np. 2 3)" >&2
  echo "Przyklad: $0 2 3   # zatrzymuje kafka-2 i kafka-3 (Faza A)" >&2
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

echo "Zatrzymuje VM: ${INSTANCES[*]} (strefa $GCP_ZONE) ..."
gcloud compute instances stop "${INSTANCES[@]}" --zone "$GCP_ZONE"

echo "Zazadano zatrzymania: ${INSTANCES[*]}."
echo "Weryfikacja statusu:"
gcloud compute instances list --filter="name:(${INSTANCES[*]// /\ OR\ name:})" \
  --format="table(name,status)" || true

echo "OCZEKIWANY WYNIK: STATUS=TERMINATED dla kazdej z instancji powyzej"
echo "(gcloud instances list moze pokazac STOPPING chwile po wywolaniu;"
echo "odczekaj i sprawdz ponownie, jesli tak)."
