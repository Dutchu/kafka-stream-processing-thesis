# Terraform v2 — sieć serverless (`thesis-v2/terraform/`)

**Spec:** `../spec/spec-infra.md` R4. **Stan:** własny, lokalny (`terraform.tfstate` w tym katalogu) —
NIE współdzielony z `C:\thesis\terraform\`. Zero zmian w istniejącym stanie/infrastrukturze poza
odczytem istniejącej sieci `kafka-thesis-vpc` przez `data "google_compute_network"`.

## Zasoby tworzone przez ten moduł (dokładnie 2)

1. `google_compute_subnetwork.serverless` — `kafka-serverless-subnet`, `10.8.0.0/23`, region `europe-central2`.
   Używana przez Cloud Run functions gen2 (`hexweather-producer-v2`) do Direct VPC egress
   (`gcloud run services update --network kafka-thesis-vpc --subnet kafka-serverless-subnet`).
2. `google_compute_firewall.allow_serverless_to_kafka` — `allow-serverless-to-kafka`,
   `tcp:9092` z `10.8.0.0/23` na instancje z tagiem sieciowym `kafka`.

## ⚠️ Otwarte pytanie / założenie (zapisane też w raporcie końcowym W1)

Reguła firewall filtruje po `target_tags = ["kafka"]`. Spec (§ R4) podaje `target_tags = ["kafka"]`
dosłownie, ale nie precyzuje, czy VM-y `kafka-1/2/3` w oryginalnym Terraform mają już ten tag sieciowy
przypisany (`main.tf` oryginału nie definiuje `google_compute_instance` w pliku, który czytaliśmy —
prawdopodobnie w osobnym `compute.tf`, poza zakresem lektury tego workera). **Przyjęty wariant
(najbliższy spec):** reguła używa `target_tags = ["kafka"]` dokładnie jak w spec. Autor musi:

- zweryfikować `gcloud compute instances describe kafka-1 --zone europe-central2-a --format='value(tags.items)'`
  na wszystkich trzech brokerach,
- jeśli tag `kafka` nie jest ustawiony, dodać go read-write poleceniem addytywnym (nie zarządzanym przez
  ten moduł, żeby nie zaburzyć oryginalnego stanu Terraform):
  `gcloud compute instances add-tags kafka-1 kafka-2 kafka-3 --tags=kafka --zone europe-central2-a`.

Bez tego tagu reguła firewall nie obejmie żadnej instancji i funkcja nie połączy się z brokerami przez
Direct VPC egress (objaw w RUNBOOK-v2 §6 troubleshooting: "funkcja bez dostępu do 9092").

## Komendy

```bash
cd thesis-v2/terraform
terraform init
terraform plan
# Oczekiwany wynik: "Plan: 2 to add, 0 to change, 0 to destroy." — dokładnie te
# dwa zasoby wypisane wyżej, ZERO odwołań do zasobów istniejącego stanu
# (data source nie liczy się jako "change").
terraform apply
# ... później, po zakończeniu prac (RUNBOOK-v2 §5 teardown), jeśli autor
# zdecyduje się usunąć sieć serverless:
terraform destroy
```

## Zmienne (`variables.tf`)

| Zmienna | Domyślna | Uwaga |
| --- | --- | --- |
| `project_id` | `example-project` | jak w oryginale — nadpisz `-var` jeśli inny projekt |
| `region` | `europe-central2` | jak w oryginale |
| `zone` | `europe-central2-a` | jak w oryginale |
| `existing_vpc_name` | `kafka-thesis-vpc` | nazwa czytana przez `data` — musi istnieć |
| `serverless_subnet_cidr` | `10.8.0.0/23` | zgodnie ze spec R4 |

Provider `google ~> 5.x`, identycznie jak w `C:\thesis\terraform\provider.tf`.
