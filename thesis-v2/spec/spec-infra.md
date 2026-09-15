# Spec W1 — Infrastruktura i automatyzacja (thesis-v2)

**Właściciel:** Infra agent. **Plan:** `thesis-v2/experiment-plan.md` §4, §5, §8.
**Zasada nadrzędna:** operujemy **addytywnie i odwracalnie** na istniejącej infrastrukturze GCP. Nic w `C:\thesis\{ansible,gcloud,terraform,monitoring_node}` nie jest modyfikowane — kopiujemy wzorce do `thesis-v2/` i tam zmieniamy. Żadnych nowych VM. Żadnego ponownego pobierania/instalowania Kafki (jest w `/opt/kafka` → `/opt/kafka_2.13-3.7.0` na kafka-1/2/3; Java jest na kafka-load).

## 1. Cel i zakres

Dostarczyć skrypty i playbooki, którymi autor (jedyny aktor z `gcloud`/SSH) przeprowadzi: (a) przełączenie klastra między trybem **1 brokera** (kafka-1, 1-węzłowe kworum KRaft; kafka-2/3 zatrzymane) i **3 brokerów** (3-węzłowe kworum); (b) tworzenie/czyszczenie tematów wg konfiguracji; (c) sieć dla funkcji (Direct VPC egress) i deploy funkcji v2; (d) deploy dashboardu v2 na kafka-load; (e) zbieranie artefaktów biegu i uruchamianie serii eksperymentów jedną komendą; (f) RUNBOOK-v2.

Poza zakresem: kod funkcji (W2), dashboardu (W3), reguły JMX/Grafana (W4 — ale W1 dostarcza playbook `update_monitoring_v2.yml` kopiujący pliki W4).

## 2. Wejścia (tylko do czytania)

- `C:\thesis\ansible\install_kafka.yml` — wzorzec `server.properties`, format, systemd (kopiować logikę).
- `C:\thesis\ansible\inventory.ini` — grupy `kafka_nodes` (10.0.0.6, .3, .4; node.id = indeks+1), `monitoring` (10.0.0.5), `load_generator` (10.0.0.2); user `researcher`.
- `C:\thesis\ansible\{update_monitoring.yml,deploy_dashboard.yml,wipe_topics.yml,check_clock_sync.yml}` — wzorce.
- `C:\thesis\gcloud\{cluster_hosts.env,collect_jmx_artifacts.sh,cleanup_topics.sh,deploy_function.sh,start.sh,stop.sh,run_scenario.sh}` — wzorce.
- `C:\thesis\terraform\main.tf` — VPC `kafka-thesis-vpc`, podsieć `kafka-thesis-subnet` 10.0.0.0/24, firewall `allow_internal` (source 10.0.0.0/24), `allow_external` (22/3000/9090/8080 z 0.0.0.0/0). Zona `europe-central2-a`, region `europe-central2`.
- Fakty: Kafka 3.7.0 nie wspiera dynamicznej zmiany kworum (KIP-853 od 3.9) → zmiana liczby voterów wymaga ponownego formatu katalogu metadanych z nowym cluster-id na wszystkich aktywnych węzłach. Cloud Run functions gen2 = usługa Cloud Run; Direct VPC egress konfigurowany przez `gcloud run services update`, wymaga podsieci i reguły firewall (ruch z podsieci serverless nie jest objęty `allow_internal`).

## 3. Wymagania

### R1 `ansible/kraft_reconfigure.yml` (hosts: kafka_nodes; parametr `-e quorum_size=1|3`)
1. Preflight: `assert quorum_size in [1,3]`; wypisuje plan działań; wymaga `-e confirm_wipe=yes` (dane i metadane zostaną skasowane).
2. Na **wszystkich** węzłach z inventory (także tych, które będą wyłączone, jeśli są osiągalne — `ignore_unreachable: yes`): `systemctl stop kafka`; usuń zawartość `/var/lib/kafka/data/*` (w tym `meta.properties` i `__cluster_metadata-0`).
3. Wygeneruj **jeden** nowy cluster-id (`/opt/kafka/bin/kafka-storage.sh random-uuid` na pierwszym węźle, `run_once`, `set_fact` dla wszystkich).
4. Zapisz `/opt/kafka/config/kraft/server.properties`:
   - `quorum_size=1`: `controller.quorum.voters=1@10.0.0.6:9093`; `default.replication.factor=1`, `offsets.topic.replication.factor=1`, `transaction.state.log.replication.factor=1`, `transaction.state.log.min.isr=1`.
   - `quorum_size=3`: jak w `install_kafka.yml` (3 voterzy, RF 3, min.isr 2).
   - Wspólne: `process.roles=broker,controller`, `node.id` jak dotąd, listenery jak dotąd, `num.partitions=6`, `log.retention.hours=24`, `auto.create.topics.enable=false` (tematy tworzone jawnie, żeby RF był kontrolowany).
5. `kafka-storage.sh format -t <cluster-id> -c server.properties` na węzłach aktywnych (`quorum_size=1` → tylko host 10.0.0.6; `=3` → wszystkie).
6. `systemctl start kafka` na węzłach aktywnych; węzły nieaktywne zostają zatrzymane (`enabled: no` na kafka-2/3 w trybie 1, przywrócić `enabled: yes` w trybie 3).
7. Post-check: `kafka-metadata-quorum.sh --bootstrap-server <aktywny>:9092 describe --status` pokazuje `LeaderId` i liczbę voterów = quorum_size; `kafka-broker-api-versions.sh` odpowiada. Wypisz `CLUSTER_ID`, `QUORUM_SIZE` i zapisz je do `/etc/kafka-thesis-v2.env` na aktywnych węzłach (używane przez kolektor do `config.env`).

### R2 `gcloud/stop_brokers.sh 2 3` / `gcloud/start_brokers.sh 2 3`
`gcloud compute instances stop|start kafka-N --zone europe-central2-a` dla podanych numerów; `start` czeka na SSH i wypisuje status `systemctl is-active kafka` (w trybie 3 kafka ma być `active`, ale **nie startuj kafka automatycznie** — o tym decyduje playbook R1). Sekwencja Faza A: `kraft_reconfigure quorum_size=1` (przy działających VM) → `stop_brokers.sh 2 3`. Sekwencja Faza B: `start_brokers.sh 2 3` → `kraft_reconfigure quorum_size=3`.

### R3 `gcloud/create_topics.sh <1b-rf1|3b-rf1|3b-rf3>` i `gcloud/cleanup_topics_v2.sh`
- Tematy `weather-rain`, `weather-temp`, `weather-wind`: `--partitions 6`, RF = 1 (1b-rf1, 3b-rf1) lub 3 (3b-rf3), `--config min.insync.replicas=1|2`, `--config retention.ms=86400000`.
- Temat `control-metrics`: `--partitions 1`, RF jak wyżej, `retention.ms=86400000`.
- Idempotentne (jeśli istnieje z inną RF → błąd z komunikatem „uruchom cleanup”). Wykonywane przez SSH na kafka-1 (jak `cleanup_topics.sh`), bootstrap = aktywny broker (dla 1b-rf1 tylko 10.0.0.6:9092!).
- `cleanup_topics_v2.sh`: usuwa `weather-*`, `control-metrics`, czeka aż znikną (wzorzec `cleanup_topics.sh`), po czym `create_topics.sh` można wywołać ponownie. Używany między biegami E4 (dysk) i przy zmianie CONFIG.

### R4 Terraform v2 (`thesis-v2/terraform/`, **własny stan lokalny**, provider google ~> 5.x jak w oryginale)
- `data "google_compute_network" "kafka_vpc" { name = "kafka-thesis-vpc" }`.
- `resource "google_compute_subnetwork" "serverless"`: `kafka-serverless-subnet`, `10.8.0.0/23`, region `europe-central2`, network = data.
- `resource "google_compute_firewall" "allow_serverless_to_kafka"`: network = `kafka-thesis-vpc`, allow tcp `9092`, `source_ranges = ["10.8.0.0/23"]`, `target_tags = ["kafka"]`.
- `terraform plan` musi pokazać dokładnie **2 zasoby do utworzenia**, zero zmian/usunięć. README z komendami `init/plan/apply/destroy`.

### R5 `gcloud/deploy_function_v2.sh`
1. `gcloud functions deploy hexweather-producer-v2 --gen2 --region europe-central2 --runtime java21 --source thesis-v2/kafka-app/function --entry-point com.thesis.kafka.WeatherFunction --trigger-http --allow-unauthenticated --memory 1Gi --cpu 1 --concurrency 1 --timeout 600s --max-instances 300 --set-env-vars "^~^KAFKA_BROKERS=<BOOTSTRAP>~KAFKA_ACKS=all~CONTROL_TOPIC=control-metrics"` — **bez** `--vpc-connector`.
2. Następnie `gcloud run services update hexweather-producer-v2 --region europe-central2 --network kafka-thesis-vpc --subnet kafka-serverless-subnet --vpc-egress private-ranges-only` (Direct VPC egress). Jeśli polecenie zgłosi, że flaga nie jest wspierana w tej wersji gcloud → wypisz dokładną komendę fallback z `--vpc-connector kafka-conn-waw` i zakończ z kodem 2 (autor decyduje).
3. `BOOTSTRAP` zależy od CONFIG: `1b-rf1` → `10.0.0.6:9092`; `3b-*` → wszystkie trzy. Skrypt przyjmuje `<CONFIG>` jako `$1` i wykonuje tylko `gcloud run services update --update-env-vars KAFKA_BROKERS=...` gdy funkcja już istnieje (`--reconfigure-only`).
4. Wypisz URL usługi (`gcloud functions describe … --format='value(serviceConfig.uri)'`) i zapisz do `thesis-v2/gcloud/function_url.env` (`export FUNCTION_URL=…`), który czyta dashboard (`FUNCTION_URL` jako env w systemd, R6) i `run_experiment.sh`.
5. Nie modyfikuj ani nie usuwaj funkcji `hexweather-producer` ani connectora `kafka-conn-waw`.

### R6 `ansible/deploy_dashboard_v2.yml` (hosts: load_generator)
Wzorzec `deploy_dashboard.yml`, ale: źródło `thesis-v2/kafka-app/dashboard` → `/opt/thesis-dashboard/`; Maven build; usługa `thesis-dashboard.service` z env: `KAFKA_BROKERS` (wg CONFIG, parametr `-e config=…`), `FUNCTION_URL` (z `function_url.env`), `DASHBOARD_PORT=8080`, `DB_PATH=/var/lib/thesis-dashboard/thesis.db`, `PROFILES_FILE=/opt/thesis-dashboard/profiles.json`; `-Xms512m -Xmx3g`. Kopiuje `thesis-v2/analysis/output/profiles.json` jeśli istnieje (μ/τ per CONFIG). Post-check: `curl -s localhost:8080/api/health` zwraca `{"status":"ok"}`.

### R7 `gcloud/collect_run_artifacts.sh <run_dir> <run_id>`
1. Pobiera z dashboardu (`http://10.0.0.2:8080`, przez `gcloud compute ssh kafka-load -- curl` **lub** bezpośrednio przez zewn. IP — parametr `DASH_URL` z `cluster_hosts_v2.env`): `GET /api/runs/<run_id>/manifest` → `run.json`; `GET /api/runs/<run_id>/series` → `series.json`; `GET /api/runs/<run_id>/ack` → `ack.json` (JSON prosto z SQLite dashboardu, żadnych CSV).
2. Z `run.json` odczytuje `startEpoch`, `stopEpoch`; odpytuje Prometheus na 10.0.0.5:9090 (`/api/v1/query_range`, `step=5s`, `start-30`, `end+60`) — wzorzec 2-hopowy z `collect_jmx_artifacts.sh` (jeden SSH wykonuje wszystkie zapytania do katalogu tymczasowego, jeden `scp --recurse`). Stały zbiór zapytań → `prom/*.json` (nazwy plików = klucze):
   - `prom_msgs_in_broker`: `sum by (broker) (rate(kafka_server_brokertopicmetrics_messagesin_total[30s]))`
   - `prom_msgs_in_topic`: `sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_topic_total[30s]))`
   - `prom_bytes_in_broker`: `sum by (broker) (rate(kafka_server_brokertopicmetrics_bytesin_total[30s]))`
   - `prom_log_size`: `sum by (broker, topic) (kafka_log_logendoffset - kafka_log_logstartoffset)`
   - `prom_log_bytes`: `sum by (broker, topic, partition) (kafka_log_Size)` (bajty logów per partycja — rozkład dyskowy bez SSH na brokery)
   - `prom_leaders`: `sum by (broker, topic) ((kafka_log_logendoffset - kafka_log_logstartoffset) and on (broker, topic, partition) (kafka_cluster_partition_replicascount > 0))`
   - `prom_handler_idle`: `kafka_server_requesthandleravgidlepercent`
   - `prom_netproc_idle`: `kafka_network_networkprocessoravgidlepercent`
   - `prom_request_queue`: `kafka_network_requestchannel_requestqueuesize`
   - `prom_cpu_broker`: `1 - avg by (node)(rate(node_cpu_seconds_total{mode="idle",node=~"kafka-.*"}[1m]))`
   - `prom_nic_rx`: `sum by (node)(rate(node_network_receive_bytes_total{node=~"kafka-.*",device!~"lo"}[30s]))`
   - `prom_nic_tx`: `sum by (node)(rate(node_network_transmit_bytes_total{node=~"kafka-.*",device!~"lo"}[30s]))`
   - `prom_disk_write`: `sum by (node)(rate(node_disk_written_bytes_total{node=~"kafka-.*"}[30s]))`
   - `prom_consumer_host`: `1 - avg(rate(node_cpu_seconds_total{mode="idle",node="kafka-load"}[1m]))`
   - `prom_kingman`: `{__name__=~"thesis_kingman_.*"}`
   - `prom_produce_latency`: `kafka_network_requestmetrics_totaltimems{request="Produce"}`
   Per-query WARN-and-continue; wszystkie nieudane → exit 1. Zapisz `window.env` (start/stop) i `config.env` (CONFIG, CLUSTER_ID, QUORUM_SIZE z `/etc/kafka-thesis-v2.env`, FUNCTION_URL, data).

### R8 `gcloud/run_experiment.sh <E1|E4|E2> <CONFIG> [--runs 3] [--start 1] [--mu <msg/s>]`
Wywołuje dashboard API (`POST /api/run`, kontrakt w `spec-dashboard.md` §4.3) z parametrami z tabeli planu §4. DASH_URL to wewnętrzny IP VPC — ze stacji wszystkie wywołania API idą przez `gcloud compute ssh kafka-load -- curl localhost:8080` (`DASH_VIA_SSH=1`, domyślne; `=0` tylko ze stacji z trasą do VPC). Kolektor dziedziczy tryb (`COLLECT_VIA_SSH=$DASH_VIA_SSH`).
- **E1:** `{"exp":"E1","config":CONFIG,"parallelism":1,"ratePerSec":20,"durationSec":90,"topics":["weather-rain","weather-temp","weather-wind"]}`.
- **E4:** dla `P` w `10 25 50 100 200 300`: `{"exp":"E4","parallelism":P,"ratePerSec":0,"durationSec":120,…}`; po każdym P czyta `GET /api/runs/<id>/summary` (λ_plateau, sygnatura) i **zatrzymuje drabinkę**, gdy λ nie wzrosło o > 5 % względem poprzedniego P przez 2 kolejne P i `saturationSignature=true`; między P: `cleanup_topics_v2.sh && create_topics.sh` (dysk) i 20 s przerwy. Katalog `results/E4/<CONFIG>/P<P>/run<N>/`.
- **E2:** wymaga `--mu` (lub czyta `profiles.json`): dla ρ ∈ {0.25,0.5,0.75,0.9}: `P=20`, `ratePerSec = round(ρ·μ/20)`, `durationSec=120`. Katalog `results/E2/<CONFIG>/rho<25|50|75|90>/run<N>/`.
- Po każdym biegu: czeka na `status=finished`, wywołuje `collect_run_artifacts.sh`, 15 s przerwy. Loguje do `results/logs/<EXP>_<CONFIG>_<date>.log`. Kody wyjścia ≠ 0 nie przerywają serii (bieg oznaczony `FAILED` w logu).

### R9 `RUNBOOK-v2.md`
Pełna sekwencja komend: 0) preflight (`gcloud auth`, `verify_monitoring` z oryginału jest OK do użycia read-only, `check_clock_sync`), 1) Terraform v2 apply, 2) `deploy_jmx_v2.yml` + `update_monitoring_v2.yml` (pliki od W4), 3) **Faza A**: `kraft_reconfigure quorum_size=1` → `stop_brokers 2 3` → `create_topics 1b-rf1` → `deploy_function_v2.sh 1b-rf1` → `deploy_dashboard_v2 -e config=1b-rf1` → `run_experiment E1` → `E4` → (pipeline → profiles.json → redeploy dashboard) → `E2`; 4) **Faza B**: `start_brokers 2 3` → `kraft_reconfigure quorum_size=3` → dla `3b-rf1` i `3b-rf3`: `cleanup/create_topics` → `deploy_function_v2.sh 3b-*` (zmiana KAFKA_BROKERS) → redeploy dashboard → E1/E4/E2; 5) teardown (stop VM, nie niszcz Terraform v2 do końca prac); 6) troubleshooting (kworum bez lidera, funkcja bez dostępu do 9092 → firewall/podsieć, dashboard lag).
`gcloud/cluster_hosts_v2.env`: kopia `cluster_hosts.env` + `DASH_URL`, `FUNCTION_URL` (source), `PROM_URL=http://10.0.0.5:9090`.

## 4. Układ plików

```
thesis-v2/
  ansible/ kraft_reconfigure.yml  deploy_dashboard_v2.yml  update_monitoring_v2.yml(*)  deploy_jmx_v2.yml(*)  inventory.ini (kopia)
  gcloud/  cluster_hosts_v2.env  stop_brokers.sh  start_brokers.sh  create_topics.sh  cleanup_topics_v2.sh
           deploy_function_v2.sh  collect_run_artifacts.sh  run_experiment.sh
  terraform/ main.tf  variables.tf  provider.tf  README.md
  RUNBOOK-v2.md
```
(*) treść reguł/dashboardów dostarcza W4; W1 tworzy playbooki kopiujące pliki z `thesis-v2/monitoring_node/`.

## 5. Kryteria akceptacji

- `bash -n` na wszystkich `.sh`; `ansible-playbook --syntax-check` na playbookach (lokalnie, bez połączenia).
- `kraft_reconfigure.yml` nigdy nie startuje kafki na węźle nieaktywnym w trybie 1; wymaga `confirm_wipe=yes`.
- `create_topics.sh 1b-rf1` używa wyłącznie bootstrapu 10.0.0.6:9092.
- Terraform v2: plan = 2 zasoby, brak odwołań do istniejącego stanu.
- `deploy_function_v2.sh` nie dotyka `hexweather-producer` ani connectora.
- Zapytania PromQL w R7 odwołują się wyłącznie do nazw metryk zdefiniowanych w `spec-monitoring.md` §3 (lista nazw jest tam autorytatywna).
- RUNBOOK-v2 czyta się od góry do dołu jako jedna sesja; każda komenda ma oczekiwany wynik.

## 6. Prompt dla workera (samowystarczalny)

> Jesteś inżynierem infrastruktury. Pracujesz **wyłącznie** w `C:\thesis\thesis-v2\` (twórz `ansible/`, `gcloud/`, `terraform/`, `RUNBOOK-v2.md`). Katalog `C:\thesis\` poza `thesis-v2/` jest tylko do czytania — kopiuj wzorce, nigdy nie edytuj. Nie masz dostępu do shella ani GCP: dostarczasz pliki; autor je uruchomi. Przeczytaj najpierw: `thesis-v2/experiment-plan.md` (§4, §5, §8), `thesis-v2/spec/spec-infra.md` (ten dokument, wymagania R1–R9), `thesis-v2/spec/spec-monitoring.md` §3 (nazwy metryk), `thesis-v2/spec/spec-dashboard.md` §4 (API dashboardu), oraz wzorce: `C:\thesis\ansible\install_kafka.yml`, `inventory.ini`, `update_monitoring.yml`, `deploy_dashboard.yml`, `C:\thesis\gcloud\cluster_hosts.env`, `collect_jmx_artifacts.sh`, `cleanup_topics.sh`, `deploy_function.sh`, `run_scenario.sh`, `C:\thesis\terraform\main.tf`. Zrealizuj R1–R9 dokładnie wg spec; nie podejmuj własnych decyzji projektowych — jeśli coś jest niejednoznaczne, zapisz pytanie w raporcie i przyjmij wariant najbliższy spec. Używaj neutralnej terminologii (bieg, konfiguracja, saturacja, kolektor). Raport końcowy: lista plików, jak każdy spełnia kryteria §5, otwarte pytania, oraz — jeśli potrzebujesz weryfikacji poleceniem — dokładne komendy do uruchomienia przez autora (`bash -n …`, `ansible-playbook --syntax-check …`, `terraform plan`).
