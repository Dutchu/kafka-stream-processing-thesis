# Thesis v2 — Grafana dashboards (W4 Monitoring workstream)

Właściciel: W4 Monitoring (`thesis-v2/spec/spec-monitoring.md`). Ten katalog
jest **addytywny** względem `C:\thesis\monitoring_node\grafana\` (W5,
pierwotna praca): nic z oryginału nie jest usuwane ani modyfikowane — trzy
nowe pliki JSON i ten README opisują wyłącznie nową część stosu.

Deploy: `thesis-v2/ansible/deploy_jmx_v2.yml` (reguły JMX na brokerach) →
`thesis-v2/ansible/update_monitoring_v2.yml` (job Prometheus + prowizja
Grafany na węźle monitoring). Oba playbooki są kopiami oryginałów z
`C:\thesis\ansible\` rozszerzonymi addytywnie — zobacz nagłówki komentarzy w
tych plikach.

## 1. Pliki w tym katalogu

| Plik | Wdrażany do | Cel |
| --- | --- | --- |
| `thesis-v2-cluster-live.json` | `/var/lib/grafana/dashboards/` | uid `thesis-v2-live` — stan kolejki (całka) i tempo napływu (pochodna) per temat/broker/partycja/lider |
| `thesis-v2-kingman.json` | `/var/lib/grafana/dashboards/` | uid `thesis-v2-kingman` — predykcja Kingmana (pred/obs/ε) z eksportera dashboardu v2 |
| `thesis-v2-saturation.json` | `/var/lib/grafana/dashboards/` | uid `thesis-v2-sat` — atrybucja saturacji (CPU, wątki obsługi, sieć, kolejka żądań, dysk, host konsumenta) |

Pliki wzorcowe pierwotnej pracy (tylko do czytania — nie modyfikowane tu):
`C:\thesis\monitoring_node\grafana\{thesis-kafka-dashboard.json,
dashboards-provider.yml, datasource-prometheus.yml, README.md}`. Provisioning
(`datasources-prometheus.yml`, `dashboards-provider.yml`) jest re-używany
verbatim z oryginalnej lokalizacji przez `update_monitoring_v2.yml` — nie ma
tu jego kopii, bo datasource uid `thesis-prom` i file-provider watchujący
`/var/lib/grafana/dashboards` już obejmują nowe pliki bez zmian.

Wszystkie trzy dashboardy: datasource uid `thesis-prom`, `refresh: "5s"`,
`schemaVersion: 39`, zmienne szablonu `$topic` i `$broker` (multi-select,
opcja "All"), źródło zmiennych: `label_values(kafka_log_logendoffset, topic)`
/ `label_values(kafka_log_logendoffset, broker)`.

## 2. Definicje (do cytowania w manuskrypcie, spec-monitoring.md §1)

- **Całka** strumienia wiadomości na zbiorze partycji =
  Σ(LogEndOffset − LogStartOffset) — liczba wiadomości aktualnie
  przechowywanych (stan kolejki); dla licznika `MessagesInPerSec Count`
  całka w oknie = `increase()`.
- **Pochodna** = `rate()` licznika lub `deriv()` gauge'a LEO —
  wiadomości/s.
- **Lider** partycji = broker, na którym
  `kafka.cluster:type=Partition,name=ReplicasCount` > 0 (na followerach
  gauge = 0).

Każdy panel na wszystkich trzech dashboardach ma pole `description` z
odpowiednią definicją zastosowaną w danym PromQL.

## 3. Kontrakt nazw metryk (spec-monitoring.md §3)

### Istniejące (z oryginalnych reguł `deploy_jmx.yml`, verbatim)
`kafka_server_brokertopicmetrics_messagesin_total`,
`kafka_server_brokertopicmetrics_bytesin_total`,
`kafka_network_requestmetrics_totaltimems{request,quantile}`,
`kafka_network_requestchannel_requestqueuesize`,
`kafka_server_replicamanager_leadercount`,
`kafka_server_replicamanager_partitioncount`,
`kafka_server_replicamanager_underreplicatedpartitions`,
`kafka_server_purgatorysize`,
`node_cpu_seconds_total{node,mode}`,
`node_network_receive_bytes_total{node,device}`,
`node_network_transmit_bytes_total`,
`node_disk_written_bytes_total`,
`node_memory_MemAvailable_bytes`.

### Nowe (dodane w `deploy_jmx_v2.yml`, spec-monitoring.md §2)
`kafka_server_brokertopicmetrics_messagesin_topic_total{topic}`,
`kafka_server_brokertopicmetrics_bytesin_topic_total{topic}`,
`kafka_log_logendoffset{topic,partition}`,
`kafka_log_logstartoffset{topic,partition}`,
`kafka_log_size{topic,partition}`,
`kafka_cluster_partition_replicascount{topic,partition}`,
`kafka_cluster_partition_insyncreplicascount{topic,partition}`,
`kafka_cluster_partition_underreplicated{topic,partition}`,
`kafka_server_requesthandleravgidlepercent`,
`kafka_network_networkprocessoravgidlepercent`.

Wszystkie nazwy w małych literach (`lowercaseOutputName: true` /
`lowercaseOutputLabelNames: true` — ustawienie potwierdzone w oryginale i
zachowane w `deploy_jmx_v2.yml`).

### Etykiety z Prometheusa
`broker` (job `kafka_jmx`), `node` (job `node_exporter`). Z dashboardu (job
`dashboard_v2`, target `10.0.0.2:8080`): patrz sekcja 4 poniżej.

## 4. Metryki `thesis_*` (eksporter dashboardu v2, `spec-dashboard.md` R6)

`thesis_kingman_lambda_msgs`, `thesis_kingman_mu_msgs`, `thesis_kingman_rho`,
`thesis_kingman_wq_ms`, `thesis_kingman_pred_ack_ms`,
`thesis_kingman_pred_e2e_ms`, `thesis_kingman_obs_ack_ms`,
`thesis_kingman_obs_e2e_ms`, `thesis_kingman_err_ack_ms`,
`thesis_kingman_err_e2e_ms`, `thesis_kingman_err_ack_rel`,
`thesis_kingman_err_e2e_rel`, `thesis_kingman_ca2`, `thesis_kingman_cs2`,
`thesis_kingman_ca2_est`, `thesis_kingman_cs2_est`, `thesis_ack_p50_ms`,
`thesis_ack_p99_ms`, `thesis_ack_sent_total`, `thesis_ack_failed_total`,
`thesis_e2e_p50_ms`, `thesis_e2e_p99_ms`, `thesis_consumer_rate_msgs`,
`thesis_consumer_lag_msgs`, `thesis_cluster_total_msgs`,
`thesis_broker_msgs{broker,topic,role}` (role=`leader|follower`),
`thesis_run_active{run_id,exp}`, `thesis_admin_errors_total`.

Job `dashboard_v2` scrapuje `10.0.0.2:8080/metrics` (etykieta
`node: kafka-load`), dopisany w `update_monitoring_v2.yml` obok dwóch
istniejących jobów Prometheusa (`node_exporter`, `kafka_jmx` — verbatim).

## 5. Progi saturacji użyte na dashboardzie (`experiment-plan.md` §5.4)

Plateau w E4 jest limitem klastra tylko gdy zapala się ≥ 1 z:

| # | Warunek | Panel | Próg na wykresie |
| --- | --- | --- | --- |
| (i) | CPU idle brokera < 10% (busy > 0,9) **lub** `RequestHandlerAvgIdlePercent` < 0,1 **lub** `NetworkProcessorAvgIdlePercent` < 0,1 | *CPU brokerów*, *Request handler idle*, *Network processor idle* | 0,9 / 0,1 / 0,1 |
| (ii) | `RequestQueueSize` > 0 przez > 50% okna | *Kolejka żądań* | (brak linii — sprawdzane czasowo) |
| (iii) | NIC RX brokera ≥ 80% udokumentowanego limitu (e2-standard-4: 8 Gb/s egress) | *NIC RX/TX brokerów* | 1 GB/s (odniesienie egress) |
| (iv) | Zapis dysku ≥ 80% limitu wolumenu (48 MB/s dla 100 GB pd-ssd) | *Zapis dysku* | 48 MB/s |

Po stronie klienta (nie na dashboardzie Grafany, weryfikowane z logów
funkcji): brak `BufferExhausted`/`TimeoutException` przed plateau, wzrost
p50 ACK między kolejnymi `P` monotoniczny — panel *Produce TotalTimeMs p99*
wspiera tę obserwację.

## 6. Procedura weryfikacji

1. Na brokerze (po `deploy_jmx_v2.yml`):
   ```
   curl -s localhost:7071/metrics | grep kafka_log_logendoffset
   ```
   Powinno zwrócić niepuste serie z etykietami `topic`/`partition`.
2. Na węźle monitoring (po `update_monitoring_v2.yml`):
   `http://<kafka-monitoring-ip>:9090/targets` → job `dashboard_v2` **UP**
   (target `10.0.0.2:8080`), obok `node_exporter` i `kafka_jmx` nadal UP.
3. Grafana (`http://<kafka-monitoring-ip>:3000`) → trzy nowe dashboardy
   widoczne: **Thesis v2 — Klaster na żywo** (`thesis-v2-live`), **Thesis v2
   — Kingman** (`thesis-v2-kingman`), **Thesis v2 — Atrybucja saturacji**
   (`thesis-v2-sat`) — bez błędów „datasource not found”.
4. Zmienne `$topic`/`$broker` wypełniają się (dropdown pokazuje
   `weather-rain`, `weather-temp`, `weather-wind` / `kafka-1`, `kafka-2`,
   `kafka-3` gdy dany broker jest osiągalny).

## 7. Eksport CSV z panelu (fallback, jak w oryginale)

Hover na tytuł panelu → menu (⋮ / kebab) → **Inspect → Data → Download
CSV**. Używane jako uzupełnienie/fallback dla `prom/*.json` z protokołu
pomiarowego (`experiment-plan.md` §5 pkt 1, 6) — zapytania zakresowe
Prometheusa (`/api/v1/query_range`) pozostają głównym źródłem danych do
pipeline'u analizy; eksport CSV z Grafany jest ręcznym fallbackiem, gdy
kolektor automatyczny jest niedostępny.

## 8. Uwagi / poza zakresem

- Kardynalność nowych reguł JMX: 3 tematy × 6 partycji × 3 metryki `Log` ×
  3 brokery ≈ 162 serie + `Partition` 3 metryki × 18 (temat×partycja) × 3
  brokery — akceptowalna (spec-monitoring.md §2).
- Restart Kafki w `deploy_jmx_v2.yml` używa `ignore_unreachable: yes` —
  w Fazie A (`1b-rf1`) kafka-2/kafka-3 są zatrzymane celowo i nie są błędem.
- Ten katalog nie zawiera osobnych plików `datasource-prometheus.yml` /
  `dashboards-provider.yml` — są re-używane verbatim z lokalizacji
  pierwotnej pracy przez `update_monitoring_v2.yml`.
