# Spec W4 — Monitoring: reguły JMX, job Prometheus, dashboardy Grafany (thesis-v2)

**Właściciel:** Monitoring agent. **Plan:** `experiment-plan.md` §5.6, §5.4, §7.2.
**Wzorce (tylko do czytania):** `C:\thesis\ansible\deploy_jmx.yml` (whitelista + reguły), `C:\thesis\ansible\update_monitoring.yml`, `C:\thesis\monitoring_node\grafana\{thesis-kafka-dashboard.json,dashboards-provider.yml,datasource-prometheus.yml,README.md}`.
**Zasada:** addytywnie. Istniejące reguły JMX, joby Prometheus i dashboardy pozostają **verbatim**; dopisujemy nowe. Pliki powstają w `thesis-v2/monitoring_node/` i `thesis-v2/ansible/`; W1 tylko je wywołuje z RUNBOOK-v2.

## 1. Cel

Umożliwić w Grafanie: (a) stan wiadomości w brokerach (całka) i tempo napływu (pochodna) per temat / broker / partycja / lider, w czasie; (b) panel Kingmana (pred/obs/ε) z eksportera dashboardu; (c) panel atrybucji saturacji (CPU, wątki obsługi, sieć, kolejka żądań, dysk, host konsumenta).

**Definicje (do manuskryptu):** *całka* strumienia wiadomości na zbiorze partycji = Σ(LogEndOffset − LogStartOffset) — liczba wiadomości aktualnie przechowywanych (stan kolejki); dla licznika `MessagesIn Count` całka = `increase()` w oknie. *Pochodna* = `rate()` licznika lub `deriv()` gauge'a LEO — wiadomości/s. *Lider* partycji = broker, na którym `kafka.cluster:type=Partition,name=ReplicasCount` > 0 (na followerach gauge = 0).

## 2. `ansible/deploy_jmx_v2.yml` (hosts: kafka_nodes)

Kopia `deploy_jmx.yml` z rozszerzoną konfiguracją `jmx_exporter.yaml`: zachowaj **wszystkie** istniejące wpisy `whitelistObjectNames` i `rules` byte-identycznie, dopisz:

whitelist:
```
- kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*
- kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*
- kafka.log:type=Log,name=LogEndOffset,topic=*,partition=*
- kafka.log:type=Log,name=LogStartOffset,topic=*,partition=*
- kafka.log:type=Log,name=Size,topic=*,partition=*
- kafka.cluster:type=Partition,name=ReplicasCount,topic=*,partition=*
- kafka.cluster:type=Partition,name=InSyncReplicasCount,topic=*,partition=*
- kafka.cluster:type=Partition,name=UnderReplicated,topic=*,partition=*
- kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent
- kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent
```
rules (przed istniejącą regułą `BrokerTopicMetrics` bez topic, żeby wzorzec z `topic=` dopasował się pierwszy):
```
- pattern: kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec, topic=(.+)><>Count
  name: kafka_server_brokertopicmetrics_messagesin_topic_total
  labels: {topic: "$1"}
  type: COUNTER
- pattern: kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec, topic=(.+)><>Count
  name: kafka_server_brokertopicmetrics_bytesin_topic_total
  labels: {topic: "$1"}
  type: COUNTER
- pattern: kafka.log<type=Log, name=(LogEndOffset|LogStartOffset|Size), topic=(.+), partition=(.+)><>Value
  name: kafka_log_$1
  labels: {topic: "$2", partition: "$3"}
  type: GAUGE
- pattern: kafka.cluster<type=Partition, name=(ReplicasCount|InSyncReplicasCount|UnderReplicated), topic=(.+), partition=(.+)><>Value
  name: kafka_cluster_partition_$1
  labels: {topic: "$2", partition: "$3"}
  type: GAUGE
- pattern: kafka.server<type=KafkaRequestHandlerPool, name=RequestHandlerAvgIdlePercent><>OneMinuteRate
  name: kafka_server_requesthandleravgidlepercent
  type: GAUGE
- pattern: kafka.network<type=SocketServer, name=NetworkProcessorAvgIdlePercent><>Value
  name: kafka_network_networkprocessoravgidlepercent
  type: GAUGE
```
Uwaga: `lowercaseOutputName: true` (jak w oryginale, sprawdź) → końcowe nazwy małymi literami: `kafka_log_logendoffset`, `kafka_log_logstartoffset`, `kafka_log_size`, `kafka_cluster_partition_replicascount`, `kafka_cluster_partition_insyncreplicascount`, `kafka_cluster_partition_underreplicated`. Restart kafki na wszystkich **osiągalnych** węzłach (`ignore_unreachable`).
Kardynalność: 3 tematy × 6 partycji × 3 metryki Log × 3 brokery ≈ 162 serie + Partition 3×18×3 — akceptowalna.

## 3. Autorytatywna lista nazw metryk (używana przez W1 R7, W3 R6, W5)

Istniejące (z oryginalnych reguł): `kafka_server_brokertopicmetrics_messagesin_total`, `kafka_server_brokertopicmetrics_bytesin_total`, `kafka_network_requestmetrics_totaltimems{request,quantile}`, `kafka_network_requestchannel_requestqueuesize`, `kafka_server_replicamanager_leadercount`, `kafka_server_replicamanager_partitioncount`, `kafka_server_replicamanager_underreplicatedpartitions`, `kafka_server_purgatorysize`, `node_cpu_seconds_total{node,mode}`, `node_network_receive_bytes_total{node,device}`, `node_network_transmit_bytes_total`, `node_disk_written_bytes_total`, `node_memory_MemAvailable_bytes`.
Nowe (§2): `kafka_server_brokertopicmetrics_messagesin_topic_total{topic}`, `kafka_server_brokertopicmetrics_bytesin_topic_total{topic}`, `kafka_log_logendoffset{topic,partition}`, `kafka_log_logstartoffset{topic,partition}`, `kafka_log_size{topic,partition}`, `kafka_cluster_partition_replicascount{topic,partition}`, `kafka_cluster_partition_insyncreplicascount{topic,partition}`, `kafka_cluster_partition_underreplicated{topic,partition}`, `kafka_server_requesthandleravgidlepercent`, `kafka_network_networkprocessoravgidlepercent`.
Etykiety z Prometheusa: `broker` (job kafka_jmx), `node` (node_exporter). Z dashboardu (job `dashboard_v2`): lista `thesis_*` z `spec-dashboard.md` R6.

## 4. `ansible/update_monitoring_v2.yml` (hosts: monitoring)

Kopia `update_monitoring.yml` z różnicami: (1) `prometheus.yml` = dwa istniejące joby **verbatim** + job:
```
- job_name: 'dashboard_v2'
  static_configs:
    - targets: ["10.0.0.2:8080"]
      labels: {node: kafka-load}
```
(2) kopiuje **dodatkowo** `thesis-v2/monitoring_node/grafana/thesis-v2-*.json` do `/var/lib/grafana/dashboards/` (istniejące JSON-y też kopiowane jak dotąd — nic nie znika); (3) preflight `stat` na nowych plikach; (4) restarty jak w oryginale.

## 5. Dashboardy Grafany (`thesis-v2/monitoring_node/grafana/`), datasource uid `thesis-prom`, refresh 5 s, zmienne: `$topic` (label_values(kafka_log_logendoffset, topic), multi, All), `$broker` (label_values(kafka_log_logendoffset, broker), multi, All)

### 5.1 `thesis-v2-cluster-live.json` — uid `thesis-v2-live`, tytuł „Thesis v2 — Klaster na żywo”
Rząd A „Stan kolejki (całka)”:
- A1 *Wiadomości w brokerach* (bar gauge / time series stacked): `sum by (broker) (kafka_log_logendoffset{topic=~"weather-.*"} - kafka_log_logstartoffset{topic=~"weather-.*"})`.
- A2 *Wiadomości w brokerach wg tematu* (time series stacked, legenda `{{broker}} / {{topic}}`): `sum by (broker, topic) (kafka_log_logendoffset{topic=~"$topic"} - kafka_log_logstartoffset{topic=~"$topic"})`.
- A3 *Wiadomości u liderów wg tematu*: `sum by (broker, topic) ((kafka_log_logendoffset{topic=~"$topic"} - kafka_log_logstartoffset{topic=~"$topic"}) and on (broker, topic, partition) (kafka_cluster_partition_replicascount > 0))`.
- A4 *Liderzy per broker* (stat): `kafka_server_replicamanager_leadercount`; *Partycje per broker*: `kafka_server_replicamanager_partitioncount`.
- A5 *Mapa liderów* (table, instant): `kafka_cluster_partition_replicascount{topic=~"weather-.*"} > 0` z transformacją do kolumn topic/partition/broker.
Rząd B „Tempo (pochodna)”:
- B1 *Napływ wg tematu (msg/s)*: `sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_topic_total{topic=~"$topic"}[30s]))`.
- B2 *Napływ wg brokera (msg/s)*: `sum by (broker) (rate(kafka_server_brokertopicmetrics_messagesin_total[30s]))`.
- B3 *Pochodna LEO per partycja (msg/s)* (legenda `{{topic}}-{{partition}}@{{broker}}`): `deriv(kafka_log_logendoffset{topic=~"$topic",broker=~"$broker"}[30s])` filtrowane liderami: `… and on (broker,topic,partition) (kafka_cluster_partition_replicascount > 0)`.
- B4 *Napływ u liderów wg brokera*: `sum by (broker) (deriv(kafka_log_logendoffset{topic=~"weather-.*"}[30s]) and on (broker,topic,partition) (kafka_cluster_partition_replicascount > 0))`.
Rząd C „Całka w oknie”:
- C1 *Wiadomości przyjęte w oknie wykresu (skumulowane)*: `sum by (topic) (increase(kafka_server_brokertopicmetrics_messagesin_topic_total{topic=~"$topic"}[$__range]))` (stat) oraz wersja time series z `sum by (topic) (kafka_log_logendoffset{topic=~"$topic"} … and liderzy)`.
Opisy paneli (description) zawierają definicje z §1 (do cytowania w manuskrypcie).

### 5.2 `thesis-v2-kingman.json` — uid `thesis-v2-kingman`, „Thesis v2 — Kingman”
- K1 *Przewidywane vs zmierzone (ACK)*: `thesis_kingman_pred_ack_ms`, `thesis_kingman_obs_ack_ms`, `thesis_ack_p50_ms`, `thesis_ack_p99_ms`.
- K2 *Przewidywane vs zmierzone (e2e)*: `thesis_kingman_pred_e2e_ms`, `thesis_kingman_obs_e2e_ms`, `thesis_e2e_p50_ms`, `thesis_e2e_p99_ms`.
- K3 *Błąd predykcji*: `thesis_kingman_err_ack_ms`, `thesis_kingman_err_e2e_ms`; K4 *Błąd względny*: `thesis_kingman_err_ack_rel`, `thesis_kingman_err_e2e_rel` (percentunit).
- K5 *ρ i λ/μ*: `thesis_kingman_rho`, `thesis_kingman_lambda_msgs`, `thesis_kingman_mu_msgs` (progi 0.5/0.8/1.0).
- K6 *Parametry*: stat `thesis_kingman_ca2`, `_cs2`, `_ca2_est`, `_cs2_est`, `thesis_kingman_wq_ms`.
- K7 *Konsument*: `thesis_consumer_lag_msgs`, `thesis_consumer_rate_msgs`, `thesis_ack_failed_total`.
- K8 *Aktywny bieg*: `thesis_run_active == 1` (state timeline, legenda `{{run_id}}`).

### 5.3 `thesis-v2-saturation.json` — uid `thesis-v2-sat`, „Thesis v2 — Atrybucja saturacji”
- S1 *CPU brokerów*: `1 - avg by (node)(rate(node_cpu_seconds_total{mode="idle",node=~"kafka-.*"}[1m]))` (próg 0.9).
- S2 *Request handler idle*: `kafka_server_requesthandleravgidlepercent` (próg 0.1); S3 *Network processor idle*: `kafka_network_networkprocessoravgidlepercent` (próg 0.1).
- S4 *Kolejka żądań*: `kafka_network_requestchannel_requestqueuesize`; S5 *Purgatory*: `kafka_server_purgatorysize`.
- S6 *NIC RX/TX brokerów (B/s)*: `sum by (node)(rate(node_network_receive_bytes_total{node=~"kafka-.*",device!~"lo"}[30s]))`, analogicznie transmit; linia odniesienia 1 GB/s (8 Gb/s egress e2-standard-4, opis: „udokumentowany limit egress; ingress cytowany w manuskrypcie”).
- S7 *Zapis dysku (B/s)*: `sum by (node)(rate(node_disk_written_bytes_total{node=~"kafka-.*"}[30s]))`, linia 48 MB/s (udokumentowany limit wolumenu 100 GB pd-ssd).
- S8 *Produce TotalTimeMs p99*: `kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"}`.
- S9 *Host konsumenta (kafka-load) CPU / pamięć*: `1 - avg(rate(node_cpu_seconds_total{mode="idle",node="kafka-load"}[1m]))`, `node_memory_MemAvailable_bytes{node="kafka-load"}`.
- S10 *λ z dashboardu*: `thesis_kingman_lambda_msgs` (do zestawienia plateau z sygnaturą).

## 6. `thesis-v2/monitoring_node/grafana/README.md`
Kontrakt metryk (§3), definicje całki/pochodnej/lidera (§1), procedura weryfikacji (curl :7071 na brokerze → grep `kafka_log_logendoffset`; `/api/v1/targets` → `dashboard_v2` UP; dashboardy widoczne), procedura eksportu CSV z panelu (Inspect → Data) jako fallback.

## 7. Kryteria akceptacji
- `deploy_jmx_v2.yml` zawiera oryginalne reguły verbatim + nowe; YAML poprawny; nazwy metryk zgodne z §3.
- `update_monitoring_v2.yml` nie usuwa żadnego istniejącego joba/dashboardu.
- Trzy JSON-y dashboardów parsują się (poprawny JSON, `uid` unikalne, `schemaVersion` ≥ 36), każde PromQL używa wyłącznie nazw z §3 lub `thesis_*` z `spec-dashboard.md` R6.
- Opisy paneli zawierają definicje §1.

## 8. Prompt dla workera (samowystarczalny)
> Jesteś inżynierem obserwowalności (Prometheus/Grafana/JMX exporter). Pracujesz **wyłącznie** w `C:\thesis\thesis-v2\monitoring_node\` i `C:\thesis\thesis-v2\ansible\` (pliki `deploy_jmx_v2.yml`, `update_monitoring_v2.yml`). Wzorce tylko do czytania: `C:\thesis\ansible\deploy_jmx.yml`, `C:\thesis\ansible\update_monitoring.yml`, `C:\thesis\monitoring_node\grafana\*`. Zrealizuj `C:\thesis\thesis-v2\spec\spec-monitoring.md` §2–§6 dokładnie; nazwy metryk eksportera dashboardu znajdziesz w `C:\thesis\thesis-v2\spec\spec-dashboard.md` R6. Nie masz shella ani dostępu do GCP — dostarczasz pliki i komendy weryfikacyjne dla autora (`ansible-playbook --syntax-check`, `python -m json.tool` na JSON-ach). Nie podejmuj decyzji poza spec; pytania zapisz w raporcie. Raport końcowy: lista plików, mapowanie na §2–§6, lista wszystkich użytych nazw metryk, komendy weryfikacyjne.
