# Spec W3 — Dashboard v2: konsument, inspektor klastra, silnik Kingmana, kontroler biegów (thesis-v2)

**Właściciel:** Dashboard agent. **Plan:** `experiment-plan.md` §1, §3, §4, §5, §7.
**Kod bazowy (tylko do czytania, kopiować):** `C:\thesis\kafka-app\dashboard\` (`DashboardApp.java`, `consumer/MetricsConsumer.java`, `math/MathEngine.java`, `pom.xml`, `src/main/resources/public/*`).
**Host docelowy:** `kafka-load` (e2-standard-2, 8 GB, **OpenJDK 17** — kod musi być zgodny z Java 17), port 8080. Deploy robi W1 (`deploy_dashboard_v2.yml`); kontrakt funkcji: `spec-function.md` §2–§3.

## 1. Cel i zakres

Jedna aplikacja Java (Javalin) = **konsument** (opóźnienie e2e, lag, tempo), **inspektor klastra** (AdminClient: offsety, liderzy, repliki per partycja, 1 Hz), **odbiornik raportów ACK** (`control-metrics`, scalanie HdrHistogram), **silnik Kingmana** (λ, ρ, W_q, L_pred, ε; estymatory c²; profile μ/τ per konfiguracja), **kontroler biegów** (fan-out do Cloud Functions, manifest, serie CSV), **eksporter Prometheus** (`/metrics`), **frontend** (formularz sterowania, słupki skumulowane per broker × temat × rola, linia tempa leaderów w czasie per broker z delt LEO, panel liderów, wykres Kingmana 1 Hz z cieniem ±MAE(60 s) wzdłuż predykcji, karty błędu).
Poza zakresem: mapa hex i presety (usunięte), analiza offline (W5), Grafana (W4).

## 2. Konfiguracja (env)

`KAFKA_SEED` (jeden bootstrap, default `10.0.0.6:9092`; lista brokerów resolve'owana na starcie przez `describeCluster`, retry ~60 s) — jawne `KAFKA_BROKERS` wygrywa (compat), ale po reconfigure redeploy NIE jest potrzebny. `FUNCTION_URL`, `CONFIG` (`1b-rf1|3b-rf1|3b-rf3` — etykieta profilu, nie okablowanie), `DASHBOARD_PORT=8080`, `DB_PATH=/var/lib/thesis-dashboard/thesis.db` (pojedynczy plik SQLite, tryb WAL; katalog musi być zapisywalny dla usera serwisowego), `PROFILES_FILE=/opt/thesis-dashboard/profiles.json`, `PROM_URL=http://10.0.0.5:9090` (opcjonalny), `TOPICS=weather-rain,weather-temp,weather-wind` (default startowy; potem rządzi selektor UI `watched_topics`), `CONTROL_TOPIC=control-metrics`, `E2E_SAMPLE_EVERY=1`, `ADMIN_POLL_MS=1000`, `TOPIC_DESCRIBE_MS=5000`.

## 3. Komponenty i wymagania

### R1 `cluster/ClusterInspector` (AdminClient)
- Co `TOPIC_DESCRIBE_MS`: `describeTopics(TOPICS)` → dla każdej partycji: `leader`, `replicas`, `isr`; `describeCluster()` → lista brokerów (id, host).
- Co `ADMIN_POLL_MS`: `listOffsets(earliest)` i `listOffsets(latest)` dla wszystkich partycji → `msgs[p] = latest − earliest`.
- Snapshot `ClusterSnapshot { ts, brokers[], partitions[] {topic, partition, leader, replicas[], isr[], msgs, latest}, perBroker[] {brokerId, byTopic{topic → {leaderMsgs, followerMsgs, leaderPartitions, replicaPartitions}}}, totalMsgs, lambdaLeo }` gdzie `lambdaLeo = Δ(Σ latest po partycjach)/Δt` (EMA nie stosować; surowa różnica 1 s; przy Δt≠1 s normalizować).
- Odporność: brak brokera 2/3 w trybie 1b nie jest błędem (w snapshot są tylko brokery zwrócone przez `describeCluster`). Wyjątki logowane, poprzedni snapshot utrzymany, licznik `adminErrors`.

### R2 `consumer/E2EConsumer`
- `KafkaConsumer<byte[],byte[]>` (ByteArrayDeserializer — **bez** deserializacji String/JSON), group `thesis-dashboard-v2`, `auto.offset.reset=latest`, `max.poll.records=20000`, `fetch.min.bytes=1`, `fetch.max.wait.ms=100`, subskrypcja `TOPICS`.
- Per rekord (co `E2E_SAMPLE_EVERY`-ty dla opóźnienia; zliczanie zawsze): `lat = now − record.timestamp()`; ujemne NIE są połykane po cichu — licznik `negLat` w oknie (`E2ESnapshot.negLat`) + głośny log `e2e SUSPECT` gdy ujemnych > 50% (podejrzenie skew zegarów; histogram i tak dostaje max(lat,0)); do `Recorder` HdrHistogram okna (1 µs–60 s, 3 cyfry; wartości w µs), bajty = `serializedKeySize+serializedValueSize`, licznik per temat.
- **c_a² (indeks dyspersji):** bin 100 ms po `record.timestamp()`; okno kroczące 60 s (600 binów); `ca2 = var(N_bin)/mean(N_bin)` liczony co 1 s (jeśli mean < 5 → NaN).
- Co 1 s snapshot `E2ESnapshot { rate, bytesRate, mean, p50, p99, max, count, perTopicCount, lag, ca2 }`; `lag = Σ(endOffsets(assignment) − position)` (`consumer.endOffsets` raz na sekundę w wątku konsumenta).
- Wątek konsumenta nie wykonuje nic poza `poll`/zliczaniem; snapshot przekazywany przez `volatile` referencję.

### R3 `control/AckAggregator`
- Osobny `KafkaConsumer<String,String>` na `CONTROL_TOPIC`, group `thesis-dashboard-v2-control`, `latest`.
- Parsuje `AckReport` (kontrakt `spec-function.md` §3); przypisuje do kubełka sekundowego `floor(windowEndMs/1000)`; per kubełek: `Histogram merged` (dekoduj `hdrBase64` → `add`), Σ sent/acked/failed/bytes, mapa errors, zbiór invocationId. Kubełki starsze niż 10 s od „teraz” są zamykane (spóźnione raporty ignorowane, licznik `lateReports`). Utrzymuje też histogram całego aktywnego biegu (do τ/c_s²) — resetowany na start biegu.
- Snapshot 1 Hz: ostatni zamknięty kubełek → `AckSnapshot { windowEnd, invocations, sent, acked, failed, bytes, mean, p50, p95, p99, max, errors }`.

### R4 `math/KingmanEngine`
- `Profile { config, muMsgs, tauAckMs, tauE2eMs, ca2, cs2, source }` ładowany z `PROFILES_FILE` (JSON: mapa config → profile; przykład w README) dla `CONFIG`; brak pliku → profil `DEFAULT` (`mu=NaN` → ρ/W_q = NaN, UI pokazuje „brak profilu — wykonaj E1/E4”).
- Co 1 s: `lambda = lambdaLeo`; `rho = lambda/mu`; `rhoEff = min(rho, 0.999)`; `wq = rhoEff/(1−rhoEff) · (ca2+cs2)/2 · tauAckMs`… **uwaga:** τ we wzorze to średni czas obsługi — używamy `tauAckMs` jako τ obsługi w **obu** predykcjach (`predAck = tauAckMs + wq`, `predE2e = tauE2eMs + wq`); `errAck = obsAckMean − predAck`, `errE2e = obsE2eMean − predE2e`, `errRel = err/pred`; kroczące MAE/MAPE (60 s). `ca2`/`cs2` z profilu; równolegle pokazuj estymaty bieżące (`ca2Est` z R2, `cs2Est = (sd/mean)²` histogramu biegu z R3) — **nie** podmieniają profilu automatycznie. **Tryb live** (`POST /api/kingman/mode {"mode":"live"}`; domyślnie `"profile"`): W_q liczone z bieżących estymat ticka (gdy skończone, inaczej fallback na profil); τ i μ zawsze z profilu.
- `PUT /api/profile` (JSON profilu) nadpisuje profil w pamięci i w `PROFILES_FILE` oraz zapisuje go w bibliotece i aktywuje; `POST /api/profile/from-run/{runId}` i `from-runs` liczą wyłącznie ze ZAPISANEGO biegu (`tauAckMs` = `summary.ackP50`, `cs2` = odporne: mediana po kubełkach w oknie z `(p95-p50)/1.645` do kwadratu nad `ackMean` — pojedyncze wielosekundowe stalle rozruchowe nie trują estymatora, w przeciwieństwie do sd z histogramu całego biegu; `tauE2eMs` = mediana `e2e_p50_ms`, `ca2` = mediana `ca2_est` albo fallback Poissona `1.0`) i zapisują pod nazwą w bibliotece, BEZ aktywacji; `GET /api/profiles` (biblioteka + aktywny), `POST /api/profiles?name=X` (upsert, np. seed OMB przez curl), `DELETE /api/profiles/{name}`, `POST /api/profile/active {"name"}` (aktywacja; nazwa musi istnieć i zgadzać się CONFIG).
- Testy: wartości W_q dla (ρ=0.5, c²=1, 0.5, τ=6) = 4.5 ms; ρ≥1 → `rhoEff=0.999`, flaga `overload=true`; NaN-safe.

### R5 `run/RunController`
- `POST /api/run` body: `{ "exp":"E1|E4|E2|FREE", "label":"rho50", "parallelism":20, "ratePerSec":500, "durationSec":120, "count":null, "topics":["weather-rain","weather-temp","weather-wind"], "intensity":0.9, "runIndex":1 }`.
  Waliduj: `parallelism ∈ [1,500]`, `durationSec ∈ [10,540]` XOR `count`, `ratePerSec ≥ 0`, tylko jeden aktywny bieg. `runId = <exp>-<CONFIG>-<label>-run<runIndex>-<epochSec>`.
- Start: `startEpochMs=now`; reset akumulatorów R3; **fan-out**: `parallelism` asynchronicznych `POST FUNCTION_URL` (Java 11+ `HttpClient`, `sendAsync`, timeout `durationSec+180 s`), `invocationId=1..P`, `topic = topics[(i−1) mod topics.size()]`, `zoneId = "zone-%03d"`, body wg `spec-function.md` §2. Odpowiedzi (lub błędy HTTP) zapisywane w manifeście. `stopEpochMs = max(endEpochMs z odpowiedzi)` lub `now` gdy wszystkie zakończone/błędne.
- W trakcie biegu każdy tick 1 Hz (R7) jest dopisywany do pamięci biegu (od `start−5 s` do `stop+10 s`), a każdy zamknięty kubełek ACK do listy ACK biegu. **Utrwalanie wyłącznie w SQLite** (`store/RunStore`, jeden plik `DB_PATH`, WAL): tabela `series` (po jednej kolumnie na każde pole skalarne ticka), tabela `ack` (kubełki), tabela `runs` (manifest JSON z summary). Zero plików CSV/JSON na dysku dashboardu.
- Koniec: `analysisStartMs = start+15000`; `summary` liczone po obiektach ticków z pamięci (nie po parsowaniu tekstu): `lambdaMean/lambdaP50` (z `lambdaLeo` w oknie analizy), `ackMean/p50/p95/p99/max` (histogram biegu R3), `e2eMean` (średnia `e2e_mean_ms`), `e2eP50` (mediana `e2e_p50_ms`), `e2eP99` (średnia `e2e_p99_ms`), `rhoMean`, `predAck/predE2e/errAck/errE2e` (średnie), `consumerLagMax`, `consumerLimited = (udział sekund z lag>5·λ) > 10 %`, `sent/acked/failed` = **dokładne sumy z odpowiedzi funkcji** (`manifest.invocations[]`, nie agregaty serii), `avgRecordBytes`, `saturationSignature` (jak dotąd).
- Persist w SQLite pod `runId`: wiersz `runs` (manifest), wsad `series`, wsad `ack`. Endpoints: `GET /api/runs` (lista z `runs`), `GET /api/runs/{id}/manifest|summary` (z kolumny `manifest`), `GET /api/runs/{id}/series` (JSON array z tabeli `series`), `GET /api/runs/{id}/ack` (JSON array z tabeli `ack`), `GET /api/run/active`, `POST /api/run/{id}/abort` (oznacza `aborted`, NIE przerywa inwokacji funkcji — te dojeżdżają do `durationSec`; UI mówi to wprost po kliknięciu).

### R6 `export/PrometheusExporter` — `GET /metrics` (text/plain; version=0.0.4), format ręczny, metryki (gauge, bez etykiet o wysokiej kardynalności):
`thesis_kingman_lambda_msgs`, `thesis_kingman_mu_msgs`, `thesis_kingman_rho`, `thesis_kingman_wq_ms`, `thesis_kingman_pred_ack_ms`, `thesis_kingman_pred_e2e_ms`, `thesis_kingman_obs_ack_ms`, `thesis_kingman_obs_e2e_ms`, `thesis_kingman_err_ack_ms`, `thesis_kingman_err_e2e_ms`, `thesis_kingman_err_ack_rel`, `thesis_kingman_err_e2e_rel`, `thesis_kingman_ca2`, `thesis_kingman_cs2`, `thesis_kingman_ca2_est`, `thesis_kingman_cs2_est`, `thesis_ack_p50_ms`, `thesis_ack_p99_ms`, `thesis_ack_sent_total` (counter per bieg), `thesis_ack_failed_total`, `thesis_e2e_p50_ms`, `thesis_e2e_p99_ms`, `thesis_consumer_rate_msgs`, `thesis_consumer_lag_msgs`, `thesis_cluster_total_msgs`, `thesis_broker_msgs{broker,topic,role}` (role=`leader|follower`), `thesis_run_active{run_id,exp}` (1/0), `thesis_admin_errors_total`.

### R7 Pętla 1 Hz i WebSocket `/ws`
Jeden `ScheduledExecutorService` co 1 s: pobiera snapshoty R1–R3, liczy R4, buduje `Tick` (wszystkie pola z §5 series.csv + `cluster` snapshot + `run` status + `profile`), zapisuje do ring-buffera (2 h), do biegu (R5), do eksportera (R6), wysyła JSON do klientów WS. Frontend ma jedno źródło prawdy: ramka WS.

### R8 Frontend (osobny projekt `kafka-app/dashboard-front/`, vanilla JS + Chart.js z CDN jak dotąd; IIFE, guardy jak w oryginale)
Serwowanie: `/opt/thesis-frontend` na `kafka-load` (EXTERNAL, priorytet — deployuje kanał frontendu, hot-reload bez restartu, ze stemplem `version.txt` jako dowodem świeżości) z fallbackiem do classpath `/public` w jarze. Praca lokalna wg `kafka-app/dashboard-front/README.md` (Vite + tunel, PyCharm Deployment, Chrome Overrides). Zakładki: **Na żywo** (formularz, baner eksportu, karty, wykresy) i **Kalibracja** (przełącznik live/profil, 5 kart parametrów).
1. **Nagłówek:** CONFIG, bootstrap, profil (μ, τ_ack, τ_e2e, c_a², c_s², źródło), stan biegu.
2. **Formularz „Wysyłanie”** (zakładka Na żywo): wiersze 50/50 (input | opis), tryby duration/count z dynamicznym labelem i zakresami (duration 10–540, count ≥1, P 1–500, intensity 0–1); tematy (checkboxy 3), eksperyment (E1/E4/E2/FREE) z presetami przepisu; lista ostatnich biegów z linkami do `manifest/series/ack/summary` (JSON).
3. **Karty:** λ (LEO), λ (konsument), ρ, W_q, L_pred ACK / obs ACK / ε ACK (ms, %), L_pred e2e / obs e2e / ε e2e, MAE 60 s, lag, błędy producenta (typy), Σ wiadomości w klastrze.
4. **Wykres „Wiadomości w brokerach”** (Chart.js `bar`, `stacked`): oś X = brokery; datasety = temat × rola (lider = pełny kolor tematu, replika-follower = ten sam kolor z alfa 0.4 i obramowaniem); tooltip z liczbą partycji. Kolory tematów stałe: rain `#3b82f6`, temp `#f59e0b`, wind `#10b981`.
5. **Wykres „Liderzy”**: jak 4, ale tylko partycje, których broker jest liderem; pod nim tabela partycja → lider / repliki / ISR (18 wierszy).
6. **Wykres „Kingman na żywo”** (`line`, ostatnie 300 s): serie L_pred ACK, obs ACK mean, L_pred e2e, obs e2e mean (oś Y ms), ρ (oś Y2 0–1); pod nim wykres ε ACK / ε e2e (ms). Odświeżanie 1 Hz z ramki WS.
7. **Wykres „Tematy”**: stacked bar Σ wiadomości per temat (w klastrze) — prosty widok ogólny.
8. Brak błędów w konsoli przy pustym klastrze / braku profilu / braku biegu.
9. **Baner eksportu:** po przejściu biegu active→finished frontend pobiera manifest+summary najnowszego biegu i pokazuje baner „Bieg gotowy do pobrania" ze zdaniem `sent/acked/failed/status` oraz gotową komendą kolektora do wklejenia na stację (`COLLECT_VIA_SSH=1 bash thesis-v2/gcloud/collect_run_artifacts.sh thesis-v2/results/<EXP>/<CONFIG>/run<runIndex>/ <runId>`, dla E4/E2 z przypomnieniem o dopisaniu `P…/rho…`) + przyciskiem kopiowania.
10. **Zakładka Kalibracja:** przełącznik źródła c² (profil/live); 5 kart parametrów (opis, scrollowana lista checkboxów kwalifikujących się biegów o stałej wysokości, wynik: mediana ± SD, CoV, n); nazwa profilu do zapisu; zapis wymaga min. 3 biegów na każdy podany parametr (waliduje też serwer: 400) i trafia do biblioteki BEZ aktywacji (aktywacja na karcie Wykresów).
11. **Karta Wykresów:** selektor profilu z biblioteki (aktywny oznaczony; odświeżany co 15 s, więc wiersz `live` żyje); aktywacja przełącza silnik predykcji. Profil `live` sam się uzupełnia (rolling mediany 60 s; μ = obserwowane maksimum λ — estymata, nie saturacja).
11. **Tooltips z jednego źródła:** `glossary.js` (tytuł + opis per klucz) zasila atrybuty `data-tip` nagłówka, kart, wykresów i pól formularza oraz opisy kart kalibracji — tekst edytowany w jednym miejscu.

### R9 Pozostałe
- `GET /api/health` → `{"status":"ok","config":…,"brokers":n}`; `GET /api/tick` (ostatnia ramka JSON).
- Logowanie: INFO start/stop biegu, WARN błędy admin/konsument (z throttlingiem 1/10 s).
- `pom.xml`: Java 17, Javalin 6.x, kafka-clients 3.7.x, Jackson, HdrHistogram 2.2.2, JUnit 5; fat-jar `thesis-dashboard-2.0.0.jar` (shade/assembly jak oryginał).
- Testy: `KingmanEngineTest`, `AckAggregatorTest` (merge 3 zakodowanych histogramów → p50 zgodne), `ClusterSnapshotTest` (agregacja per broker/rola z syntetycznych opisów partycji), `RunStoreTest` (round-trip manifest/series/ack na tymczasowym pliku SQLite, NULL↔NaN).

## 4. API — podsumowanie (używane przez W1 `run_experiment.sh` i `collect_run_artifacts.sh`)

| Metoda | Ścieżka | Opis |
| --- | --- | --- |
| GET | `/api/health` | stan |
| GET | `/api/tick` | ostatnia ramka 1 Hz |
| POST | `/api/run` | start biegu (R5) → `{"runId":…}` |
| GET | `/api/run/active` | aktywny bieg + postęp |
| POST | `/api/run/{id}/abort` | oznacz przerwany (funkcje dojeżdżają do `durationSec`) |
| GET | `/api/runs` | lista biegów (z tabeli `runs`, najnowsze pierwsze) |
| GET | `/api/runs/{id}/manifest` | `run.json` (manifest z kolumny `runs.manifest`) |
| GET | `/api/runs/{id}/summary` | `summary` z manifestu (+ `status`) |
| GET | `/api/runs/{id}/series` | seria 1 Hz biegu (JSON array, klucze jak kolumny tabeli `series`) |
| GET | `/api/runs/{id}/ack` | scalone raporty ACK (JSON array, `errors` jako obiekt) |
| GET | `/api/profile/candidates` | zakładka Kalibracja: kwalifikujące się biegi per parametr (`tauAckMs/tauE2eMs/cs2/ca2` z E1 `completed`, `muMsgs` z E4 `completed`; tylko skończone wartości) |
| POST | `/api/profile/from-runs` | mediana ± SD/CoV z wybranych biegów (min. 3 na parametr, inaczej 400); `?dryRun=true` = podgląd bez zapisu; zapis do biblioteki pod nazwą, BEZ aktywacji |
| GET | `/api/profiles` | biblioteka + aktywny (selektor na karcie Wykresów) |
| POST | `/api/profiles?name=X` | upsert (seed OMB, import) |
| DELETE | `/api/profiles/{name}` | usuń z biblioteki (`live` odrasta sam) |
| POST | `/api/profile/active` | aktywuj `{"name"}` (musi istnieć, CONFIG musi grać) |
| GET/POST | `/api/kingman/mode` | źródło c²: `profile`/`live` |
| GET/PUT | `/api/profile` | profil Kingmana |
| POST | `/api/profile/from-run/{id}` | τ/c² z biegu E1 |
| GET | `/api/topics` | wszystkie tematy klastra (nazwa/partycje/RF/LEO/liderzy); on-demand, sekundy, nigdy na 1 Hz |
| GET/PUT | `/api/topics/watched` | selektor tematów UI (persist w SQLite; inspektor + konsument podążają na żywo) |
| GET | `/api/shape` | wykryty kształt `{brokers, topicsRf, suggestedConfig, profileConfig}` — detektor rozjazdu |
| GET | `/api/version` | detektor dryfu deploya `{backend, frontend, config}` ze stempli SHA (`unknown` = pół-deploy) |
| GET | `/api/logs` | konsola hosta: ogon journala usługi (`?n=10..300`, tylko odczyt, stała komenda) |
| GET | `/metrics` | Prometheus |
| WS | `/ws` | ramki 1 Hz |

## 5. Format danych (SQLite + JSON, żadnych CSV)

**Tabele** (plik `DB_PATH`, WAL): `runs(run_id PK, manifest TEXT)` — manifest to pełny JSON `RunManifest` z `summary`; `series(run_id, ts_ms, …)` — po jednej kolumnie na każde skalarne pole ticka 1 Hz (nazwy kolumn = dawne nagłówki CSV: `lambda_leo`, `lambda_consumer`, `rho`, `wq_ms`, `pred_ack_ms`, `pred_e2e_ms`, `ack_mean_ms`, `ack_p50_ms`, `ack_p95_ms`, `ack_p99_ms`, `ack_max_ms`, `ack_count`, `ack_invocations`, `e2e_mean_ms`, `e2e_p50_ms`, `e2e_p99_ms`, `e2e_max_ms`, `e2e_count`, `err_ack_ms`, `err_e2e_ms`, `err_ack_rel`, `err_e2e_rel`, `consumer_lag`, `sent`, `acked`, `failed`, `error_types`, `ca2_est`, `cs2_est`, `total_msgs_cluster`, `bytes_rate`; NaN = NULL); `ack(run_id, window_end_ms, invocations, sent, acked, failed, bytes, ack_mean_ms, ack_p50_ms, ack_p95_ms, ack_p99_ms, ack_max_ms, errors TEXT)` — `errors` jako JSON-obiekt `{Typ: liczba}`.

**Endpointy JSON:** `GET /api/runs/{id}/series` zwraca array obiektów (klucze = nazwy kolumn + `ts_ms`, `run_id`; NULL-e jako `null`); `GET /api/runs/{id}/ack` zwraca array kubełków (`errors` jako obiekt, nie string `Type:count`). Tick na `/ws` i `/api/tick` bez zmian (pełny obiekt z `cluster`/`run`/`profile`).

**`run.json` (kolumna `runs.manifest`):** pola R5 (camelCase); serie/kubełki **nie** inline (osobne tabele). Przykład kompletny w README.

**`profiles.json`:** `{ "1b-rf1": {"muMsgs": 180000, "tauAckMs": 7.2, "tauE2eMs": 21.5, "ca2": 1.05, "cs2": 0.62, "source": "E1/E4 pipeline 2026-09-10"}, "3b-rf1": {...}, "3b-rf3": {...} }`.

## 6. Układ plików

```
thesis-v2/kafka-app/dashboard/
  pom.xml  README.md  profiles.example.json
  src/main/java/com/thesis/dashboard/DashboardApp.java
  src/main/java/com/thesis/dashboard/cluster/{ClusterInspector,ClusterSnapshot}.java
  src/main/java/com/thesis/dashboard/consumer/{E2EConsumer,E2ESnapshot}.java
  src/main/java/com/thesis/dashboard/control/{AckAggregator,AckReport,AckSnapshot}.java
  src/main/java/com/thesis/dashboard/math/{KingmanEngine,Profile,KingmanResult}.java
  src/main/java/com/thesis/dashboard/run/{RunController,RunManifest,RunSummary,FunctionClient}.java
  src/main/java/com/thesis/dashboard/store/RunStore.java
  src/main/java/com/thesis/dashboard/export/PrometheusExporter.java
  src/main/java/com/thesis/dashboard/Tick.java
  src/test/java/... (R9)
thesis-v2/kafka-app/dashboard-front/   (projekt widoku — źródło prawdy UI)
  index.html  style.css  api-base.js  app.js  charts.js  control.js  calibration.js  glossary.js
  package.json  vite.config.js  README.md
  (deploy-frontend.yml: lint → stempel version.txt → sync do /opt/thesis-frontend/ → weryfikacja SHA;
  build-playbook publikuje ten katalog na dysk; jarowy fallback w resources/public/)
```

## 7. Kryteria akceptacji

- `mvn -q test` i `mvn -q -DskipTests package` przechodzą (autor uruchamia).
- Wszystkie endpointy §4 istnieją; serie/kubełki z SQLite (klucze JSON §5); ramka WS zawiera pełny tick z `cluster`.
- Konsument e2e nie deserializuje wartości rekordów; opóźnienie z `record.timestamp()`.
- λ pochodzi z ΔLEO (AdminClient), nie z konsumenta; lag i `consumerLimited` obliczane wg R5.
- Histogramy ACK scalane przez `Histogram.add` na zdekodowanych `hdrBase64` (nie średnie z percentyli).
- Profil (μ, τ, c²) nigdy nie zmienia się bez `PUT`/`from-run`; estymaty bieżące pokazywane obok.
- `sent/acked/failed` w summary = sumy z odpowiedzi funkcji; `e2e*` liczone z serii (mean/mediana) — nigdy `max()` snapshotów ani puste NaN przy żywych danych.
- UI: 4 wykresy (brokery, liderzy, Kingman+ε, tematy), formularz, karty; brak mapy hex; zero błędów konsoli bez danych.

## 8. Prompt dla workera (samowystarczalny)

> Jesteś programistą Java/JS. Utwórz projekt `C:\thesis\thesis-v2\kafka-app\dashboard\` dokładnie wg `C:\thesis\thesis-v2\spec\spec-dashboard.md` (ten dokument). Kod bazowy do skopiowania wzorców (IIFE, guardy, WS reconnect, ring-buffer, CSV): `C:\thesis\kafka-app\dashboard\` — **nie modyfikuj oryginału**. Kontrakt raportów ACK i odpowiedzi funkcji: `C:\thesis\thesis-v2\spec\spec-function.md` §2–§3 (wiążący). Cel: Java 17 (host ma OpenJDK 17), Javalin 6, kafka-clients 3.7, HdrHistogram 2.2.2. Nie masz shella; pisz kod kompilowalny i podaj komendy weryfikacyjne (`mvn -q test`). Nie podejmuj decyzji projektowych poza spec; niejasności zapisz jako pytania w raporcie. Używaj neutralnej terminologii (bieg, obciążenie, saturacja). Raport końcowy: lista plików, mapowanie na R1–R9, komendy weryfikacyjne, pytania.
