# Plan eksperymentu — thesis-v2: system kolejkowy Apache Kafka z producentami bezserwerowymi i monitorowaniem opóźnień według wzoru Kingmana

**Status:** v1.0 — plan nadrzędny (autorytatywny). Raporty workerów są wpisywane z powrotem do tego pliku (§10).
**Relacja do pracy pierwotnej (`C:\thesis\`):** ten katalog (`thesis-v2/`) jest w pełni samodzielny. Materiał pierwotny jest **tylko do czytania** (kod można kopiować, nie modyfikować). Wspólna jest wyłącznie fizyczna infrastruktura GCP (VM, VPC, Prometheus/Grafana), na której operujemy addytywnie i odwracalnie.

---

## 1. Tytuł i cel

**Tytuł roboczy (PL):** *Projekt i realizacja skalowalnego systemu kolejkowego Apache Kafka z producentami bezserwerowymi w Google Cloud oraz monitorowaniem opóźnień na podstawie wzoru Kingmana*
(do potwierdzenia przez autora; wersja EN do streszczenia: *Design and implementation of a horizontally scalable Apache Kafka queueing system with serverless producers on Google Cloud and Kingman-formula latency monitoring*).

**Cel pracy (inżynierski, nie tezowy):** zaprojektować, zrealizować i uruchomić kompletny system: klaster Apache Kafka (KRaft) na GCP → producenci = Cloud Functions (Java) → konsument = dashboard (Java/Javalin) mierzący opóźnienie e2e i ACK, liczący na żywo wzór Kingmana (wartości teoretyczne vs zmierzone vs błąd, 1 Hz), sterujący wysyłaniem wiadomości i pokazujący rozkład wiadomości po brokerach/tematach/partycjach/liderach; obserwowalność w Prometheus/Grafana (przebiegi w czasie, całka i pochodna strumienia wiadomości). System jest następnie użyty do zbadania trzech konfiguracji klastra — **1 broker, 3 brokery bez replikacji, 3 brokery z replikacją** — pod kątem skalowalności horyzontalnej, kosztu replikacji i trafności predykcji Kingmana.

**Jedyny wzór w pracy (Kingman, VUT):**
`E[W_q] ≈ ρ/(1−ρ) · (c_a² + c_s²)/2 · τ`, `ρ = λ/μ`, przewidywane opóźnienie `L_pred = τ + E[W_q]`, błąd `ε = L_obs − L_pred` (ms) i `ε_rel = ε / L_pred` (%).
Parametry: `λ` — zmierzony strumień wejściowy (msg/s); `μ` — zmierzona pojemność konfiguracji (msg/s, E4); `τ` — opóźnienie bazowe przy niskim obciążeniu (E1; osobno τ_ack i τ_e2e); `c_a²` — zmienność napływu (indeks dyspersji zliczeń w oknach 100 ms); `c_s²` — zmienność obsługi (CoV² opóźnienia ACK przy kalibracji). Wszystko mierzone, nic nie zakładane z dokumentacji.

## 2. Pytania badawcze (zamiast tez)

- **P1 (skalowalność horyzontalna):** o ile rośnie pojemność μ (msg/s) po przejściu z 1 brokera na 3 brokery bez replikacji (RF=1)? Idealnie 3×; raportujemy przyspieszenie S = μ_3B-RF1/μ_1B i efektywność S/3.
- **P2 (koszt replikacji):** ile pojemności kosztuje replikacja RF=3, `acks=all`, `min.insync.replicas=2` w porównaniu z 3B/RF1 (μ_3B-RF3/μ_3B-RF1)?
- **P3 (trafność Kingmana):** jak dobrze `L_pred(ρ)` przewiduje zmierzone opóźnienie ACK i e2e na drabince ρ ≈ 0,25/0,5/0,75/0,9 w każdej konfiguracji (błąd bezwzględny i względny, MAE/MAPE)?
- **P4 (prawdziwe przeciążenie):** czy producenci bezserwerowi potrafią doprowadzić klaster do rzeczywistej saturacji — plateau λ **z jednoczesnym** wzrostem p50 (nie tylko p99), rosnącym backlogiem i sygnaturą limitu po stronie brokera (CPU / wątki obsługi żądań / sieć), a nie po stronie dysku ani ścieżki klienta?

Wynik negatywny (np. Kingman chybia przy ρ≈0,9 lub przyspieszenie ≪ 3) jest raportowany jako wniosek inżynierski, nie ukrywany.

## 3. Zmienne

| Rodzaj | Zmienne |
| --- | --- |
| **Niezależne** | konfiguracja klastra `CONFIG ∈ {1b-rf1, 3b-rf1, 3b-rf3}`; docelowe tempo λ (msg/s, sterowane z dashboardu: `ratePerSec × parallelism`); liczba równoległych instancji funkcji `P` (E4); |
| **Zależne** | λ osiągnięte (msg/s, z ΔLogEndOffset); opóźnienie ACK (mean/p50/p95/p99/max, HdrHistogram z funkcji); opóźnienie e2e (mean/p50/p99, konsument); `E[W_q]`, `L_pred`, ε, ε_rel; błędy producenta wg typu; lag konsumenta; sygnatury brokera (CPU idle, `RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent`, `RequestQueueSize`, bajty NIC, zapis dysku) |
| **Kontrolowane** | VM brokerów kafka-1/2/3 = e2-small, pd-ssd 100 GB, europe-central2-a (obniżone z e2-standard-4 decyzją z 2026-09-10: za duży broker jest nie do przeciążenia jednym generatorem — v. raport omb-kampania; heap 512M przez drop-in); Kafka 3.7.0 KRaft; 3 tematy `weather-rain`, `weather-temp`, `weather-wind` × 6 partycji; ładunek = JSON `WeatherEvent` (~130–160 B, rozmiar zmierzony i raportowany); `acks=all` we wszystkich konfiguracjach (`min.insync.replicas=1` dla RF1, `=2` dla RF3); `linger.ms=5`, `batch.size=16384`, `enable.idempotence` domyślne; funkcja: 1 GiB / 1 vCPU / concurrency=1 / Direct VPC egress; konsument na `kafka-load` (e2-standard-2, 8 GB); Prometheus 5 s na `kafka-monitoring`; okno pomiaru bez pierwszych 15 s rozgrzewki |

## 4. Warunki eksperymentalne i powtórzenia

**Powtórzenia: 3 biegi na scenariusz.** Dla kluczowej metryki raportujemy medianę, średnią ± SD, CoV i zakres (n=3). Statystyczna waga pochodzi z liczby wiadomości w biegu (≥10⁵), powtórzenia potwierdzają powtarzalność.

Kolejność faz (wymuszona przez kworum KRaft — §8): **Faza A = 1b-rf1** (kafka-1 jako 1-węzłowe kworum, kafka-2/3 zatrzymane) → **Faza B = 3b-rf1 i 3b-rf3** (3-węzłowe kworum, RF ustawiane per temat). Wewnątrz każdej konfiguracji kolejność: E1 → E4 → E2 (E2 potrzebuje μ z E4 do wyznaczenia szczebli).

**Kolejność kampanii (decyzja autora 2026-09-09): najpierw benchmark OMB, potem funkcje.** Producenci serverless nie dowodzą niczego w próżni — ich wyniki mają się oprzeć o niezależny punkt odniesienia. Dlatego każda konfiguracja zaczyna od kampanii OMB (tabela niżej, wiersz E0), a dopiero jej μ staje się celem do pobicia/potwierdzenia przez E4 na funkcjach. Porównanie OMB-vs-funkcje (ten sam klaster, inny sterownik obciążenia) jest osobnym wynikiem pracy (P4 + akapit 5.1).

| ID | Scenariusz | Sterowanie (dashboard → funkcje) | Czas biegu | Wyjście |
| --- | --- | --- | --- | --- |
| **E0/OMB** | Benchmark odniesienia — TYLKO 1b-rf1 (rewizja 2026-09-10: trzech brokerów jednym producentem się nie przeciąży, więc benchmarków 3b nie ma; skalowalność 1b-vs-3b pokazują funkcje E4, nie OMB) | `perf-test` z `kafka-load`: temat `heavy-topic` (30 partycji, RF=1), drabinka throttlingu `T10→T25→T50→T100→T200→Tinf` (`--throughput N`, `-1` = bez limitu), 3M rekordów/szczebel, 1 KB, `acks=all`; `HW=esmall bash thesis-v2/gcloud/run_omb_rung.sh 1b-rf1 <rung> 1 <thr> all` (pojedynczo) | ~6 × 1–4 min | `thesis-v2/results/OMB-reference/1b-rf1-<hw>/T<rung>/run1/{perf.txt, run_window.env, prom/}` → `omb_import.py`: `tab-omb-profile.csv` (μ, τ_ack **z błędami**: SD/CoV/n, metoda) + wpis `OMB` w `profiles.json` + `fig-omb-ladder` |
| **E1** | Kalibracja τ i c²: niskie obciążenie | `P=1`, `ratePerSec=20`, 3 tematy round-robin, 90 s | 90 s | τ_ack (mediana ACK), τ_e2e (mediana e2e), c_s² (CoV² ACK), c_a² (indeks dyspersji) → `run.json`, `series.json`, `ack.json` |
| **E4** | Saturacja: wyznaczenie μ | `ratePerSec=0` (bez limitu), drabinka równoległości `P ∈ {10, 25, 50, 100, 200, 300}`, każdy `P` 120 s; przerwanie drabinki gdy λ przestaje rosnąć przez 2 kolejne P **i** sygnatura brokera zapaliła się (§5.4) | ≤ 6 × 120 s | μ = mediana λ na plateau (msg/s, także MB/s); p50/p99 ACK na plateau; sygnatura wiążąca; lag; błędy producenta |
| **E2** | Drabinka ρ (Kingman) | `P` i `ratePerSec` dobrane tak, by λ ≈ 0,25/0,5/0,75/0,9 · μ (P = 20, rate = λ/P), 120 s na szczebel | 4 × 120 s | punkty (ρ_real, L_ack, L_e2e) vs krzywa L_pred(ρ); ε, ε_rel |

Liczba biegów: 3 konfiguracje × (E1 3 + E4 ≤6·3 + E2 4·3) ≈ 3 × 33 = ~100 krótkich biegów (1,5–2 min) ≈ 4–5 h czasu klastra w 2–3 sesjach. Wszystkie biegi wywołuje jeden skrypt (`gcloud/run_experiment.sh`) przez API dashboardu, więc sesja jest jedną komendą na konfigurację.

**Wymiar satelitarny `acks` (decyzja autora):** główna kampania (OMB + funkcje) jedzie na `acks=all` (ścieża durable = trzon tezy). Satelita `acks=1`: jeden `Tinf` OMB + jedno plateau E4 na konfigurację → twarda liczba kosztu replikacji (μ₁ − μ_all) do akapitu P1/P2. Przełączenie funkcji: `KAFKA_ACKS=1 bash thesis-v2/gcloud/deploy_function_v2.sh <CONFIG>` (env, default `all`; kod producenta bez zmian). Inny `acks` = inny τ i cs² → osobny wiersz profilu w bibliotece, nie mieszamy.

**Eskalacja w E4 (decyzja autora):** jeżeli λ osiąga plateau **bez** sygnatury brokera (CPU idle > 20 %, handler idle > 20 %, kolejka żądań ≈ 0, NIC ≪ limit), limitem jest ścieżka klienta i **nie** wolno ogłosić μ. Wtedy kolejno: (1) podnieść `--max-instances` do 500 i `P` do 500; (2) podnieść funkcję do 2 vCPU / 2 GiB; (3) zweryfikować Direct VPC egress (brak connectora w ścieżce) i limity podsieci; (4) zwiększyć `batch.size`/`linger.ms` w funkcji (mniej żądań na wiadomość). Każdy krok jest odnotowany w §10 jako incydent i trafia do rozdziału o przebiegu prac.

**Odniesienie do wcześniejszych benchmarków OMB:** wyniki E2/E4 z pracy pierwotnej (`../results/`, tylko do czytania) nie są powtarzane; manuskrypt zawiera jeden akapit: autor wykonał benchmarki OMB/perf-test z jednej VM, nie uzyskał przekonującego dowodu saturacji (szumiące p99 przy stabilnym p50, brak sygnatury brokera) i dlatego zaprojektował przeciążenie wieloma równoległymi producentami bezserwerowymi.

## 5. Protokół pomiarowy

1. **Układ wyników (obowiązkowy):** `thesis-v2/results/<EXP>/<CONFIG>/run<N>/` z plikami: `run.json` (manifest z dashboardu: runId, parametry, epoki start/stop, odpowiedzi funkcji), `series.json` (seria 1 Hz prosto z SQLite dashboardu), `ack.json` (scalone raporty ACK per sekunda, `errors` jako obiekt), `prom/*.json` (zapytania zakresowe Prometheus), `config.env` (CONFIG, wersje, IP). `<EXP> ∈ {E1, E4, E2}`, `<CONFIG> ∈ {1b-rf1, 3b-rf1, 3b-rf3}`; w E4 podkatalog `P<parallelism>` przed `run<N>`, w E2 `rho<25|50|75|90>`.
2. **Źródło λ:** ΔΣLogEndOffset/Δt ze wszystkich partycji tematów `weather-*` (AdminClient `listOffsets`, 1 Hz) — niezależne od konsumenta. Tempo konsumenta i lag raportowane obok; bieg, w którym lag konsumenta > 5 s przez > 10 % okna, ma flagę `consumer_limited=true` (e2e niewiarygodne, ACK wiarygodne).
3. **Opóźnienie ACK:** w funkcji, per rekord `send()` → callback, HdrHistogram (1 µs–60 s, 3 cyfry), raport co 1 s do tematu `control-metrics` (klucz = runId) z histogramem w Base64; dashboard scala histogramy per sekunda (dokładne percentyle, nie średnie z percentyli).
4. **Opóźnienie e2e:** konsument liczy `now − record.timestamp()` (CreateTime ustawiany przez producenta), bez parsowania JSON; zegary Google NTP — różnica zegarów sprawdzana `check_clock_sync` przed sesją i raportowana.
5. **Okno analizy:** od `start+15 s` do `stop` (rozgrzewka JIT/połączeń wykluczona).
6. **Prometheus (każdy bieg):** kolektor odpytuje `/api/v1/query_range` (krok 5 s, okno start−30 → stop+60) dla stałego zbioru zapytań: napływ msg/s per broker/temat (`rate(kafka_server_brokertopicmetrics_messagesin_total…)`), stan kolejki (`kafka_log_logendoffset − kafka_log_logstartoffset` per broker/temat/partycja), liderzy (`kafka_cluster_partition_replicascount > 0`), `RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent`, `RequestQueueSize`, CPU brokerów, bajty NIC RX/TX, zapis dysku, CPU/pamięć `kafka-load` (konsument), metryki dashboardu (`thesis_kingman_*`). Zawsze wszystkie, każdy bieg.
7. **Wszystkie tabele i rysunki generuje jeden pipeline Python** (`thesis-v2/analysis/`) do `paper/tables/*.csv` i `paper/figures/*`; manuskrypt wczytuje CSV przez `csvsimple`. Żadnych ręcznych tabel.

### 5.4 Sygnatura prawdziwej saturacji (E4) — plateau jest limitem klastra tylko gdy zapala się ≥ 1 z:
(i) CPU idle brokera (średnia po rdzeniach) < 10 % **lub** `RequestHandlerAvgIdlePercent` < 0,1 **lub** `NetworkProcessorAvgIdlePercent` < 0,1; (ii) `RequestQueueSize` > 0 utrzymujące się > 50 % okna; (iii) NIC RX brokera ≥ 80 % udokumentowanego limitu (e2-standard-4: 8 Gb/s egress; ingress raportowany, limit z dokumentacji cytowany); (iv) zapis dysku ≥ 80 % udokumentowanego limitu wolumenu (48 MB/s dla 100 GB pd-ssd) — **przy ~150 B wiadomościach (iv) nie powinno zapalać się pierwsze; jeśli tak, odnotować**. Po stronie klienta równocześnie: brak `BufferExhausted`/`TimeoutException` przed plateau (ich pojawienie się **na** plateau jest oczekiwanym objawem backpressure), a wzrost p50 ACK między kolejnymi `P` jest monotoniczny.

## 6. Wartości odniesienia (obliczane z pomiarów, nie z dokumentacji)

- `μ_CONFIG` — z E4 (mediana 3 biegów na plateau); wpisywane do dashboardu przez `PUT /api/profile` (plik `profiles.json`, generowany przez pipeline z `tab-e4-mu.csv`).
- `τ_ack, τ_e2e, c_a², c_s²` — z E1 tej samej konfiguracji (mediana 3 biegów).
- Krzywa `L_pred(ρ)` — z powyższych; rysowana na wykresie E2 i w panelu dashboardu na żywo.
- **Wzór na rate z backpressure (do manuskryptu): `R(ρ) = ρ · μ / (P · η)`**, gdzie μ = mediana R0 z dysku (1b-rf1-esmall: 432437 ± 8119), η = zmierzona wydajność dowożenia celu = **0.80 ± 0.03** (oficjalne biegi E2, mediana η per szczebel: 0.819/0.837/0.810/0.760 — płaskie przez drabinkę), P = inwokacje. Dowód: tabela ilorazów osiągnięte/cel w raporcie E2.
- Oczekiwania inżynierskie (nie hipotezy): S_3B-RF1 ≈ 2–3; μ_3B-RF3 ≈ 0,4–0,7 · μ_3B-RF1 (każda wiadomość zapisywana 3× i potwierdzana przez 2 repliki); Kingman najlepiej dopasowany dla ρ ≥ 0,5 (przybliżenie heavy-traffic), najgorzej blisko ρ→0,9 (wrażliwość na μ).

## 7. Kryteria sukcesu (deliverable-level)

1. System działa end-to-end: z dashboardu można uruchomić bieg (temat, liczba/tempo, równoległość), obserwować na żywo: słupki skumulowane per broker (kolor = temat) z podziałem na repliki, panel liderów, wykres Kingmana (pred/obs/ε, 1 Hz) — w każdej z 3 konfiguracji.
2. Grafana pokazuje w czasie: stan (całkę) i tempo (pochodną) wiadomości per temat/broker/partycja/lider oraz Kingmana z eksportera dashboardu.
3. Każda konfiguracja ma komplet E1/E4/E2 × 3 biegi z plikami wg §5.1; E4 kończy się sygnaturą brokera (albo udokumentowaną eskalacją i jej wynikiem).
4. Pipeline generuje wszystkie tabele CSV i rysunki jedną komendą; manuskrypt kompiluje się bez błędów i odwołuje wyłącznie do wygenerowanych plików.
5. Tabela skalowalności (μ_1B, μ_3B-RF1, μ_3B-RF3, S, efektywność, koszt RF) i tabela błędów Kingmana istnieją i są omówione.

## 8. Wymagania infrastrukturalne (workstream Infra — `spec/spec-infra.md`)

- **Żadnych nowych VM.** Używane: kafka-1 (10.0.0.6), kafka-2 (10.0.0.3), kafka-3 (10.0.0.4), kafka-monitoring (10.0.0.5, Prometheus/Grafana — bez zmian roli), kafka-load (10.0.0.2 — **nowa rola: host dashboardu/konsumenta**).
- **Faza 1 brokera (decyzja A):** kafka-1 sformatowany jako 1-węzłowe kworum (`controller.quorum.voters=1@10.0.0.6:9093`, RF tematów systemowych = 1), kafka-2/3 zatrzymane (`gcloud compute instances stop`). Powrót do 3 węzłów = ponowny format wszystkich trzech (nowy cluster-id, 3 voters). Playbook `ansible/kraft_reconfigure.yml` z parametrem `quorum_size ∈ {1,3}`; kopiuje wzorzec `install_kafka.yml`, nie modyfikuje go; bez ponownego pobierania Kafki.
- **Tematy:** skrypt `gcloud/create_topics.sh <CONFIG>` tworzy `weather-rain|temp|wind` (6 partycji, RF wg CONFIG, `min.insync.replicas` 1/2) i `control-metrics` (1 partycja, RF wg CONFIG). Skrypt czyszczenia usuwa wszystkie tematy `weather-*`, `control-metrics` między biegami E4 (dysk).
- **Sieć funkcji:** nowy, addytywny moduł Terraform `thesis-v2/terraform/` (własny stan): `data.google_compute_network kafka-thesis-vpc` + podsieć `kafka-serverless-subnet` (10.8.0.0/23, europe-central2) dla Direct VPC egress + reguła firewall `allow-serverless-to-kafka` (source 10.8.0.0/23 → tcp 9092, target tag `kafka`). Zero zmian w istniejącym stanie Terraform.
- **Funkcja:** deploy przez `gcloud functions deploy --gen2` + `gcloud run services update` (Direct VPC egress: `--network kafka-thesis-vpc --subnet kafka-serverless-subnet --vpc-egress private-ranges-only`, `--max-instances 300`, `--memory 1Gi --cpu 1 --concurrency 1 --timeout 600`). Connector `kafka-conn-waw` zostaje nietknięty (fallback).
- **Monitoring (addytywnie):** rozszerzone reguły JMX (kopia `deploy_jmx.yml` → `deploy_jmx_v2.yml`), dodatkowy job Prometheus `dashboard_v2` (10.0.0.2:8080/metrics), nowe pliki dashboardów Grafany obok istniejących (`update_monitoring_v2.yml` zapisuje `prometheus.yml` z istniejącymi 2 jobami **verbatim** + nowy).
- **Dashboard na kafka-load:** `deploy_dashboard_v2.yml` (hosts: load_generator), usługa `thesis-dashboard.service`, port 8080 (już otwarty regułą `allow_external`).
- **Kolektor artefaktów:** `gcloud/collect_run_artifacts.sh <run_dir>` (pobiera `run.json`, CSV z API dashboardu i `prom/*.json` z Prometheusa). `gcloud/run_experiment.sh <E1|E4|E2> <CONFIG> --runs 3` wywołuje dashboard API i kolektor.

## 9. Plan manuskryptu (workstream LaTeX — `spec/spec-manuscript.md`)

Nowy manuskrypt PL w `thesis-v2/paper/` na szablonie `../paper-pl/main.tex` (preambuła, strona tytułowa, styl bibliografii skopiowane; rozdziały od zera), ~50–60 stron, nacisk na inżynierię oprogramowania:
1. **Wstęp** — problem (dlaczego kolejki w systemach rozproszonych), cel, zakres, struktura.
2. **Kolejki i Apache Kafka** — rola kolejek (odsprzęganie, buforowanie, backpressure), Kafka vs RabbitMQ (log vs broker kolejek; dlaczego Kafka), architektura Kafki (tematy, partycje, replikacja, ISR, liderzy, KRaft), parametry używane w pracy (`acks`, RF, `min.insync.replicas`, partycje, `linger`/`batch`) i co niosą w ujęciu PACELC (krótko, opisowo), **wzór Kingmana** — jedyna matematyka: znaczenie λ, μ, ρ, τ, c², jak mierzymy każdy parametr.
3. **Projekt systemu** — wymagania; architektura i topologia (VPC, podsieci, Direct VPC egress, firewall, porty); komponenty: producenci (Java Cloud Functions; wzorce: Strategy/Factory zdarzeń, Template Method biegu, Observer/WebSocket w dashboardzie, Singleton klientów, Builder rekordów), klaster, konsument-dashboard (Javalin, WebSocket, AdminClient, MathEngine, kontroler biegów, eksporter Prometheus), monitoring (JMX → Prometheus → Grafana; definicja całki i pochodnej strumienia); kontrakty danych (JSON `WeatherEvent`, raport `control-metrics`, CSV); automatyzacja (Terraform/Ansible/gcloud), tryb 1 vs 3 brokerów.
4. **Realizacja** — implementacja producenta (limiter tempa, pomiar ACK, HdrHistogram), konsumenta (pomiar e2e bez parsowania, lag), próbkowania AdminClient, silnika Kingmana i estymatorów c², frontendu (słupki skumulowane, panel liderów, wykres Kingmana), eksportera; testy; problemy napotkane i rozwiązane (log incydentów z §10).
5. **Eksperymenty i wyniki** — metodyka (krótko), przebieg (1B → 3B), E1/E4/E2 per konfiguracja, skalowalność (P1/P2), Kingman (P3), saturacja (P4), zrzuty dashboardu i Grafany, akapit o OMB.
6. **Podsumowanie** — wnioski inżynierskie, ograniczenia, kierunki rozwoju.
Wszystkie tabele z `paper/tables/*.csv` (csvsimple), wszystkie rysunki z `paper/figures/` (pipeline) + zrzuty ekranu autora w `paper/img/`.

## 10. Podział zadań

| # | Zadanie | Właściciel | Zależy od | Kryteria akceptacji |
| --- | --- | --- | --- | --- |
| W1 | Infra: `kraft_reconfigure.yml` (quorum 1/3), `create_topics.sh`, `cleanup_topics.sh`, `stop/start_brokers.sh`, Terraform v2 (podsieć + firewall), `deploy_function_v2.sh`, `deploy_dashboard_v2.yml`, `collect_run_artifacts.sh`, `run_experiment.sh`, RUNBOOK-v2 | Infra agent (`spec/spec-infra.md`) | — | **DONE (przegląd architekta):** `bash -n` czyste dla 7 skryptów; 4 playbooki parsują się jako YAML (`ansible-playbook --syntax-check` do wykonania na kontrolerze — brak Ansible lokalnie); `terraform/main.tf` = 1 data + 2 resources (tag `kafka` na kafka-1/2/3 potwierdzony w `../terraform/compute.tf:40`); `kraft_reconfigure.yml` przejrzany: format/start tylko na aktywnych, wymaga `confirm_wipe=yes`; **poprawka architekta:** usunięto `args: warn: false` (parametr wycofany w ansible-core ≥ 2.14). RUNBOOK-v2 (499 linii) z układem kontrolera `~/thesis-v2/` obok `~/ansible/`. **Do wykonania przez autora:** `terraform init && terraform plan` (oczekiwane `2 to add`) |
| W2 | Cloud Function v2: parametry `topic/eventType, count|durationSec, ratePerSec, runId, invocationId`, limiter tempa, pomiar ACK (HdrHistogram), raporty 1 s do `control-metrics`, odpowiedź HTTP z podsumowaniem, klasyfikacja błędów | Function agent (`spec/spec-function.md`) | — | **DONE (zweryfikowane przez architekta w WSL):** `mvn test` 19/19 zielone (RateLimiter 5, ReportSerialization 4, EventStrategyFactory 10), `target/weather-producer-function-v2-2.0.0.jar` zbudowany; przegląd `LoadRun.java`: brak jawnego timestampu rekordu, ACK w callbacku → HdrHistogram okna + całościowy, `avgRecordBytes` z `RecordMetadata`, raport `final:true` po `flush()`. Decyzje workera: `intensity∉[0,1]` → 400; `client.id=fn-<runId>-<invocationId>` (runId generowany przez dashboard musi być `[A-Za-z0-9-]`) |
| W3 | Dashboard v2 (Java/Javalin na kafka-load): konsument e2e (bez parsowania), AdminClient (offsety, liderzy, repliki, 1 Hz), scalanie HdrHistogram z `control-metrics`, MathEngine (Kingman, estymatory c², profile μ/τ per CONFIG), kontroler biegów (`POST /api/run`, fan-out do funkcji, manifest), eksport CSV/JSON, eksporter Prometheus `/metrics`, frontend (formularz, słupki skumulowane per broker × temat × replika, panel liderów, wykres Kingmana 1 Hz, karta błędu) | Dashboard agent (`spec/spec-dashboard.md`) | W2 kontrakt | **DELIVERED — kompilacja w toku u autora** (WSL ma tylko JRE 21 bez `ct.sym`, `--release 17` nie działa lokalnie; autor uruchamia `mvn test` na Windowsie). Struktura wg §6 + `run/PromQueryClient.java`; WS pod `/ws`; `ack_count` = acked zamkniętego kubełka; abort nie przerywa funkcji (udokumentowane) |
| W4 | Monitoring: `deploy_jmx_v2.yml` (reguły per-topic/per-partition/lider/idle-percent), `update_monitoring_v2.yml` (job `dashboard_v2` + nowe dashboardy), dashboardy Grafany `thesis-v2-cluster-live.json`, `thesis-v2-kingman.json`, `thesis-v2-saturation.json` | Monitoring agent (`spec/spec-monitoring.md`) | W3 nazwy metryk | **DONE (zweryfikowane):** 3 JSON-y parsują się (uid `thesis-v2-live/-kingman/-sat`), `deploy_jmx_v2.yml` = 9 reguł oryginalnych verbatim + 6 nowych (przed regułą bez `topic`), `update_monitoring_v2.yml` = 2 joby verbatim + `dashboard_v2`. Decyzja architekta: układ na kontrolerze = `~/thesis-v2/` obok `~/ansible/` i `~/monitoring_node/` na kafka-monitoring (ścieżki `../../monitoring_node/…` poprawne); inventory = kopia w `thesis-v2/ansible/` (W1). Metryki bez panelu (`kafka_log_size`, ISR count, `thesis_broker_msgs`…) celowo tylko w eksporterze |
| W5 | Analiza: parsery (`run.json`, `series.json`, `ack.json`, `prom/*.json`), estymatory (τ, c², μ-plateau, sygnatura), tabele CSV, rysunki, `profiles.json` dla dashboardu, testy na fixture'ach | Analysis agent (`spec/spec-analysis.md`) | układ §5.1, W3 formaty | **DONE (zweryfikowane, jedna runda poprawek):** pierwsza dostawa 4 testy czerwone (asercje fixture 9/7 vs 10/8; `KeyError: mbps`) → naprawione; `pytest` 38/38, `analyze.py` na fixture'ach → 12 tabel + 39 plików rysunków + `profiles.json` (1b-rf1: μ=9495.6 msg/s **syntetyczne**). Decyzje: τ_ack z `summary.ackP50` (fallback mediana serii, kolumna `method`), `max` po węzłach dla NIC/dysku, priorytet sygnatury (i)→(iv). **UWAGA:** `paper/tables/` i `paper/figures/` zawierają obecnie dane z fixture'ów — służą wyłącznie jako stuby do kompilacji W6; nadpisze je `analyze.py --results results/` po U1/U2 |
| W6 | Manuskrypt PL (rozdziały 1–6, streszczenia, bibliografia, tabele z CSV, guardy `\IfFileExists` na rysunki) | **Architekt osobiście** (`spec/spec-manuscript.md`) — decyzja autora 2026-09-07: manuskrypt pisze architekt, **dopiero po uruchomieniu systemu i zebraniu wyników (U1/U2)**; delegacja W6 zatrzymana przed jakimkolwiek zapisem plików | U1, U2, W5 prawdziwe CSV | kompilacja `pdflatex+biber` 0 błędów; każda tabela z CSV; brak twardo wpisanych liczb wynikowych; `img/logo.pdf` skopiować binarnie przez shell (nie `write`) |
| U1 | Wykonanie Fazy A (1b-rf1: E1, E4, E2) na GCP wg RUNBOOK-v2 | **Autor** | W1–W4 | `results/*/1b-rf1/` kompletne |
| U2 | Wykonanie Fazy B (3b-rf1, 3b-rf3) | **Autor** | U1 | `results/*/3b-*/` kompletne |
| U3 | Zrzuty ekranu dashboardu i Grafany do `paper/img/` (lista w spec-manuscript) | **Autor** | U1–U2 | pliki obecne |
| A1 | Przegląd wyników, fold-back do §10, rewizja manuskryptu | Architekt | U1–U3, W5 | plan i spec zaktualizowane |

### 10b. Dziennik wykonania (incydenty, decyzje, pomiary czasu) — uzupełniany w trakcie

- 2026-09-08 — sieć serverless DONE: podsieć `10.20.0.0/23` + firewall po `apply` (`1 added, 1 changed`); tagi 3× `kafka`; tematy 4× `OK RF=3`; funkcja `hexweather-producer-v2` rev `00003-rsh` (`100×1CPU`, Direct VPC egress). Po drodze: konflikt CIDR `10.8`, lean sync, prefiks `researcher@`, regex RF, sufity 300→200→100. Raport: [`reports/2026-09-08-infra-siec-serverless.md`](reports/2026-09-08-infra-siec-serverless.md).
- 2026-09-08 — decyzja RF: workload telemetryczny o malejącej wartości → docelowo RF=1, RF=3 jako zmierzony koszt (P1/P2/P3 bez dogmatu), trwałość per temat do Podsumowania. Raport: [`reports/2026-09-08-rf-decyzja-trwalosc.md`](reports/2026-09-08-rf-decyzja-trwalosc.md).
- 2026-09-08 — split deployu dashboardu: install (root) / build (`researcher`, serwis `User=` nie-root) po awarii `target/` na własności. Raport: [`reports/2026-09-08-dashboard-root-split.md`](reports/2026-09-08-dashboard-root-split.md).
- 2026-09-08 — decyzja SQLite: koniec plików CSV w dashboardzie (po błędach summary ze smoke), jeden `thesis.db` (WAL), serie/kubełki po JSON; W3+W5 w przebudowie, kolektor i specyfikacje (dashboard/infra/analysis) + plan §5.1 już przestawione. Raport: [`reports/2026-09-08-sqlite-decyzja.md`](reports/2026-09-08-sqlite-decyzja.md).
- 2026-09-08 — strażnik narracji: generator to nie symulacja pogody (`intensity=0.7` stałe, liczy się gabaryt ~134 B). Raport: [`reports/2026-09-08-generator-narracja.md`](reports/2026-09-08-generator-narracja.md).
- 2026-09-08 — CI/CD przez IAP: workflow `deploy-dashboard.yml` (test-gate + deploy tunelem, bez statycznego IP); seria dostępowa MUSI do manuskryptu (topologia 3.2 + metodyka 3.7). Raport: [`reports/2026-09-08-cicd-iap.md`](reports/2026-09-08-cicd-iap.md).
- 2026-09-08 — mapa przypisów theory/ → rozdziały (co cytować/gdzie/czego nie tykać za §11) + nowe klucze biblio w spec-manuscript. Raport: [`reports/2026-09-08-teoria-mapa.md`](reports/2026-09-08-teoria-mapa.md).
- 2026-09-08 — pokrycie źródłowe decyzji technicznych (checklista W6: decyzja → docs/ADR/CSV + reguła „akapit bez pokrycia nie wchodzi"). Raport: [`reports/2026-09-08-pokrycie-zrodel.md`](reports/2026-09-08-pokrycie-zrodel.md).
- 2026-09-09 — pull-model deployu działa end-to-end (CI → inbox → agent → `deployed OK`, zombie ubite bez rąk, health wraca); E1 dobite do cs2 × 3, kalibracja odblokowana. (Wszystkie raporty z 09-08 kontynuowane z datą 09-09; pliki zachowują nazwy.)
- 2026-09-09 — biblioteka profili w SQLite (tabela `profiles` + aktywny na CONFIG), OMB jako profil do wyboru, selektor na karcie Wykresów (Kalibracja tylko zapisuje), profil `live` samouzupełniający; dowody OMB uratowane do `results/OMB-reference/`. Raport: [`reports/2026-09-09-profile.md`](reports/2026-09-09-profile.md).
- 2026-09-09 — ZAMKNIĘCIE DNIA: kampania OMB najpierw (E0 per konfiguracja, drabinka progowa, importer wyspecyfikowany), emergency STOP wycięty w całości (stop ≠ wipe), resztki posprzątane. Raport: [`reports/2026-09-09-omb-kampania.md`](reports/2026-09-09-omb-kampania.md).

## 11. Świadome wyłączenia zakresu

Brak testów odporności na awarie (partycje sieciowe, TTR), brak PACELC jako przedmiotu eksperymentu (tylko opis konfiguracji), brak EVT/GPD, brak Jepsen/TLA+, brak OMB (odniesienie w jednym akapicie), jeden region GCP, ≤3 powtórzenia, brak strojenia JVM brokerów. Każde wyłączenie zapisane w rozdziale „Ograniczenia”.
