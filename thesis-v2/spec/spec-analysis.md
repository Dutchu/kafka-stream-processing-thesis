# Spec W5 — Pipeline analizy: CSV do tabel, rysunki, profile Kingmana (thesis-v2)

**Właściciel:** Analysis agent. **Plan:** `experiment-plan.md` §2, §4, §5, §6, §7.
**Wzorce (tylko do czytania, można kopiować fragmenty):** `C:\thesis\analysis\{parsers.py,theory.py,style.py,loaders.py}` (parser `prom/*.json`, styl rysunków). Środowisko: Python 3.12, `pandas`, `numpy`, `matplotlib`, `pytest` (bez seaborn/scipy — prostota).
**Zasada:** wszystkie liczby w manuskrypcie pochodzą z CSV wygenerowanych tutaj; żadnych ręcznych tabel. Nazwy plików wyjściowych są **zamrożone** (manuskrypt W6 odwołuje się do nich).

## 1. Wejścia

`thesis-v2/results/<EXP>/<CONFIG>/[P<P>|rho<R>/]run<N>/` z plikami: `run.json` (manifest, `spec-dashboard.md` R5), `series.json` (array obiektów 1 Hz, klucze `spec-dashboard.md` §5, `null` = brak danych), `ack.json` (array kubełków, `errors` jako obiekt), `prom/*.json` (klucze z `spec-infra.md` R7), `config.env`, `window.env`. `CONFIG ∈ {1b-rf1, 3b-rf1, 3b-rf3}` **z przyrostkiem żelaza** `-<HW>` (`es4` = e2-standard-4, `esmall` = e2-small): `loaders.py` rozcina ostatni segment na `config` + `hw` po ostatnim `-` przed cyfrą wariantu (np. `3b-rf3-es4` → `3b-rf3` + `es4`); tabele niosą kolumnę `hw`, a porównania między konfiguracjami wymagają zgodnego `hw` (mieszanie es4 z esmall w jednej figurze = błąd walidacji). Okno analizy: `[startEpochMs+15000, stopEpochMs]` z manifestu.

**Wejścia OMB** (kampania E0, plan §4 — TYLKO `1b-rf1`: trzech brokerów jednym producentem się nie przeciąży): `thesis-v2/results/OMB-reference/1b-rf1-<hw>/T<rung>/run<N>/` z plikami: `perf.txt` (interim + agregaty `perf-test`), `run_window.env`, `prom/*.json` oraz eksporty Grafany (`*-per-broker-*.csv`, opcjonalne, tylko do wglądu). **Konwencja parametrów:** każdy katalog runa mierzy τ_ack (z `perf.txt`); τ_e2e tylko gdy obok leży `perf-consumer.txt` (konsument musiał jechać w trakcie obciążenia — wstecz się nie da). Parsery v1 (`C:\thesis\analysis\parsers.py`: `parse_perf_interim_lines`, `parse_perf_aggregate_lines`) kopiować, nie importować. Wpis w `profiles.json` ma klucz **`OMB`** (jeden profil odniesienia; `config` = `1b-rf1`).

## 2. Obliczenia

- **E1 (per bieg):** `tau_ack_ms` = `summary.ackP50` z manifestu (histogram całego biegu; fallback = mediana `ack_p50_ms` z kubełków, kolumna `method`); `tau_e2e_ms` = mediana `e2e_p50_ms`; `cs2` = ODPORNE (lustro dashboardu): mediana po kubełkach ack.json w oknie z `(p95-p50)/1.645` do kwadratu nad `summary.ackMean` (min. 3 kubełki, inaczej NaN + warning) — sd z histogramu całego biegu jest trute pojedynczymi stallami rozruchowymi (zmierzone: cs2=432 vs odporne ~0.001 na 1b-rf1-esmall); `ca2` = mediana `ca2_est` w oknie, a gdy niepoliczalna (E1 za chude) — fallback Poissona `1.0` z warningiem (lustro endpointu from-run dashboardu). Per konfiguracja: mediana z 3 biegów, CoV, zakres.
- **E0/OMB (per szczebel throttlingu, per bieg):** z interim `perf.txt`: `thr_target` (cel `--throughput`, `inf` = bez limitu), `lambda_mean/msg_s` (średnia `records/sec` ze szczebla), `mbps`, `ack_p50_ms` (z agregatu, gdy jest; inaczej mediana interim-`avg`), `ack_p99_ms`, `payload_bytes` (z nagłówka `mean payload`); plateau μ per konfiguracja jak w E4 (reguła 5 % + sygnatura z `prom/` gdy dostępna). Wyjście w `tab-omb-profile.csv`: `config, mu_msgs, mu_sd, mu_cov, mu_n, tau_ack_ms, tau_ack_sd, payload_bytes, method` — kolumny błędu OBOWIĄZKOWE (profil bez błędu nie wchodzi do biblioteki). `τ_e2e/ca2/cs2` dla OMB = NaN z `method=unavailable-no-e2e`. Porównanie OMB-vs-funkcje (`fig-omb-vs-functions`: μ i τ_ack per źródło × konfiguracja) jest wynikiem P4.
- **E4 (per P, per bieg):** `lambda_mean`, `lambda_p50` z `lambda_leo` w oknie; `mbps = bytes_rate/1e6`; `ack_p50/p99/mean`, `e2e_p50/p99`, `failed`, `error_types`; sygnatura z `prom/`: `handler_idle_min` (min po brokerach średniej w oknie `prom_handler_idle`), `netproc_idle_min`, `cpu_busy_max`, `request_queue_mean`, `nic_rx_max_bps`, `disk_write_max_bps`, `signature_fired` (reguła §5.4 planu), `binding_resource` = nazwa pierwszego progu przekroczonego (`cpu|handler|network_processor|request_queue|nic|disk|none`). Plateau: dla konfiguracji uporządkuj P rosnąco; `mu_plateau` = mediana `lambda_mean` (3 biegi) dla najmniejszego P, po którym kolejne P nie zwiększa mediany λ o > 5 %; `mu_msgs` = ta mediana; jeśli sygnatura nie zapaliła się na plateau → `mu_status = "client-limited"` i tabela to raportuje (manuskrypt musi to omówić); `p50_growth` = `ack_p50(P_max)/ack_p50(P_plateau)`.
- **E2 (per szczebel, per bieg):** `rho_target`, `lambda_mean`, `rho_real = lambda_mean/mu_msgs` (μ z E4 tej konfiguracji), `ack_mean/p50/p99`, `e2e_mean/p50/p99`, `wq_pred = rho/(1−rho)·(ca2+cs2)/2·tau_ack` (parametry z E1 tej konfiguracji — pipeline liczy niezależnie od dashboardu, a kolumna `pred_ack_dashboard` z serii służy do kontroli zgodności ±1 %), `pred_ack = tau_ack + wq_pred`, `pred_e2e = tau_e2e + wq_pred`, `err_ack = ack_mean − pred_ack`, `err_ack_rel`, analogicznie e2e; `consumer_limited` z manifestu; **brama backlogu:** start z `consumer_lag > 100k` → `backlog_contaminated=true`, e2e biegu wypada z median szczebla i z błędów Kingmana (ACK/λ niewzruszone). Per konfiguracja: `MAE_ack`, `MAPE_ack`, `MAE_e2e`, `MAPE_e2e` po 4 szczeblach (mediany biegów).
- **Skalowalność:** `S_rf1 = mu(3b-rf1)/mu(1b-rf1)`, `eff = S_rf1/3`, `rf_cost = mu(3b-rf3)/mu(3b-rf1)`, `S_rf3 = mu(3b-rf3)/mu(1b-rf1)`; także dla τ (`tau_ack` per konfiguracja) — koszt replikacji w opóźnieniu bazowym.
- **Statystyki n=3:** mediana, średnia, SD (próbkowe), CoV, min, max — funkcja wspólna.

## 3. Wyjścia (nazwy zamrożone)

Tabele → `thesis-v2/paper/tables/`:
| Plik | Kolumny |
| --- | --- |
| `tab-e1-tau.csv` | `config, run, tau_ack_ms, tau_e2e_ms, cs2, ca2, ack_count, e2e_count, method` |
| `tab-e1-summary.csv` | `config, tau_ack_med, tau_ack_cov, tau_e2e_med, tau_e2e_cov, cs2_med, ca2_med` |
| `tab-e4-ladder.csv` | `config, P, run, lambda_mean, lambda_p50, mbps, ack_mean, ack_p50, ack_p99, e2e_p50, failed, error_types, handler_idle_min, netproc_idle_min, cpu_busy_max, request_queue_mean, nic_rx_max_mbps, disk_write_max_mbps, signature_fired, binding_resource` |
| `tab-e4r-ladder.csv` | jak `tab-e4-ladder` + `rate` zamiast `P` (drabinka rate'owa E4R, lustro OMB) |
| `tab-e4r-mu.csv` | jak `tab-e4-mu`, kolumna plateau to `rate_plateau` |
| `tab-e4-mu.csv` | `config, P_plateau, mu_msgs, mu_cov, mu_mbps, ack_p50_plateau, ack_p50_pmax, p50_growth, binding_resource, mu_status` |
| `tab-scalability.csv` | `metric, 1b-rf1, 3b-rf1, 3b-rf3, S_rf1, eff_rf1, rf_cost, S_rf3` (wiersze: `mu_msgs`, `mu_mbps`, `tau_ack_ms`, `tau_e2e_ms`) |
| `tab-e2-ladder-<config>.csv` (×3) | `rho_target, run, lambda_mean, rho_real, ack_mean, ack_p50, ack_p99, e2e_mean, e2e_p50, e2e_p99, wq_pred, pred_ack, pred_e2e, err_ack, err_ack_rel, err_e2e, err_e2e_rel, consumer_limited` |
| `tab-e2-summary.csv` | `config, rho_target, rho_real_med, ack_mean_med, pred_ack, err_ack_med, err_ack_rel_med, e2e_mean_med, pred_e2e, err_e2e_med, err_e2e_rel_med` |
| `tab-e2bp-ladder.csv` / `tab-e2bp-summary.csv` | jak E2 + kolumna `variant` (warianty potwierdzające `rho90bp`, … — trójka standardowa nigdy nie mieszana) |
| `tab-partition-usage.csv` | `topic, partition, broker, size_bytes, source` (dowód rozkładu: `partitioning-id-evidence/*.json` + legacy `evidence/logdirs-*` + `prom_log_bytes`) |
| `tab-partition-imbalance.csv` | `topic, n_partitions, min_bytes, max_bytes, max_min_ratio, cov` (jednorazowy dowód do manuskryptu; ratio=inf = sygnatura hot-partycji) |
| `tab-kingman-error.csv` | `config, MAE_ack_ms, MAPE_ack, MAE_e2e_ms, MAPE_e2e, best_rho, worst_rho` |
| `tab-run-census.csv` | `exp, config, label, runs_ok, runs_failed, runs_consumer_limited, total_msgs` |
| `tab-time-budget.csv` | `exp, config, runs, wall_clock_min` (z epok manifestów) |
| `tab-omb-profile.csv` | `config, mu_msgs, mu_sd, mu_cov, mu_n, tau_ack_ms, tau_ack_sd, payload_bytes, method` (profil OMB z błędami — ten wiersz zasila wpis `OMB` w `profiles.json`) |

Rysunki → `thesis-v2/paper/figures/` (PDF + PNG, styl: czarno-biało czytelny, markery per konfiguracja, siatka lekka; `style.py` z oryginału jako wzorzec):

> **NA JUTRO (decyzja autora 2026-09-09): każdy wykres z analizy MUSI nieść błąd:** każda krzywa dostaje CIENIOWANE pasmo (`fill_between`) wzdłuż niej — górny/dolny limit z wyznaczonego błędu (MAE/SD/min–max ze zmierzonych danych, per figura w opisie); każdy słupek dostaje WĄSY (`yerr`). Rysunek bez pasma/wąsów nie przechodzi review W5. Dotyczy też figur OMB (`fig-omb-ladder`, `fig-omb-vs-functions`).
- `fig-kingman-<config>` (×3): krzywa `pred_ack(ρ)` i `pred_e2e(ρ)` dla ρ∈[0,0.95) z CIENIEM ±MAE (MAE ze `summary.md` per konfiguracja), punkty ACK mean i e2e mean (mediany biegów, słupki min–max), etykiety ρ_real.
- `fig-kingman-all`: trzy krzywe ACK + punkty na jednym wykresie (porównanie konfiguracji); cienie ±MAE per konfiguracja.
- `fig-kingman-error`: słupki ε_rel per szczebel × konfiguracja.
- `fig-e4-saturation-<config>` (×3): oś X = P, oś Y1 = λ (msg/s, mediana, min–max), oś Y2 = ack p50 i p99 (log), zaznaczone P_plateau; pod spodem panel z `handler_idle_min`, `cpu_busy_max`.
- `fig-e4-timeseries-<config>-P<Pplateau>`: przebieg 1 Hz jednego biegu na plateau: λ, ack p50, lag (dowód „p50 rośnie / kolejka rośnie”).
- `fig-scalability`: słupki μ per konfiguracja z linią „idealne 3×”.
- `fig-e1-tau`: boxplot/punkty ACK i e2e per konfiguracja.
- `fig-omb-ladder`: oś X = throttle, Y1 = osiągnięte msg/s (mediana biegów + CIENIOWANE pasmo SD), Y2 = p99 (log); plateau μ_omb na 1b-rf1.
- `fig-omb-vs-functions`: μ i τ_ack per źródło (OMB vs funkcje) × konfiguracja — wynik P4.
- `fig-client-bound`: singiel vs suma dual (1b-rf1, T200/Tinf2x) z adnotacją CPU klienta (99.9%) i brokera (37.6%) — dowód „za duży broker" do rozdziału o ograniczeniach aparatury.
- `fig-e2-target-vs-real`: oś X = rate zadany, oś Y = rho_real; serie nominalna (×) i korygowana η (●) + przekątna celu; cienie z SD trójek (dowód działania współczynnika).
- `fig-omb-vs-functions-ladder-<cfg-hw>`: oferowane vs dowiezione (log-log) per źródło — OMB (thr → rps ± SD) vs funkcje (rate×P → λ, min/max); przekątna y=x; w tytule adnotacja o payloadach (porównywalne msg/s, nie MB/s).
Każdy rysunek ma bliźniaczy `data-fig-*.csv` (dane wykreślone) w `paper/figures/`.

Dodatkowo: `thesis-v2/analysis/output/profiles.json` (format `spec-dashboard.md` §5; wartości z `tab-e1-summary` i `tab-e4-mu`) oraz `thesis-v2/analysis/output/summary.md` (zestawienie wyników słownie z liczbami — źródło dla W6 przy pisaniu omówień). **Kontrakt: nieznane pola OMIJAMY, nigdy null** (POJO trzyma NaN-defaulty dla brakujących kluczy; jawny null zrzutowałby się do 0.0 w prymitywach Javy i zepsuł Kingmana).

**Wpisy OMB:** `omb_import.py` dopisuje do `profiles.json` klucz `OMB` (jeden profil odniesienia z `tab-omb-profile.csv`: μ/τ z błędami jako komentarz w `method`; pola niemierzalne = NaN). Dashboard przy starcie importuje brakujące wpisy z pliku do biblioteki — profil OMB jest gotowy bez klikania, a selektor na karcie Wykresów pokazuje go obok kalibracyjnych i `live`.

## 4. Struktura kodu

```
thesis-v2/analysis/
  requirements.txt  README.md  analyze.py (CLI: --results DIR --out-tables DIR --out-figures DIR --profiles PATH)
  loaders.py    # discovery katalogów, wczytanie run.json/series.json/ack.json/prom JSON → dataclasses
  omb_import.py # importer kampanii E0: perf.txt/omb.json (parsery portowane z v1) → wiersze tab-omb-profile + wpis OMB w profiles.json
  parsers.py    # parse_prom_json (skopiowany/przystosowany), okno analizy, statystyki n=3
  metrics.py    # E1/E4/E2/skalowalność (funkcje czyste na DataFrame)
  kingman.py    # wq(), pred(), błąd — jedno miejsce z formułą
  tables.py     # emisja CSV (nazwy §3)
  figures.py    # rysunki (nazwy §3) + data-fig CSV
  style.py
  tests/  test_kingman.py  test_metrics.py  test_loaders.py  fixtures/ (mini results/ z 1 biegiem E1, 2×E4 (P=10,25), 1×E2 dla 1b-rf1 — syntetyczne, ale w prawdziwym formacie)
```
`analyze.py` musi działać na niepełnych danych (np. tylko 1b-rf1) — brakujące konfiguracje → puste wiersze/`NaN`, ostrzeżenie, ale wszystkie pliki z §3 powstają (W6 kompiluje bez `\IfFileExists`-niespodzianek).

## 5. Kryteria akceptacji
- `pytest -q` zielone; `python analyze.py --results tests/fixtures/results …` tworzy **wszystkie** pliki z §3.
- `kingman.py` daje `wq(0.5, 1.0, 0.5, 6.0) == 4.5`.
- Kolumny CSV dokładnie jak w §3 (W6 używa `csvsimple` — nagłówki bez spacji, separator `,`, liczby z kropką, 2–3 miejsca dla ms, 3 dla ρ/współczynników).
- `mu_status="client-limited"` pojawia się, gdy sygnatura nie zapaliła się na plateau.
- README: jak uruchomić, jak dodać konfigurację, jak wygenerować `profiles.json` i wgrać do dashboardu (`PUT /api/profile`).

## 6. Prompt dla workera (samowystarczalny)
> Jesteś inżynierem danych w Pythonie. Utwórz `C:\thesis\thesis-v2\analysis\` dokładnie wg `C:\thesis\thesis-v2\spec\spec-analysis.md`. Formaty wejściowe: `C:\thesis\thesis-v2\spec\spec-dashboard.md` §5 (series.json, ack.json, run.json, profiles.json) i `C:\thesis\thesis-v2\spec\spec-infra.md` R7 (klucze `prom/*.json`, format odpowiedzi Prometheus `query_range`); reguła sygnatury saturacji: `C:\thesis\thesis-v2\experiment-plan.md` §5.4. Wzorce do czytania/kopiowania fragmentów: `C:\thesis\analysis\parsers.py` (parse_prom_json), `style.py`. Nie modyfikuj nic poza `thesis-v2/analysis/`, `thesis-v2/paper/tables/`, `thesis-v2/paper/figures/`. Nie masz shella: napisz kod i testy oraz podaj komendy (`pip install -r requirements.txt && pytest -q && python analyze.py …`). Stwórz syntetyczne fixture'y w prawdziwym formacie. Nazwy wyjść są zamrożone. Raport: lista plików, mapowanie na §2–§3, komendy, pytania.
