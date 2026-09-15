# thesis-v2/analysis -- pipeline analizy (W5)

Pipeline Python (bez scipy/seaborn) zamieniający wyniki biegów
(`results/<EXP>/<CONFIG>/[P<P>|rho<R>/]run<N>/`, patrz
`../spec/spec-analysis.md` SS1 i `../experiment-plan.md` SS5.1) w tabele CSV
(`paper/tables/`), rysunki PDF+PNG (`paper/figures/`), profil dla dashboardu
(`analysis/output/profiles.json`) i podsumowanie słowne
(`analysis/output/summary.md`).

Nazwy wszystkich plików wyjściowych są **zamrożone** (`spec-analysis.md`
SS3) -- manuskrypt (W6) się do nich odwołuje wprost.

## Wymagania

Python 3.12, `pandas`, `numpy`, `matplotlib`, `pytest` (bez `scipy`/`seaborn`).

```bash
cd C:\thesis\thesis-v2\analysis
pip install -r requirements.txt
```

## Uruchomienie testów

```bash
cd C:\thesis\thesis-v2\analysis
pytest -q
```

Testy używają syntetycznych fixture'ów w `tests/fixtures/results/` (1 bieg
E1, 2 szczeble P (10, 25) dla E4, 1 szczebel rho50 dla E2 -- wszystkie dla
`1b-rf1`, w prawdziwym formacie plików: `run.json`, `series.json`,
`ack.json`, `prom/*.json`, `config.env`, `window.env`).

## Uruchomienie pipeline'u

```bash
cd C:\thesis\thesis-v2\analysis
python analyze.py --results tests/fixtures/results --out-tables ../paper/tables --out-figures ../paper/figures --profiles output/profiles.json
```

Na prawdziwych danych (po zebraniu wyników przez W1
`gcloud/collect_run_artifacts.sh`):

```bash
cd C:\thesis\thesis-v2\analysis
python analyze.py --results ../results --out-tables ../paper/tables --out-figures ../paper/figures --profiles output/profiles.json
```

CLI zawsze tworzy **wszystkie** pliki z `spec-analysis.md` SS3, nawet gdy
brakuje danych dla którejś konfiguracji/szczebla -- brakujące wartości są
`NaN` (puste komórki CSV), a pipeline drukuje `WARNING: ...` na stdout i
dopisuje te same ostrzeżenia na koniec `output/summary.md`. Dzięki temu W6
(LaTeX) nigdy nie trafia na brakujący plik `\input`.

Wyjście końcowe CLI (`OK: ...`) raportuje liczbę wygenerowanych wierszy per
sekcja i liczbę ostrzeżeń -- `0` ostrzeżeń oznacza komplet danych dla
wszystkich 3 konfiguracji.

## Struktura kodu

| Plik | Rola |
| --- | --- |
| `parsers.py` | Czyste funkcje parsujące: `parse_prom_json` (Prometheus `query_range`, wzorzec z `C:\thesis\analysis\parsers.py`), `parse_dashboard_series_json`, `parse_ack_reports_json`, `parse_run_json`, `parse_env_file`, `parse_error_types` (obiekt JSON albo legacy string). |
| `loaders.py` | Discovery katalogów (`discover_e1/e4/e2`), `RunData` (dataclass), okno analizy `[start+15000, stop]`, `dispersion()` (statystyki n=3: mediana/średnia/SD/CoV/min/max). |
| `kingman.py` | **Jedyne** miejsce ze wzorem Kingmana: `wq(rho, ca2, cs2, tau)`, `pred_latency()`, `error()`. |
| `metrics.py` | E1 (tau/c²), E4 (drabinka + plateau μ + sygnatura saturacji), E2 (drabinka ρ + błąd Kingmana), skalowalność, census/time-budget. |
| `tables.py` | Emisja **wszystkich** CSV z nazwami zamrożonymi w `spec-analysis.md` SS3. |
| `figures.py` | Emisja **wszystkich** rysunków (PDF+PNG) + bliźniacze `data-fig-*.csv`. |
| `style.py` | rcParams (czarno-biały styl czytelny), `save_fig`, `write_csv`, `place_legend`, log `WARNINGS`. |
| `analyze.py` | CLI: `--results`, `--out-tables`, `--out-figures`, `--profiles`; orkiestruje wszystko powyżej, pisze `profiles.json` i `summary.md`. |

## Jak dodać konfigurację / nowe biegi

1. Skopiuj wynik zebrany przez `gcloud/collect_run_artifacts.sh` do
   `results/<EXP>/<CONFIG>/[P<P>|rho<R>/]run<N>/` z plikami: `run.json`,
   `series.json`, `ack.json`, `prom/*.json`, `config.env`,
   `window.env` (formaty: `spec-dashboard.md` SS5, `spec-infra.md` R7).
2. `CONFIG` musi być jednym z `1b-rf1`, `3b-rf1`, `3b-rf3`; `run<N>` musi być
   `run` + liczba (np. `run1`, `run2`, `run3`).
3. Uruchom ponownie `analyze.py` z `--results` wskazującym na katalog
   nadrzędny `results/` -- wszystkie tabele/rysunki się przeliczą; brakujące
   biegi (np. tylko 2 z 3 powtórzeń) dają mniejsze `n` w statystykach
   dyspersji (CoV nadal liczony dla n>=2), nie błąd.
4. Jeśli brakuje całej konfiguracji, pipeline i tak wypełni jej wiersze w
   każdej tabeli wartością `NaN` (puste komórki) i wypisze ostrzeżenie --
   żadny plik z listy zamrożonych nazw nigdy nie znika.

## Generowanie `profiles.json` i wgranie do dashboardu

`analyze.py` zawsze zapisuje `analysis/output/profiles.json` (dokładnie ten
plik, ścieżka podana przez `--profiles`) w formacie `spec-dashboard.md` SS5:

```json
{
  "1b-rf1": {"muMsgs": 9500.0, "tauAckMs": 8.02, "tauE2eMs": 22.02,
             "ca2": 1.0, "cs2": 0.5, "source": "E1/E4 pipeline (analyze.py)"},
  "3b-rf1": {...},
  "3b-rf3": {...}
}
```

Wartości pochodzą z median (`tab-e1-summary.csv` dla `tau_ack_med`,
`tau_e2e_med`, `ca2_med`, `cs2_med`; `tab-e4-mu.csv` dla `mu_msgs`).
Konfiguracja bez kompletu E1+E4 dostaje pola `null` (dashboard pokazuje
"brak profilu -- wykonaj E1/E4" wg `spec-dashboard.md` R4).

Aby wgrać nowy profil do dashboardu v2 (po skopiowaniu pliku na
`kafka-load` albo bezpośrednio, jeśli masz dostęp do `DASH_URL`):

```bash
curl -X PUT "$DASH_URL/api/profile" \
  -H "Content-Type: application/json" \
  --data-binary @output/profiles.json
```

Uwaga: `PUT /api/profile` (wg `spec-dashboard.md` R4/SS4) przyjmuje **jeden**
profil (dla aktualnego `CONFIG` działającej instancji dashboardu), nie mapę
wszystkich trzech konfiguracji -- jeśli endpoint faktycznie oczekuje obiektu
pojedynczego profilu (`{"muMsgs":..,"tauAckMs":..,...}`), wyciągnij właściwy
klucz z `profiles.json` przed wysyłką, np.:

```bash
python -c "import json,sys; d=json.load(open('output/profiles.json')); print(json.dumps(d['1b-rf1']))" \
  | curl -X PUT "$DASH_URL/api/profile" -H "Content-Type: application/json" --data-binary @-
```

(Zobacz pytanie w raporcie końcowym worker'a dot. dokładnego kształtu body
`PUT /api/profile` -- ten README zakłada wariant "jeden profil na żądanie",
zgodny z R6 spec-infra.md, który kopiuje cały `profiles.json` na dysk hosta
zamiast wołać ten endpoint).

## Weryfikacja (komendy dla autora)

```bash
cd C:\thesis\thesis-v2\analysis
pip install -r requirements.txt
pytest -q
python analyze.py --results tests/fixtures/results --out-tables ../paper/tables --out-figures ../paper/figures --profiles output/profiles.json
```

Oczekiwany wynik: `pytest -q` zielone; ostatnia komenda drukuje
`OK: e1_tau_rows=1 e4_ladder_rows=2 e2_summary_rows=12 warnings=<N>` i
tworzy komplet plików w `../paper/tables/`, `../paper/figures/` oraz
`output/profiles.json`, `output/summary.md`.
