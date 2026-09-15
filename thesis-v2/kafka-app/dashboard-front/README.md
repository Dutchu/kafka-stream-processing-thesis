# thesis-dashboard-frontend — samodzielny projekt Node widoku

Vanilla JS + Chart.js (+ Vite tylko do lokalnego devu; produkcja to gołe pliki,
bez bundlowania). Serwis podaje ten katalog z **dysku** (`/opt/thesis-frontend`,
priorytet) z fallbackiem do classpath w jarze. Zmiana HTML/CSS/JS = podmiana
plików, **bez `mvn package` i bez restartu usługi**.

```bash
cd thesis-v2/kafka-app/dashboard-front
npm install     # raz (Vite do devu)
npm run dev     # http://localhost:3000 + proxy /api i /ws -> tunel (patrz niżej)
npm run lint    # node --check na kazdym JS (to samo robi bramka CI)
npm run build   # dist/ (weryfikacja, nie deploy — deploy to sync plikow zrodlowych)
```

## Struktura

| Plik | Rola — co tu ruszać przy zmianie układu |
|---|---|
| `index.html` | Szkielet: nagłówek (profil/bieg), zakładki, sekcja Wysyłanie, Karty, wykresy, Kalibracja. Nowy panel = nowa sekcja + `<canvas id>` + wpis w JS. |
| `style.css` | Cały wygląd. Kotwice layoutu: `:root` (kolory `--bg-dark`, `--panel-bg`, `--card-bg`, `--accent-*`, `--border`), `main { grid-template-columns: 380px 1fr; max-width: 1600px }` (kolumny), `.metrics-grid` (karty), `.chart-container` (wykresy), `.calib-*` (kalibracja), `@media (max-width: 1000px)` (zwijanie do 1 kolumny). |
| `api-base.js` | Baza API (`localStorage dashApi`, helpery `dashUrl()`/`dashWsUrl()`). Ładowany pierwszy. |
| `app.js` | Ramka WS → nagłówek + karty (mapowanie tick → DOM). |
| `charts.js` | Chart.js: brokery, liderzy, Kingman, ε, tematy. Nowy wykres = nowy `<canvas>` + seria z ticka. |
| `control.js` | Formularz biegów, presety E1/E4/E2, baner eksportu, toast profilu. |
| `calibration.js` | Zakładka Kalibracja: kandydaci, checkboxy, podgląd mediany ± SD/CoV, zapis, przełącznik live/profil. |
| `vite.config.js` | Dev-serwer :3000 + proxy `/api`, `/ws` → `localhost:8080`. |

Zasady: vanilla JS, IIFE, guardy na brak elementów (`if (!el) return`), zero błędów
w konsoli przy pustych danych. Fetch tylko endpoints z README dashboardu (`/api/*`, `/ws`).

## Wariant A — lokalny dev z Vite (polecany do layoutu)

```bash
# 1. Tunel API na localhost:8080 (osobna konsola, trzymaj otwarte):
gcloud compute start-iap-tunnel kafka-load 8080 --local-host-port=8080 --zone=europe-central2-a
# 2. Dev-serwer (druga konsola):
cd thesis-v2/kafka-app/dashboard-front
npm install   # raz
npm run dev   # http://localhost:3000 — HMR/reload przy zapisie, API tunelem
```

Edytujesz, zapisujesz, przeglądarka odświeża sama. Backend (biegi, profile,
Kingman) idzie na żywo z `kafka-load` tunelem. Gdy skończysz: commit na `main` —
haczyk pcha sam frontend na `app-front`, CI robi lint + hot-reload.

## Wariant B — PyCharm Deployment (edycja wprost na serwerze)

Tools → Deployment → Configuration → `+` (SFTP): host = zewnętrzny IP `kafka-load`,
user `researcher`, auth = klucz jak do `gcloud ssh`. Mappings: Local Path
`.../kafka-app/dashboard-front` → Deployment Path `/opt/thesis-frontend`.
Włącz **Automatic Upload**. Efekt: Ctrl+S ląduje na serwerze, odświeżasz stronę.
(Uwaga: zmiany trafiają na serwer, ale do repo wracają tylko przez commit.)

## Wariant C — Chrome Local Overrides (eksperymenty z CSS, zero setupu)

DevTools (F12) → Sources → Overrides → wskaż katalog projektu i nadaj uprawnienia.
Edytujesz style na żywo, Chrome zapisuje do plików lokalnych; potem commit.

## Publikacja (jak to ląduje na serwerze)

- **Zwykle:** commit na `main` → hook → `app-front` → CI (`node --check`, scp do
  `/opt/thesis-frontend/`, weryfikacja pobrania). Bez restartu usługi.
- **Ręcznie w 30 s:** `gcloud compute scp <plik> researcher@kafka-load:/opt/thesis-frontend/ --zone europe-central2-a`.
- Jarowy fallback (`src/main/resources/public` w dashboardzie) buduje
  `deploy_dashboard_v2_build.yml` z kopii tego katalogu — awaryjnie, nie edytuj tam.
