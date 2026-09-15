# Wymagania systemu (thesis-v2)

Artefakt inżynierii wymagań pracy *Rozproszony system przetwarzania danych zbudowany przy użyciu
Apache Kafka*. Jedno źródło prawdy to plik `requirements.csv` (czytany przez manuskrypt przez
`csvsimple`); ten plik opisuje konwencje.

## Konwencje

- **Identyfikatory:** `FR-nn` — wymaganie funkcjonalne, `NFR-nn` — niefunkcjonalne (atrybut jakości
  lub ograniczenie), `AC-nn` — kryterium akceptacyjne przypisane do wymagania `FR/NFR-nn`.
- **Interesariusze:** autor-operator (uruchamia biegi, czyta dashboard), promotor/recenzent
  (ocenia reprodukowalność i dowody), "przyszły utrzymujący" (odtwarza środowisko z kodu).
- **Komponent:** moduł, który realizuje wymaganie (klasa Java, skrypt, playbook, dashboard Grafany).
- **Weryfikacja:** `unit` (test jednostkowy w CI), `e2e` (bieg na klastrze z artefaktami
  w `results/`), `review` (przegląd kodu/konfiguracji), `measurement` (tabela w `paper/tables/`).
- **Status:** `done` — spełnione i zweryfikowane; `partial` — zrealizowane częściowo (opis w kolumnie
  `notes`); `dropped` — świadomie porzucone z uzasadnieniem.

## Kolumny `requirements.csv`

`id, type, name, statement, component, verification, evidence, status, notes`

- `statement` — jedno zdanie w trybie "system MUSI / POWINIEN".
- `evidence` — ścieżka do dowodu w repozytorium (klasa testowa, katalog wyników, tabela, raport).

## Pochodzenie

Wymagania spisano z decyzji autora utrwalonych w `experiment-plan.md` (§1, §7, §8),
`spec/spec-dashboard.md`, `spec/spec-function.md`, `spec/spec-monitoring.md`, `spec/spec-infra.md`
oraz w raportach `reports/2026-09-*.md`; statusy odpowiadają stanowi na koniec kampanii
pomiarowej (2026-09-12).
