# Badanie wydajności systemu Apache Kafka

Kod systemu wykorzystanego w pracy inżynierskiej: generowanie obciążenia,
odbiór komunikatów, pomiar opóźnień i analiza wyników eksperymentów.
Repozytorium zawiera wyłącznie końcową implementację V2 w `thesis-v2/`.
Ścieżki zachowano, aby odpowiadały konfiguracji CI/CD i skryptom wdrożeniowym.

Repozytorium jest niezależną migawką źródeł. Nie zawiera historii prywatnego
repozytorium badawczego ani wyników eksperymentów. Dane i pliki Python do
ich analizy są dostarczane oddzielnie jako `thesis-results.zip`.

## Struktura

| Katalog | Zawartość |
| --- | --- |
| `thesis-v2/kafka-app/dashboard/` | Aplikacja Java: sterowanie biegami, konsument, pomiary i API |
| `thesis-v2/kafka-app/dashboard-front/` | Interfejs przeglądarkowy |
| `thesis-v2/kafka-app/function/` | Producent komunikatów uruchamiany jako funkcja chmurowa |
| `thesis-v2/analysis/` | Analiza pomiarów i generowanie wykresów |
| `thesis-v2/terraform/`, `thesis-v2/ansible/`, `thesis-v2/gcloud/` | Definicje infrastruktury i skrypty utrzymania |
| `thesis-v2/monitoring_node/` | Konfiguracje monitorowania |
| `thesis-v2/spec/`, `thesis-v2/requirements/` | Specyfikacje i wymagania |
| `docs/` | Plan eksperymentów i instrukcje techniczne |
| `.github/workflows/` | Definicje testowania i wdrażania |

## Budowanie i testowanie

Dashboard wymaga JDK 17 i Maven:

```sh
cd thesis-v2/kafka-app/dashboard
mvn test
mvn package
```

Producent wymaga JDK 21 i Maven. Szczegóły budowania i konfiguracji znajdują
się w `thesis-v2/kafka-app/function/README.md`. Instrukcje frontendu znajdują
się w `thesis-v2/kafka-app/dashboard-front/README.md`.

Analiza wymaga Python 3.12; zależności określa
`thesis-v2/analysis/requirements.txt`. Aby odtworzyć wyniki pracy, należy
rozpakować osobno dostarczone archiwum i uruchomić jego `run_analysis.py`.
Samo repozytorium nie zawiera danych do pełnej analizy.

## Konfiguracja własnego środowiska

Adresy publiczne, dane użytkowników i identyfikatory projektu zastąpiono
oznaczeniami przykładowymi. Wartości `example-project`, `example-owner`,
`researcher`, domeny `example.invalid` i adresy `192.0.2.x` należy zastąpić
wartościami własnego środowiska. Prywatne adresy sieciowe opisują przykładową
topologię eksperymentalną.

Pliki stanu Terraform i klucze kont usługowych nie należą do repozytorium.
Nie należy ich dodawać do Git. Skrypty wdrożeniowe wymagają samodzielnego
skonfigurowania dostępu do chmury.

Istniejące definicje GitHub Actions obsługują gałęzie wydawnicze `app-dist`
i `app-front`. Nowe repozytorium startuje wyłącznie z gałęzią `main`.
Wdrożenie wymaga utworzenia odpowiednich gałęzi i skonfigurowania sekretu
`GCP_SA_KEY` oraz zmiennych opisanych w plikach workflow.

Historyczne instrukcje zachowują kontekst eksperymentów; względne ścieżki
w przeniesionych dokumentach odnoszą się do pierwotnego układu projektu.
Dokumenty z katalogu `docs/` były wcześniej w katalogu `thesis-v2/`.
Odwołania do V1 w analizie i dokumentacji opisują wcześniejsze pomiary użyte
do porównań w pracy. Kod aplikacji V1 nie jest częścią tego repozytorium;
dane historyczne potrzebne do analizy znajdują się w oddzielnym archiwum.
