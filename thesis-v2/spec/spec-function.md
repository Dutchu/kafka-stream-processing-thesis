# Spec W2 — Cloud Function v2: producent bezserwerowy z pomiarem ACK (thesis-v2)

**Właściciel:** Function agent. **Plan:** `experiment-plan.md` §3, §4, §5.3.
**Kod bazowy (tylko do czytania, kopiować):** `C:\thesis\kafka-app\function\` (`WeatherFunction.java`, `model/WeatherEvent.java`, `strategy/*`, `pom.xml`).
**Cel produktu:** `thesis-v2/kafka-app/function/` — samodzielny projekt Maven; jedna inwokacja HTTP = jeden producent Kafki, który przez `durationSec` (lub `count`) wysyła zdarzenia `WeatherEvent` z zadanym tempem, mierzy opóźnienie ACK każdego rekordu i co 1 s publikuje raport do tematu `control-metrics`.

## 1. Zakres

W: parametryzacja, limiter tempa, pomiar ACK (HdrHistogram), raporty 1 s, odpowiedź HTTP z podsumowaniem, klasyfikacja błędów, Strategy/Factory dla 3 typów zdarzeń (seismic usunięty), testy jednostkowe.
Poza: deploy (W1 `deploy_function_v2.sh`), dashboard (W3), sieć (W1).

## 2. Kontrakt HTTP

`POST <FUNCTION_URL>` JSON:
```json
{ "runId": "E2-3b-rf3-rho50-run1-1725000000", "invocationId": 7,
  "topic": "weather-rain",            // albo "eventType": "RAINFALL|TEMPERATURE|WIND"; topic ma pierwszeństwo
  "zoneId": "zone-007",
  "durationSec": 120,                 // XOR "count": 100000 (jeśli oba: durationSec wygrywa)
  "ratePerSec": 500,                  // 0 = bez limitu
  "intensity": 0.9,
  "reportIntervalMs": 1000 }          // opcjonalne, domyślnie 1000
```
Odpowiedź 200 (po zakończeniu wysyłania i `flush()`):
```json
{ "status":"ok", "runId":"…", "invocationId":7, "topic":"weather-rain",
  "startEpochMs":…, "endEpochMs":…, "sent":60000, "acked":59998, "failed":2,
  "errors": {"TimeoutException":2}, "bytes": 8700000, "avgRecordBytes": 145,
  "ack": {"count":59998,"meanMs":12.4,"p50Ms":9.1,"p95Ms":30.2,"p99Ms":55.0,"maxMs":410.0},
  "achievedRatePerSec": 499.8, "instanceId":"<K_REVISION/hostname>" }
```
Błędy walidacji → 400 z `{"status":"error","message":…}`. `OPTIONS` → 204 + CORS (`Access-Control-Allow-Origin: *`).

## 3. Kontrakt raportu `control-metrics` (klucz = `runId`, wartość = JSON, co `reportIntervalMs` oraz raport końcowy z `"final":true`)
```json
{ "runId":"…", "invocationId":7, "topic":"weather-rain", "windowStartMs":…, "windowEndMs":…,
  "sent":501, "acked":499, "failed":0, "errors":{}, "bytes":72645,
  "ackMeanMs":11.9, "ackP50Ms":9.0, "ackP99Ms":48.2, "ackMaxMs":120.0,
  "hdrBase64":"HISTFAAAA…",   // HdrHistogram compressed encoding okna (ostatniej sekundy)
  "final":false }
```
`hdrBase64` = `Histogram.encodeIntoCompressedByteBuffer` → Base64; parametry histogramu: `lowestDiscernibleValue=1` (µs), `highestTrackableValue=60_000_000` (60 s), `numberOfSignificantValueDigits=3`. Histogram okna resetowany po raporcie; histogram całościowy utrzymywany osobno dla odpowiedzi HTTP.

## 4. Wymagania implementacyjne

- **R1 Producent:** `KafkaProducer<String,String>` tworzony raz per inwokacja; props: `bootstrap.servers=$KAFKA_BROKERS`, `acks=$KAFKA_ACKS` (domyślnie `all`), `linger.ms=5`, `batch.size=16384`, `enable.idempotence=true` (domyślne dla acks=all), `max.block.ms=60000`, `delivery.timeout.ms=120000`, `client.id=fn-<runId>-<invocationId>`, `compression.type=none`. Rekord: klucz = `zoneId`, wartość = JSON `WeatherEvent` (pola jak w oryginale: zoneId, eventType, value, unit, intensity, timestamp=ms). **Nie ustawiać jawnie timestampu rekordu** — producent nadaje CreateTime, konsument liczy e2e z `record.timestamp()`.
- **R2 Limiter tempa:** token bucket / równomierne odstępy `1e9/ratePerSec` ns, `LockSupport.parkNanos` dla odstępów > 2 ms, spin/yield poniżej; `ratePerSec=0` → brak limitu (pętla `send()` blokuje się na buforze — to pożądany backpressure). Klasa `RateLimiter` z testem (osiągnięte tempo w ±3 % dla 100 i 5000 msg/s w 2 s).
- **R3 Pomiar ACK:** `long t0 = System.nanoTime()` przed `send()`; w callbacku `recordValue((nanoTime−t0)/1000)` do histogramu okna **i** całościowego (synchronizacja: `SynchronizedHistogram` lub `Recorder`); błędy klasyfikowane po `exception.getClass().getSimpleName()` w `ConcurrentHashMap<String,LongAdder>`.
- **R4 Raporter:** osobny wątek `ScheduledExecutorService` (daemon) co `reportIntervalMs` buduje raport §3 i wysyła `ProducerRecord("control-metrics", runId, json)` **tym samym producentem** (acks jak dane; raporty są małe). Po zakończeniu: `flush()`, raport `final:true`, `shutdown` executora, `producer.close(Duration.ofSeconds(30))`.
- **R5 Strategy/Factory:** `EventStrategy` (`topicName()`, `generate(zoneId,intensity)`), implementacje `RainfallStrategy`, `TemperatureStrategy`, `WindStrategy`; `EventStrategyFactory.byTopic(String)` i `.byEventType(String)`. Mapowanie: `weather-rain↔RAINFALL`, `weather-temp↔TEMPERATURE`, `weather-wind↔WIND`.
- **R6 Konfiguracja env:** `KAFKA_BROKERS`, `KAFKA_ACKS`, `CONTROL_TOPIC` (domyślnie `control-metrics`), `MAX_DURATION_SEC` (domyślnie 540 — poniżej timeoutu 600 s).
- **R7 Logowanie:** jeden wiersz INFO na start (parametry) i koniec (podsumowanie), WARN na każdy nowy typ błędu (pierwsze wystąpienie), bez logowania per rekord.
- **R8 Zależności:** `org.hdrhistogram:HdrHistogram:2.2.2`, Kafka clients 3.7.x, Jackson, Functions Framework jak w oryginale; Java 21. `pom.xml` buduje fat-jar tak jak oryginał (Cloud Functions buduje ze źródeł, więc wystarczy standardowy `pom.xml`).
- **R9 Testy (JUnit 5):** `RateLimiterTest`, `ReportSerializationTest` (round-trip JSON + dekodowanie `hdrBase64` i zgodność p50 z oryginałem), `EventStrategyFactoryTest`. Bez testów integracyjnych z brokerem.

## 5. Układ plików

```
thesis-v2/kafka-app/function/
  pom.xml
  src/main/java/com/thesis/kafka/WeatherFunction.java           # HTTP entry, walidacja, orkiestracja
  src/main/java/com/thesis/kafka/producer/LoadRun.java          # pętla wysyłania, limiter, histogramy
  src/main/java/com/thesis/kafka/producer/RateLimiter.java
  src/main/java/com/thesis/kafka/producer/AckReporter.java      # raporty 1 s → control-metrics
  src/main/java/com/thesis/kafka/model/WeatherEvent.java
  src/main/java/com/thesis/kafka/model/AckReport.java           # POJO kontraktu §3
  src/main/java/com/thesis/kafka/model/RunSummary.java          # POJO odpowiedzi §2
  src/main/java/com/thesis/kafka/strategy/{EventStrategy,EventStrategyFactory,RainfallStrategy,TemperatureStrategy,WindStrategy}.java
  src/test/java/... (R9)
  README.md   # kontrakty §2–§3, zmienne env, lokalne uruchomienie (functions-framework), przykładowy curl
```

## 6. Kryteria akceptacji

- `mvn -q -DskipTests package` i `mvn -q test` bez błędów (autor uruchomi; worker podaje komendy).
- Kontrakty §2/§3 zaimplementowane co do nazwy pola; `hdrBase64` dekodowalny przez `Histogram.decodeFromCompressedByteBuffer`.
- Brak parsowania/ustawiania timestampu rekordu; brak `Thread.sleep` w pętli wysyłania (tylko `parkNanos`).
- Rozmiar rekordu (`avgRecordBytes`) raportowany z rzeczywistych `RecordMetadata.serializedValueSize()+serializedKeySize()`.
- README opisuje, jak zmierzyć lokalnie `avgRecordBytes` (oczekiwane ~130–160 B).

## 7. Prompt dla workera (samowystarczalny)

> Jesteś programistą Java. Utwórz projekt `C:\thesis\thesis-v2\kafka-app\function\` dokładnie wg `C:\thesis\thesis-v2\spec\spec-function.md` (ten dokument). Kod bazowy do skopiowania i przerobienia: `C:\thesis\kafka-app\function\` — **nie modyfikuj oryginału**. Nie masz shella; napisz kod tak, by kompilował się bez prób, i podaj autorowi komendy weryfikacyjne (`mvn -q test` w katalogu projektu). Kontrakty JSON §2–§3 są wiążące (czyta je dashboard z `spec-dashboard.md`). Nie dodawaj funkcji poza spec (żadnych map hex, presetów, seismic). Używaj neutralnej terminologii (bieg, obciążenie, saturacja). Raport końcowy: lista plików, decyzje implementacyjne, komendy do weryfikacji, otwarte pytania.
