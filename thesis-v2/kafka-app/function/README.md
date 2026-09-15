# Thesis-v2 — Cloud Function producent bezserwerowy (W2)

Samodzielny projekt Maven implementujący **jedną inwokację HTTP = jeden producent Kafki**:
funkcja wysyła zdarzenia `WeatherEvent` przez `durationSec` (albo `count`) ze skonfigurowanym
tempem, mierzy opóźnienie ACK każdego rekordu (HdrHistogram) i co `reportIntervalMs`
publikuje raport do tematu `control-metrics`. Po zakończeniu zwraca podsumowanie JSON.

Zgodne ze specyfikacją: `C:\thesis\thesis-v2\spec\spec-function.md`.

## 1. Zmienne środowiskowe

| Zmienna | Domyślna | Znaczenie |
| --- | --- | --- |
| `KAFKA_BROKERS` | `10.0.0.6:9092,10.0.0.3:9092,10.0.0.4:9092` | lista `bootstrap.servers` |
| `KAFKA_ACKS` | `all` | `acks` producenta (`0`\|`1`\|`all`) |
| `CONTROL_TOPIC` | `control-metrics` | temat raportów sterujących |
| `MAX_DURATION_SEC` | `540` | górny limit `durationSec` (margines poniżej timeoutu funkcji 600 s) |

## 2. Kontrakt HTTP (§2 spec-function.md)

### Żądanie — `POST <FUNCTION_URL>`

```json
{ "runId": "E2-3b-rf3-rho50-run1-1725000000", "invocationId": 7,
  "topic": "weather-rain",
  "zoneId": "zone-007",
  "durationSec": 120,
  "ratePerSec": 500,
  "intensity": 0.9,
  "reportIntervalMs": 1000 }
```

Zasady:
- `topic` **albo** `eventType` (`RAINFALL|TEMPERATURE|WIND`); jeśli oba obecne, `topic` ma pierwszeństwo.
- `durationSec` **XOR** `count`; jeśli oba obecne, `durationSec` wygrywa.
- `ratePerSec = 0` → brak limitu tempa (backpressure bufora producenta).
- `reportIntervalMs` opcjonalne, domyślnie `1000`.
- Pola wymagane: `runId`, `invocationId`, `zoneId`, (`topic` lub `eventType`), (`durationSec` lub `count`).

### Odpowiedź 200 (po zakończeniu wysyłania i `flush()`)

```json
{ "status":"ok", "runId":"…", "invocationId":7, "topic":"weather-rain",
  "startEpochMs":…, "endEpochMs":…, "sent":60000, "acked":59998, "failed":2,
  "errors": {"TimeoutException":2}, "bytes": 8700000, "avgRecordBytes": 145,
  "ack": {"count":59998,"meanMs":12.4,"p50Ms":9.1,"p95Ms":30.2,"p99Ms":55.0,"maxMs":410.0},
  "achievedRatePerSec": 499.8, "instanceId":"<K_REVISION/hostname>" }
```

- Błędy walidacji → `400` z `{"status":"error","message":"…"}`.
- `OPTIONS` → `204` + `Access-Control-Allow-Origin: *`.

## 3. Kontrakt raportu `control-metrics` (§3 spec-function.md)

Klucz rekordu = `runId`; wartość = JSON poniżej; publikowany co `reportIntervalMs`
**i** jako raport końcowy z `"final":true` po zakończeniu wysyłania:

```json
{ "runId":"…", "invocationId":7, "topic":"weather-rain", "windowStartMs":…, "windowEndMs":…,
  "sent":501, "acked":499, "failed":0, "errors":{}, "bytes":72645,
  "ackMeanMs":11.9, "ackP50Ms":9.0, "ackP99Ms":48.2, "ackMaxMs":120.0,
  "hdrBase64":"HISTFAAAA…",
  "final":false }
```

`hdrBase64` = `Histogram.encodeIntoCompressedByteBuffer` (okno ostatniego interwału) → Base64.
Parametry histogramu (okno **i** całość — dwa oddzielne obiekty):
`lowestDiscernibleValue=1` µs, `highestTrackableValue=60_000_000` µs (60 s),
`numberOfSignificantValueDigits=3`. Histogram okna jest resetowany po każdym raporcie;
histogram całościowy (do odpowiedzi HTTP) żyje przez cały czas trwania inwokacji.

Dekodowanie po stronie dashboardu/testów:
```java
byte[] bytes = Base64.getDecoder().decode(hdrBase64);
Histogram h = Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(bytes), 0);
```

## 4. Architektura kodu

```
src/main/java/com/thesis/kafka/
  WeatherFunction.java           # HTTP entry point: walidacja, budowa RunParams, orkiestracja
  producer/
    LoadRun.java                 # pętla wysyłania, histogram całościowy, podsumowanie HTTP
    RateLimiter.java             # limiter tempa (parkNanos + spin), R2
    AckReporter.java             # histogram okna + raporty control-metrics co 1 s, R4
    RunParams.java               # zwalidowane parametry jednego biegu
  model/
    WeatherEvent.java            # payload zdarzenia (zoneId, eventType, value, unit, intensity, timestamp)
    AckReport.java                # POJO kontraktu §3 (control-metrics)
    RunSummary.java               # POJO kontraktu §2 (odpowiedź HTTP)
  strategy/
    EventStrategy.java            # topicName(), eventType(), generate(zoneId, intensity)
    EventStrategyFactory.java     # byTopic(String) / byEventType(String)
    RainfallStrategy.java          # weather-rain <-> RAINFALL
    TemperatureStrategy.java       # weather-temp <-> TEMPERATURE
    WindStrategy.java              # weather-wind <-> WIND
```

Wzorce: **Strategy + Factory** dla generowania zdarzeń (3 typy; seismic usunięty względem
kodu bazowego v1). Jeden `KafkaProducer<String,String>` na inwokację, współdzielony przez
pętlę wysyłania danych i reporter (ten sam producent wysyła dane i raporty — R4).

## 5. Kluczowe decyzje implementacyjne

- **Nie ustawiamy jawnie timestampu rekordu** — producent nadaje `CreateTime`; konsument
  (workstream W3) liczy e2e z `record.timestamp()`.
- **Pomiar ACK**: `long t0 = System.nanoTime()` tuż przed `send()`; w callbacku
  `(nanoTime() - t0) / 1000` (mikrosekundy) trafia jednocześnie do histogramu okna
  (`AckReporter`) i histogramu całościowego (`LoadRun`), zsynchronizowanych przez
  `synchronized` (bloki krótkie, brak kontencji w praktyce dla jednowątkowej pętli wysyłającej
  i osobnego wątku raportującego).
- **Klasyfikacja błędów** wyłącznie po `exception.getClass().getSimpleName()`, gromadzona w
  `ConcurrentHashMap<String, LongAdder>` (bez trwałego mapowania na kody — spec nie tego
  wymaga).
- **Limiter tempa**: `RateLimiter` planuje kolejne "tiknięcia" względem stałego kroku
  `1e9/ratePerSec` ns od poprzedniego zaplanowanego momentu (nie od "teraz"), by uniknąć
  systematycznego driftu; różnice > 2 ms czekane przez `LockSupport.parkNanos`, reszta
  domykana pętlą `Thread.onSpinWait()`. `ratePerSec <= 0` wyłącza limiter całkowicie —
  jedynym hamulcem jest wtedy bufor producenta (`send()` blokuje się do `max.block.ms`).
- **`avgRecordBytes`** liczone z rzeczywistych `RecordMetadata.serializedKeySize() +
  serializedValueSize()` zsumowanych po wszystkich potwierdzonych (`acked`) rekordach,
  podzielonych przez `acked` (nie estymowane z JSON).
- **Zakończenie biegu**: `producer.flush()` → raport końcowy `final:true` → zatrzymanie
  executora reportera → `producer.flush()` ponownie → `producer.close(Duration.ofSeconds(30))`.
- **`durationSec` vs `count`**: jeśli oba obecne w żądaniu, `durationSec` wygrywa (drugie pole
  jest ignorowane przy budowie `RunParams`, zgodnie ze spec).
- **Walidacja `MAX_DURATION_SEC`**: żądanie z `durationSec` przekraczającym limit env
  kończy się `400`, aby uniknąć przekroczenia timeoutu funkcji (600 s w Cloud Run gen2).
- Brak `Thread.sleep` w pętli wysyłającej — wyłącznie `LockSupport.parkNanos` (i spin poniżej
  progu 2 ms), zgodnie z wymaganiem R2/kryteriami akceptacji.

## 6. Lokalne uruchomienie (Functions Framework)

Wymaga lokalnego lub tunelowanego dostępu do klastra Kafka (`KAFKA_BROKERS`).

```bash
cd C:\thesis\thesis-v2\kafka-app\function

# kompilacja
mvn -q -DskipTests package

# uruchomienie lokalne przez functions-framework-maven-plugin
# (dodaj plugin do pom.xml lub użyj funkcji Google Cloud Functions Framework z linii poleceń,
#  jeśli zainstalowany globalnie: `mvn function:run`)
KAFKA_BROKERS=10.0.0.6:9092,10.0.0.3:9092,10.0.0.4:9092 \
KAFKA_ACKS=all \
CONTROL_TOPIC=control-metrics \
mvn function:run -Drun.functionTarget=com.thesis.kafka.WeatherFunction
```

Domyślnie functions-framework nasłuchuje na `http://localhost:8080`.

### Przykładowy `curl`

```bash
curl -s -X POST http://localhost:8080 \
  -H 'Content-Type: application/json' \
  -d '{
        "runId": "smoke-test-1",
        "invocationId": 1,
        "topic": "weather-rain",
        "zoneId": "zone-007",
        "durationSec": 10,
        "ratePerSec": 200,
        "intensity": 0.7,
        "reportIntervalMs": 1000
      }' | jq .
```

Oczekiwana odpowiedź: `status: "ok"`, `sent`/`acked` bliskie `10 s * 200 msg/s = 2000`,
`avgRecordBytes` w zakresie **~130–160 B** dla payloadu `WeatherEvent` w formacie JSON
(pola: `zoneId`, `eventType`, `value`, `unit`, `intensity`, `timestamp`). Aby zmierzyć
`avgRecordBytes` lokalnie, uruchom powyższy `curl` i odczytaj pole `avgRecordBytes`
z odpowiedzi — jest ono liczone z rzeczywistych rozmiarów zserializowanych rekordów
zwróconych przez brokera (`RecordMetadata`), nie z szacunku offline.

Do podglądu raportów `control-metrics` w trakcie biegu:
```bash
kafka-console-consumer.sh --bootstrap-server 10.0.0.6:9092 \
  --topic control-metrics --property print.key=true \
  --from-beginning
```

## 7. Testy jednostkowe (JUnit 5)

```bash
cd C:\thesis\thesis-v2\kafka-app\function
mvn -q test
```

- `RateLimiterTest` — sprawdza osiągnięte tempo w ±3% dla 100 i 5000 msg/s (okno ~2 s) oraz
  że `ratePerSec <= 0` wyłącza limiter.
- `ReportSerializationTest` — round-trip JSON `AckReport`/`RunSummary` (nazwy pól zgodne z
  §2/§3), dekodowanie `hdrBase64` przez `Histogram.decodeFromCompressedByteBuffer` i
  zgodność percentyli (p50/p99/max) z histogramem źródłowym.
- `EventStrategyFactoryTest` — mapowanie `weather-rain↔RAINFALL`,
  `weather-temp↔TEMPERATURE`, `weather-wind↔WIND`; odrzucenie `SEISMIC`/tematów nieznanych.

Brak testów integracyjnych z prawdziwym brokerem (poza zakresem W2; zob. spec §4 R9).

## 8. Weryfikacja pełnego builda

```bash
cd C:\thesis\thesis-v2\kafka-app\function
mvn -q test
mvn -q -DskipTests package
```

Oczekiwany artefakt: `target/weather-producer-function-v2-2.0.0.jar` (fat-jar przez
`maven-shade-plugin`, jak w kodzie bazowym v1).
