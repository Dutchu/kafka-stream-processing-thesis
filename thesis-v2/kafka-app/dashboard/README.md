# Thesis Dashboard v2 (`thesis-dashboard-2.0.0.jar`)

Java 17 / Javalin 6 dashboard for the thesis-v2 Kafka experiment: end-to-end
consumer, AdminClient-based cluster inspector, `control-metrics` ACK report
aggregator, Kingman (VUT) formula engine, run controller (fan-out to the
producer Cloud Function), Prometheus exporter, and a vanilla-JS frontend.
Implements `spec/spec-dashboard.md` (requirements R1–R9). Consumes the ACK
report / Cloud Function response contract defined in `spec/spec-function.md`
§2–§3.

## Build & test

```bash
cd C:\thesis\thesis-v2\kafka-app\dashboard
mvn -q test
mvn -q -DskipTests package
```

The shaded fat-jar is produced at `target/thesis-dashboard-2.0.0.jar`
(`maven-shade-plugin`, main class `com.thesis.dashboard.DashboardApp`).

## Run

```bash
java -jar target/thesis-dashboard-2.0.0.jar
```

Then open `http://<host>:8080/` (or `http://localhost:8080/` locally).

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BROKERS` | `10.0.0.6:9092,10.0.0.3:9092,10.0.0.4:9092` | Bootstrap servers for both the e2e consumer and the ACK aggregator, and for AdminClient. |
| `FUNCTION_URL` | *(empty)* | Cloud Function HTTP endpoint fanned out to by `POST /api/run`. |
| `CONFIG` | `1b-rf1` | Cluster configuration id (`1b-rf1`\|`3b-rf1`\|`3b-rf3`); selects the active Kingman profile from `PROFILES_FILE`. |
| `DASHBOARD_PORT` | `8080` | Javalin HTTP/WS port. |
| `DB_PATH` | `/var/lib/thesis-dashboard/thesis.db` | Single SQLite file (WAL) holding all runs (`runs`/`series`/`ack` tables). |
| `PROFILES_FILE` | `/opt/thesis-dashboard/profiles.json` | JSON map `config -> Profile` (see `profiles.example.json`). Missing file ⇒ `DEFAULT` profile (`muMsgs=NaN`). |
| `PROM_URL` | *(empty)* | Optional Prometheus base URL (e.g. `http://10.0.0.5:9090`) used only for the end-of-run `saturationSignature`. |
| `TOPICS` | `weather-rain,weather-temp,weather-wind` | Comma-separated topic list tracked by R1/R2 and used as the default fan-out target list. |
| `CONTROL_TOPIC` | `control-metrics` | Topic the ACK aggregator (R3) consumes. |
| `E2E_SAMPLE_EVERY` | `1` | Sample every Nth record for the e2e latency histogram (counting is always exhaustive). |
| `ADMIN_POLL_MS` | `1000` | AdminClient offsets poll period (R1, nominally 1 Hz). |
| `TOPIC_DESCRIBE_MS` | `5000` | AdminClient topology (`describeTopics`/`describeCluster`) poll period (R1). |

## Endpoints (§4)

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/health` | `{"status":"ok","config":…,"brokers":n}` |
| GET | `/api/tick` | Last 1 Hz `Tick` JSON frame (503 until the first tick). |
| POST | `/api/run` | Start a run (R5). Body: see below. Returns `{"runId":…}`. |
| GET | `/api/run/active` | `{"active":bool,"runId":…,"exp":…,"startEpochMs":…,"elapsedSec":…}`. |
| GET | `/api/topics` | Every topic in the cluster (name/partitions/RF/actual message count = LEO−earliest/leaders). On-demand AdminClient trip — seconds, never on 1 Hz. |
| POST | `/api/topics/{name}/purge` | Truncate all messages, keep the topic (refuses `__*`). |
| DELETE | `/api/topics/{name}` | Delete the whole topic (refuses `__*`). |
| POST | `/api/run/{id}/abort` | Marks the run `aborted` (does **not** cancel in-flight Cloud Function invocations — documented limitation, spec-dashboard.md R5). |
| GET | `/api/runs` | List of run ids in SQLite (`runs`), newest first. |
| GET | `/api/runs/{id}/manifest` | Raw manifest JSON (with `summary`). |
| GET | `/api/runs/{id}/summary` | `{"status":…,"summary":{…}}` extracted from the manifest. |
| GET | `/api/runs/{id}/series` | The run's 1 Hz series (JSON array, keys §5). |
| GET | `/api/runs/{id}/ack` | The run's merged ACK buckets (JSON array, `errors` as object). |
| GET/PUT | `/api/profile` | Read/replace the active Kingman profile (PUT persists to `PROFILES_FILE` + library and activates). |
| POST | `/api/profile/from-run/{id}` | Derive `tauAckMs`/`tauE2eMs`/`cs2`/`ca2` from a finished E1 run into the library (no activation); `muMsgs` untouched. |
| GET | `/api/profile/candidates` | Calibration tab: eligible runs per parameter. |
| POST | `/api/profile/from-runs` | Multi-run median ± SD/CoV (min. 3 per parameter); `?dryRun=true` previews; saves named into the library without activating. |
| GET | `/api/profiles` | Profile library + active name (charts-card selector). |
| POST | `/api/profiles?name=X` | Upsert a profile (OMB seeding via curl). |
| DELETE | `/api/profiles/{name}` | Delete from the library (`live` regenerates). |
| POST | `/api/profile/active` | Activate `{"name"}` (must exist, CONFIG must match). |
| GET/POST | `/api/kingman/mode` | c² source: `profile` (frozen) vs `live` (tick estimates). |
| GET | `/metrics` | Prometheus text exposition (`thesis_*` gauges/counters). |
| WS | `/ws` | 1 Hz `Tick` JSON frames (single source of truth for the frontend). |

### `POST /api/run` request body

```json
{
  "exp": "E1|E4|E2|FREE",
  "label": "rho50",
  "parallelism": 20,
  "ratePerSec": 500,
  "durationSec": 120,
  "count": null,
  "topics": ["weather-rain", "weather-temp", "weather-wind"],
  "intensity": 0.9,
  "runIndex": 1
}
```

Validation: `parallelism ∈ [1,500]`; exactly one of `durationSec ∈ [10,540]`
XOR `count` must be set; `ratePerSec ≥ 0`; only one active run at a time.
`runId = <exp>-<CONFIG>-<label>-run<runIndex>-<epochSec>`.

### `POST /api/profile/from-run/{runId}`

Computes, over the run's analysis window (`start+15s` → `stop`):
- `tauAckMs` = median (p50) of the run's whole-run ACK histogram (R3).
- `tauE2eMs` = median of the per-second `e2e_p50_ms` series column.
- `cs2` = `(stddev/mean)²` of the run's whole-run ACK histogram.
- `ca2` = median of the per-second `ca2_est` series column.
- `muMsgs` is **left untouched** — only an explicit `PUT /api/profile` sets it
  (it comes from the E4/W5 pipeline, not from a single E1 run).

The result is persisted as the new active profile with `source=<runId>`.

## Data format (§5)

Single SQLite file (`DB_PATH`, WAL): `runs(run_id, manifest)` — manifest is the
full `RunManifest` JSON incl. `summary`; `series(run_id, ts_ms, …)` — one column
per scalar 1 Hz tick field (names below; NaN = NULL); `ack(…)` — closed buckets,
`errors` as a JSON object. JSON endpoints return the same keys; `null` = NaN.

Series keys:
```
ts_ms,run_id,lambda_leo,lambda_consumer,rho,wq_ms,pred_ack_ms,pred_e2e_ms,ack_mean_ms,ack_p50_ms,ack_p95_ms,ack_p99_ms,ack_max_ms,ack_count,ack_invocations,e2e_mean_ms,e2e_p50_ms,e2e_p99_ms,e2e_max_ms,e2e_count,err_ack_ms,err_e2e_ms,err_ack_rel,err_e2e_rel,consumer_lag,sent,acked,failed,error_types,ca2_est,cs2_est,total_msgs_cluster,bytes_rate
```
Ack keys:
```
window_end_ms,run_id,invocations,sent,acked,failed,bytes,ack_mean_ms,ack_p50_ms,ack_p95_ms,ack_p99_ms,ack_max_ms,errors
```

`run.json` (manifest shape) — see `run.json.example` for a complete manifest (camelCase
fields per R5). In production it lives in the `runs.manifest` column, not in a file.

`profiles.json` — see `profiles.example.json` for the exact shape consumed
by `KingmanEngine` (`PROFILES_FILE`, one entry per `CONFIG`).

## Architecture notes (mapping to spec requirements)

- **R1** `cluster/ClusterInspector` — two independent scheduled pollers: a
  slow topology poller (`TOPIC_DESCRIBE_MS`, `describeTopics`/`describeCluster`)
  and a fast offsets poller (`ADMIN_POLL_MS`, `listOffsets` earliest/latest).
  `lambdaLeo` is the raw first difference of `Σ latest`, normalized by the
  actual elapsed time (no smoothing). Missing brokers are not treated as
  errors; AdminClient exceptions are logged (throttled to 1/10s) and the
  previous snapshot is kept, with `adminErrors` counted.
- **R2** `consumer/E2EConsumer` — `KafkaConsumer<byte[],byte[]>` (no value
  deserialization). Latency = `now - record.timestamp()`. `ca2` is the index
  of dispersion (`var/mean`) of 100 ms arrival-bin counts over a 600-bin
  (60 s) ring buffer, recomputed every second; NaN when the mean bin count is
  below 5. Lag is `Σ(endOffsets - position)` over the current assignment,
  refreshed at most once per second from the consumer thread itself.
- **R3** `control/AckAggregator` — a second, independent
  `KafkaConsumer<String,String>` on `CONTROL_TOPIC`. Reports are bucketed by
  `floor(windowEndMs/1000)`; histograms are merged with `Histogram.add`
  (never percentile-averaged). A bucket closes or evicts once it is more
  than 10 s older than "now"; late reports are dropped and counted. A
  separate whole-run histogram (reset per run) backs the τ/c_s² estimators.
- **R4** `math/KingmanEngine` — per-CONFIG `Profile` loaded from
  `PROFILES_FILE`; `tauAckMs` is used as the mean service time τ in **both**
  the ACK and e2e predictions, per the spec. `rho` is capped at 0.999
  (`rhoEff`) with an `overload` flag when `rho ≥ 1`. A 60-sample rolling MAE
  is kept for both ACK and e2e error. The profile is only ever replaced by
  `PUT /api/profile` or `POST /api/profile/from-run/{id}` — never by the
  per-tick live estimates (`ca2Est`/`cs2Est`), which are computed and shown
  alongside for comparison.
- **R5** `run/RunController` — validates and starts a run, fans out
  `parallelism` async `POST FUNCTION_URL` calls (JDK `HttpClient.sendAsync`,
  timeout `durationSec+180s`), accumulates the run's own `series`/`ack` append
  logs from the 1 Hz loop and the ACK aggregator, and persists
  `run.json`/`series.csv`/`ack.csv` once every invocation has completed (or
  errored). `consumerLimited` is `true` when more than 10% of the analysis
  window's seconds have `lag > 5·λ`. The optional `saturationSignature` is
  computed from `PROM_URL` (`null` when not configured).
- **R6** `export/PrometheusExporter` — hand-built text exposition
  (`text/plain; version=0.0.4`); all `thesis_*` metrics from spec-dashboard.md
  R6, including the per-(broker,topic,role) `thesis_broker_msgs` gauge.
- **R7** One 1 Hz `ScheduledExecutorService` (`DashboardApp`) ties R1–R6
  together into a single `Tick`, which is the sole payload sent over `/ws`
  and returned by `/api/tick`; the frontend has exactly one source of truth.
- **R8** `public/{index.html,style.css,app.js,charts.js,control.js}` —
  vanilla JS, IIFE-wrapped, guarded against malformed frames, an empty
  cluster, or a missing profile/run (matches the defensive style of the
  original codebase). Chart.js is loaded from the jsDelivr CDN. Topic colors
  are fixed: `weather-rain=#3b82f6`, `weather-temp=#f59e0b`,
  `weather-wind=#10b981`. No hex map, no presets.
- **R9** `GET /api/health`, `GET /api/tick`; throttled WARN logging (1/10s)
  for admin/consumer errors; JUnit 5 tests
  (`KingmanEngineTest`, `AckAggregatorTest`, `ClusterSnapshotTest`,
  `CsvFormatTest`).

## Known simplifications / open questions

These were resolved by picking the option closest to the spec text, and are
flagged here rather than left silent:

1. **`ack_count` column (series.csv).** The spec lists `ack_count` and
   `ack_invocations` as separate series.csv columns but does not define
   `ack_count` beyond "matching AckSnapshot fields"; `AckSnapshot` itself has
   no `count` field (only `sent`/`acked`/`failed`). Implemented as
   `ack.acked()` (the number of ACKs actually merged into the closed bucket's
   histogram) — closest reading of "count" as "how many ACKs are in this
   window's histogram".
2. **`saturationSignature` PromQL expressions.** Copied verbatim from
   experiment-plan.md §5.4 / spec-dashboard.md R5; the dashboard does not
   validate that these metric names exist on the target Prometheus — a
   missing metric simply yields `null` for that field (best-effort, per
   spec).
3. **Abort semantics.** `POST /api/run/{id}/abort` only flips the manifest's
   `status` to `aborted` and stops future series/ack appends being folded
   into a "completed" summary; it intentionally does not attempt to cancel
   already-dispatched Cloud Function HTTP calls (spec explicitly says to
   document this rather than implement cancellation).
4. **WebSocket path.** The base codebase used `/metrics` for its WS channel
   and `GET /metrics` for something else entirely; this project reserves
   `GET /metrics` for the Prometheus exporter (R6) and uses `/ws` for the
   push channel (R7), per spec-dashboard.md §4.

## Layout (§6)

```
thesis-v2/kafka-app/dashboard/
  pom.xml  README.md  profiles.example.json  run.json.example
  src/main/java/com/thesis/dashboard/DashboardApp.java
  src/main/java/com/thesis/dashboard/Tick.java
  src/main/java/com/thesis/dashboard/cluster/{ClusterInspector,ClusterSnapshot}.java
  src/main/java/com/thesis/dashboard/consumer/{E2EConsumer,E2ESnapshot}.java
  src/main/java/com/thesis/dashboard/control/{AckAggregator,AckReport,AckSnapshot}.java
  src/main/java/com/thesis/dashboard/math/{KingmanEngine,Profile,KingmanResult}.java
  src/main/java/com/thesis/dashboard/run/{RunController,RunManifest,RunSummary,FunctionClient,PromQueryClient}.java
  src/main/java/com/thesis/dashboard/export/{PrometheusExporter,CsvWriter}.java
  src/main/resources/public/{index.html,style.css,app.js,charts.js,control.js}
  src/test/java/com/thesis/dashboard/{math,control,cluster,export}/*Test.java
```
