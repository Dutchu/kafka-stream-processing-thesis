package com.thesis.dashboard.run;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.thesis.dashboard.Tick;
import com.thesis.dashboard.control.AckAggregator;
import com.thesis.dashboard.control.AckSnapshot;
import com.thesis.dashboard.math.KingmanEngine;
import com.thesis.dashboard.math.Profile;
import com.thesis.dashboard.store.RunStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunController implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(RunController.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final RunStore store;
    private final String functionUrl;
    private final String config;
    private final List<String> defaultTopics;
    private final AckAggregator ackAggregator;
    private final KingmanEngine kingmanEngine;
    private final FunctionClient functionClient = new FunctionClient();
    private final PromQueryClient promQueryClient;

    private final AtomicReference<ActiveRun> activeRun = new AtomicReference<>(null);

    public RunController(String dbPath, String functionUrl, String config, List<String> defaultTopics,
                          AckAggregator ackAggregator, KingmanEngine kingmanEngine, String promUrl) {
        this(new RunStore(dbPath), functionUrl, config, defaultTopics, ackAggregator, kingmanEngine, promUrl);
    }

    public RunController(RunStore store, String functionUrl, String config, List<String> defaultTopics,
                          AckAggregator ackAggregator, KingmanEngine kingmanEngine, String promUrl) {
        this.store = store;
        this.functionUrl = functionUrl;
        this.config = config;
        this.defaultTopics = defaultTopics;
        this.ackAggregator = ackAggregator;
        this.kingmanEngine = kingmanEngine;
        this.promQueryClient = new PromQueryClient(promUrl);
    }

    public RunStore store() {
        return store;
    }

    private static final class ActiveRun {
        final RunManifest manifest;
        final List<Tick> seriesTicks = new CopyOnWriteArrayList<>();
        final List<AckSnapshot> ackSnaps = new CopyOnWriteArrayList<>();
        volatile boolean aborted = false;
        volatile boolean finished = false;
        final List<CompletableFuture<FunctionClient.InvocationResult>> pending = new CopyOnWriteArrayList<>();

        ActiveRun(RunManifest manifest) {
            this.manifest = manifest;
        }
    }

    public static final class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }

    public static class RunRequest {
        public String exp;
        public String config;
        public String label;
        public int parallelism;
        public Double ratePerSec;
        public Integer durationSec;
        public Long count;
        public List<String> topics;
        public double intensity = 0.5;
        public int runIndex = 1;
    }

    public synchronized String startRun(RunRequest req) {
        if (activeRun.get() != null && !activeRun.get().finished) {
            throw new ValidationException("a run is already active: " + activeRun.get().manifest.runId);
        }
        validate(req);

        List<String> topics = (req.topics != null && !req.topics.isEmpty()) ? req.topics : defaultTopics;
        long startEpochMs = System.currentTimeMillis();
        String runId = req.exp + "-" + config + "-" + req.label + "-run" + req.runIndex + "-" + (startEpochMs / 1000);

        RunManifest manifest = new RunManifest();
        manifest.runId = runId;
        manifest.exp = req.exp;
        manifest.label = req.label;
        manifest.config = config;
        manifest.parallelism = req.parallelism;
        manifest.ratePerSec = req.ratePerSec == null ? 0.0 : req.ratePerSec;
        manifest.durationSec = req.durationSec;
        manifest.count = req.count;
        manifest.topics = topics;
        manifest.intensity = req.intensity;
        manifest.runIndex = req.runIndex;
        manifest.startEpochMs = startEpochMs;
        manifest.functionUrl = functionUrl;
        manifest.profileAtStart = kingmanEngine.activeProfile();
        manifest.status = "running";

        ActiveRun run = new ActiveRun(manifest);
        activeRun.set(run);

        ackAggregator.resetRun();

        for (int i = 1; i <= req.parallelism; i++) {
            String topic = topics.get((i - 1) % topics.size());
            String zoneId = String.format("zone-%03d", i);
            RunManifest.Invocation inv = new RunManifest.Invocation();
            inv.invocationId = i;
            inv.zoneId = zoneId;
            inv.topic = topic;
            manifest.invocations.add(inv);

            CompletableFuture<FunctionClient.InvocationResult> future = functionClient.invoke(
                    functionUrl, runId, i, topic, zoneId, req.durationSec, req.count,
                    manifest.ratePerSec, req.intensity);
            run.pending.add(future);
            future.thenAccept(result -> onInvocationComplete(run, result));
        }

        LOG.info(() -> "Run started: " + runId + " parallelism=" + req.parallelism
                + " exp=" + req.exp + " config=" + config);

        CompletableFuture.allOf(run.pending.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> finalizeRun(run));

        return runId;
    }

    private void validate(RunRequest req) {
        if (req.exp == null || !(req.exp.equals("E1") || req.exp.equals("E4") || req.exp.equals("E2") || req.exp.equals("FREE"))) {
            throw new ValidationException("exp must be one of E1|E4|E2|FREE");
        }
        if (req.config != null && !req.config.equals(config)) {
            throw new ValidationException(
                    "config mismatch: run asks for " + req.config + " but dashboard serves " + config);
        }
        if (req.label == null || req.label.isBlank()) {
            throw new ValidationException("label is required");
        }
        if (req.parallelism < 1 || req.parallelism > 500) {
            throw new ValidationException("parallelism must be in [1,500]");
        }
        boolean hasDuration = req.durationSec != null;
        boolean hasCount = req.count != null;
        if (hasDuration == hasCount) {
            throw new ValidationException("exactly one of durationSec or count must be set");
        }
        if (hasDuration && (req.durationSec < 10 || req.durationSec > 540)) {
            throw new ValidationException("durationSec must be in [10,540]");
        }
        if (req.ratePerSec != null && req.ratePerSec < 0) {
            throw new ValidationException("ratePerSec must be >= 0");
        }
    }

    private void onInvocationComplete(ActiveRun run, FunctionClient.InvocationResult result) {
        RunManifest.Invocation inv = run.manifest.invocations.stream()
                .filter(i -> i.invocationId == result.invocationId())
                .findFirst().orElse(null);
        if (inv == null) return;
        inv.ok = result.ok();
        inv.httpStatus = result.httpStatus();
        inv.error = result.error();
        if (result.body() != null) {
            var body = result.body();
            inv.startEpochMs = body.path("startEpochMs").isMissingNode() ? null : body.path("startEpochMs").asLong();
            inv.endEpochMs = body.path("endEpochMs").isMissingNode() ? null : body.path("endEpochMs").asLong();
            inv.sent = body.path("sent").isMissingNode() ? null : body.path("sent").asLong();
            inv.acked = body.path("acked").isMissingNode() ? null : body.path("acked").asLong();
            inv.failed = body.path("failed").isMissingNode() ? null : body.path("failed").asLong();
            inv.achievedRatePerSec = body.path("achievedRatePerSec").isMissingNode() ? null : body.path("achievedRatePerSec").asDouble();
            inv.instanceId = body.path("instanceId").isMissingNode() ? null : body.path("instanceId").asText();
        }
    }

    private void finalizeRun(ActiveRun run) {
        long stopEpochMs = 0L;
        for (RunManifest.Invocation inv : run.manifest.invocations) {
            if (inv.endEpochMs != null) stopEpochMs = Math.max(stopEpochMs, inv.endEpochMs);
        }
        if (stopEpochMs == 0L) stopEpochMs = System.currentTimeMillis();
        run.manifest.stopEpochMs = stopEpochMs;
        run.manifest.analysisStartMs = run.manifest.startEpochMs + 15_000L;

        RunSummary summary = computeSummary(run);
        run.manifest.summary = summary;
        if (!run.aborted) {
            run.manifest.status = "completed";
        }
        run.finished = true;

        persist(run);
        LOG.info(() -> "Run finished: " + run.manifest.runId + " status=" + run.manifest.status
                + " series=" + run.seriesTicks.size() + " ack=" + run.ackSnaps.size());
    }

    private RunSummary computeSummary(ActiveRun run) {
        RunSummary summary = new RunSummary();
        long analysisStart = run.manifest.analysisStartMs;
        long stop = run.manifest.stopEpochMs;

        long lagMax = 0;
        int secondsWithHighLag = 0;
        int totalSeconds = 0;
        double lambdaSum = 0; int lambdaN = 0;
        List<Double> lambdaValues = new ArrayList<>();
        double rhoSum = 0; int rhoN = 0;
        double predAckSum = 0; int predAckN = 0;
        double predE2eSum = 0; int predE2eN = 0;
        double errAckSum = 0; int errAckN = 0;
        double errE2eSum = 0; int errE2eN = 0;
        double e2eMeanSum = 0; int e2eMeanN = 0;
        List<Double> e2eP50s = new ArrayList<>();
        double e2eP99Sum = 0; int e2eP99N = 0;

        for (Tick t : run.seriesTicks) {
            if (t == null) continue;
            long tsMs = t.tsMs();
            if (tsMs < analysisStart || tsMs > stop) continue;
            totalSeconds++;
            double lambda = t.lambdaLeo();
            long lag = t.consumerLag();

            if (Double.isFinite(lambda)) { lambdaSum += lambda; lambdaN++; lambdaValues.add(lambda); }
            if (Double.isFinite(t.rho())) { rhoSum += t.rho(); rhoN++; }
            if (Double.isFinite(t.predAckMs())) { predAckSum += t.predAckMs(); predAckN++; }
            if (Double.isFinite(t.predE2eMs())) { predE2eSum += t.predE2eMs(); predE2eN++; }
            if (Double.isFinite(t.errAckMs())) { errAckSum += t.errAckMs(); errAckN++; }
            if (Double.isFinite(t.errE2eMs())) { errE2eSum += t.errE2eMs(); errE2eN++; }
            if (Double.isFinite(t.e2eMeanMs())) { e2eMeanSum += t.e2eMeanMs(); e2eMeanN++; }
            if (Double.isFinite(t.e2eP50Ms())) e2eP50s.add(t.e2eP50Ms());
            if (Double.isFinite(t.e2eP99Ms())) { e2eP99Sum += t.e2eP99Ms(); e2eP99N++; }
            lagMax = Math.max(lagMax, lag);
            if (Double.isFinite(lambda) && lambda > 0 && lag > 5 * lambda) {
                secondsWithHighLag++;
            }
        }

        summary.lambdaMean = lambdaN > 0 ? lambdaSum / lambdaN : Double.NaN;
        summary.lambdaP50 = median(lambdaValues);
        summary.rhoMean = rhoN > 0 ? rhoSum / rhoN : Double.NaN;
        summary.predAck = predAckN > 0 ? predAckSum / predAckN : Double.NaN;
        summary.predE2e = predE2eN > 0 ? predE2eSum / predE2eN : Double.NaN;
        summary.errAck = errAckN > 0 ? errAckSum / errAckN : Double.NaN;
        summary.errE2e = errE2eN > 0 ? errE2eSum / errE2eN : Double.NaN;
        summary.consumerLagMax = lagMax;
        summary.consumerLimited = totalSeconds > 0 && ((double) secondsWithHighLag / totalSeconds) > 0.10;
        summary.e2eMean = e2eMeanN > 0 ? e2eMeanSum / e2eMeanN : Double.NaN;
        summary.e2eP50 = median(e2eP50s);
        summary.e2eP99 = e2eP99N > 0 ? e2eP99Sum / e2eP99N : Double.NaN;

        long sentTotal = 0, ackedTotal = 0, failedTotal = 0;
        for (RunManifest.Invocation inv : run.manifest.invocations) {
            if (inv == null) continue;
            if (inv.sent != null) sentTotal += inv.sent;
            if (inv.acked != null) ackedTotal += inv.acked;
            if (inv.failed != null) failedTotal += inv.failed;
        }
        summary.sent = sentTotal;
        summary.acked = ackedTotal;
        summary.failed = failedTotal;

        summary.ackMean = ackAggregator.runMeanMs();
        summary.ackP50 = ackAggregator.runP50Ms();
        summary.ackP95 = ackAggregator.runP95Ms();
        summary.ackP99 = ackAggregator.runP99Ms();
        summary.ackMax = ackAggregator.runMaxMs();
        summary.ackSdMs = ackAggregator.runStdMs();

        Map<String, Long> errorsAcc = new HashMap<>();
        long ackBytesTotal = 0L;
        long ackSentTotal = 0L;
        for (AckSnapshot snap : run.ackSnaps) {
            if (snap == null) continue;
            ackSentTotal += snap.sent();
            ackBytesTotal += snap.bytes();
            if (snap.errors() != null) {
                snap.errors().forEach((type, count) -> errorsAcc.merge(type, count == null ? 0L : count, Long::sum));
            }
        }
        summary.errors = errorsAcc;
        summary.avgRecordBytes = ackSentTotal > 0 ? (double) ackBytesTotal / ackSentTotal : Double.NaN;

        if (promQueryClient.isConfigured()) {
            RunSummary.SaturationSignature sig = new RunSummary.SaturationSignature();
            long evalTime = stop / 1000;
            long rangeSec = Math.max(1, (stop - analysisStart) / 1000);
            sig.handlerIdle = safe(promQueryClient.avgOverTime("kafka_server_requesthandleravgidlepercent", rangeSec, evalTime));
            sig.netIdle = safe(promQueryClient.avgOverTime("kafka_network_networkprocessoravgidlepercent", rangeSec, evalTime));
            double cpuIdle = promQueryClient.avgOverTime(
                    "avg(rate(node_cpu_seconds_total{mode=\"idle\",node=~\"kafka-.*\"}[1m]))", rangeSec, evalTime);
            sig.cpuBusy = Double.isFinite(cpuIdle) ? 1.0 - cpuIdle : null;
            sig.requestQueue = safe(promQueryClient.avgOverTime("kafka_network_requestchannel_requestqueuesize", rangeSec, evalTime));
            sig.fired = (sig.handlerIdle != null && sig.handlerIdle < 0.1)
                    || (sig.netIdle != null && sig.netIdle < 0.1)
                    || (sig.cpuBusy != null && sig.cpuBusy > 0.9)
                    || (sig.requestQueue != null && sig.requestQueue > 0);
            summary.saturationSignature = sig;
        } else {
            summary.saturationSignature = null;
        }

        return summary;
    }

    private static Double safe(double v) {
        return Double.isFinite(v) ? v : null;
    }

    private void persist(ActiveRun run) {
        String runId = run.manifest.runId;
        try {
            String manifestJson = MAPPER.writeValueAsString(run.manifest);
            store.saveRun(manifestJson);
            store.insertSeriesBatch(runId, new ArrayList<>(run.seriesTicks));
            store.insertAckBatch(runId, new ArrayList<>(run.ackSnaps));
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to persist run {0}: {1}", new Object[]{runId, e.getMessage()});
        }
    }

    public void onTick(Tick tick) {
        ActiveRun run = activeRun.get();
        if (run == null || run.finished || tick == null) return;
        long lowerBound = run.manifest.startEpochMs - 5_000L;
        long upperBound = run.manifest.stopEpochMs > 0 ? run.manifest.stopEpochMs + 10_000L : Long.MAX_VALUE;
        if (tick.tsMs() >= lowerBound && tick.tsMs() <= upperBound) {
            run.seriesTicks.add(tick);
        }
    }

    public void onAckBucketClosed(AckSnapshot snapshot) {
        ActiveRun run = activeRun.get();
        if (run == null || run.finished || snapshot == null) return;
        run.ackSnaps.add(snapshot);
    }

    public Tick.RunStatus currentRunStatus() {
        ActiveRun run = activeRun.get();
        if (run == null || run.finished) return Tick.RunStatus.idle();
        long elapsed = (System.currentTimeMillis() - run.manifest.startEpochMs) / 1000;
        return new Tick.RunStatus(true, run.manifest.runId, run.manifest.exp, run.manifest.startEpochMs, elapsed);
    }

    public boolean abortRun(String runId) {
        ActiveRun run = activeRun.get();
        if (run == null || !run.manifest.runId.equals(runId)) return false;
        run.aborted = true;
        run.manifest.status = "aborted";
        return true;
    }


    public List<String> listRuns() {
        return store.listRunIds();
    }

    public String getManifest(String runId) {
        return store.getManifest(runId);
    }

    public RunManifest getManifestObject(String runId) {
        String json = store.getManifest(runId);
        if (json == null) return null;
        try {
            return MAPPER.readValue(json, RunManifest.class);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Manifest of run {0} is not parseable: {1}", new Object[]{runId, e.getMessage()});
            return null;
        }
    }

    public List<Map<String, Object>> getSeries(String runId) {
        return store.getSeries(runId);
    }

    public List<Map<String, Object>> getAck(String runId) {
        return store.getAck(runId);
    }

    public Profile computeProfileFromRun(String runId) {
        RunManifest manifest = getManifestObject(runId);
        if (manifest == null) {
            throw new ValidationException("run not found: " + runId);
        }
        long analysisStart = manifest.analysisStartMs;

        List<Double> ca2s = new ArrayList<>();
        List<Double> e2eP50s = new ArrayList<>();
        List<Map<String, Object>> rows;
        try {
            rows = store.getSeries(runId);
        } catch (RuntimeException e) {
            throw new ValidationException("failed to read series of run " + runId + ": " + e.getMessage());
        }
        for (Map<String, Object> row : rows) {
            long tsMs = asLong(row.get("ts_ms"));
            if (tsMs < analysisStart) continue;
            double ca2 = asDouble(row.get("ca2_est"));
            if (Double.isFinite(ca2)) ca2s.add(ca2);
            double e2eP50 = asDouble(row.get("e2e_p50_ms"));
            if (Double.isFinite(e2eP50)) e2eP50s.add(e2eP50);
        }

        double tauAckMs = Double.NaN;
        double cs2 = Double.NaN;
        if (manifest.summary != null) {
            if (Double.isFinite(manifest.summary.ackP50)) tauAckMs = manifest.summary.ackP50;
            cs2 = cs2Robust(runId, manifest);
        }
        double tauE2eMs = median(e2eP50s);
        double ca2 = median(ca2s);
        if (!Double.isFinite(ca2)) {
            ca2 = 1.0;
            LOG.info(() -> "from-run " + runId + ": ca2 unmeasurable at this load, using Poisson fallback 1.0");
        }

        Profile existing = kingmanEngine.activeProfile();
        double muMsgs = existing != null ? existing.getMuMsgs() : Double.NaN;

        Profile profile = new Profile(config, muMsgs, tauAckMs, tauE2eMs, ca2, cs2, runId);
        store.saveProfile(runId, profile);
        return profile;
    }

    @Override
    public void close() {
        store.close();
    }


    public Map<String, List<Map<String, Object>>> calibrationCandidates() {
        List<Map<String, Object>> tauAck = new ArrayList<>();
        List<Map<String, Object>> tauE2e = new ArrayList<>();
        List<Map<String, Object>> cs2 = new ArrayList<>();
        List<Map<String, Object>> ca2 = new ArrayList<>();
        List<Map<String, Object>> mu = new ArrayList<>();
        Map<String, Integer> skipped = new LinkedHashMap<>();
        Map<String, String> skipReason = new LinkedHashMap<>();
        for (String runId : store.listRunIds()) {
            RunManifest m = getManifestObject(runId);
            if (m == null || !"completed".equals(m.status)) continue;
            if ("E1".equals(m.exp)) {
                if (m.summary != null && Double.isFinite(m.summary.ackP50)) {
                    tauAck.add(candidate(runId, m, m.summary.ackP50, null));
                } else {
                    skip(skipped, skipReason, "tauAckMs", "bieg bez summary.ackP50");
                }
                double e2e = e2eP50Of(runId, m);
                if (Double.isFinite(e2e)) {
                    tauE2e.add(candidate(runId, m, e2e, null));
                } else {
                    skip(skipped, skipReason, "tauE2eMs", "brak danych e2e w serii");
                }
                if (m.summary != null && Double.isFinite(m.summary.ackMean)
                        && m.summary.ackMean > 0) {
                    double cs2v = cs2Robust(runId, m);
                    if (Double.isFinite(cs2v)) {
                        cs2.add(candidate(runId, m, cs2v, null));
                    } else {
                        skip(skipped, skipReason, "cs2", "za mało kubełków z p95 w oknie");
                    }
                } else {
                    skip(skipped, skipReason, "cs2", "manifest bez ackSdMs (bieg sprzed poprawki)");
                }
                double c = ca2Of(runId, m);
                if (Double.isFinite(c)) {
                    ca2.add(candidate(runId, m, c, null));
                } else {
                    skip(skipped, skipReason, "ca2", "niepoliczalne przy tym obciążeniu (fallback 1.0 przy zapisie)");
                }
            } else if ("E4".equals(m.exp)) {
                if (m.summary != null && Double.isFinite(m.summary.lambdaMean)) {
                    mu.add(candidate(runId, m, m.summary.lambdaMean, m.parallelism));
                } else {
                    skip(skipped, skipReason, "muMsgs", "bieg bez summary.lambdaMean");
                }
            }
        }
        Map<String, List<Map<String, Object>>> out = new LinkedHashMap<>();
        out.put("tauAckMs", tauAck);
        out.put("tauE2eMs", tauE2e);
        out.put("cs2", cs2);
        out.put("ca2", ca2);
        out.put("muMsgs", mu);
        List<Map<String, Object>> skippedRows = new ArrayList<>();
        for (Map.Entry<String, Integer> e : skipped.entrySet()) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("param", e.getKey());
            r.put("count", e.getValue());
            r.put("reason", skipReason.getOrDefault(e.getKey(), ""));
            skippedRows.add(r);
        }
        out.put("_skipped", skippedRows);
        return out;
    }

    private static void skip(Map<String, Integer> skipped, Map<String, String> reasons,
                             String param, String reason) {
        skipped.merge(param, 1, Integer::sum);
        reasons.putIfAbsent(param, reason);
    }

    private static Map<String, Object> candidate(String runId, RunManifest m, double value, Integer p) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("runId", runId);
        c.put("exp", m.exp);
        c.put("label", m.label);
        c.put("runIndex", m.runIndex);
        if (p != null) c.put("parallelism", p);
        c.put("value", value);
        return c;
    }

    private double e2eP50Of(String runId, RunManifest m) {
        List<Double> vals = new ArrayList<>();
        try {
            for (Map<String, Object> row : store.getSeries(runId)) {
                if (asLong(row.get("ts_ms")) < m.analysisStartMs) continue;
                double v = asDouble(row.get("e2e_p50_ms"));
                if (Double.isFinite(v)) vals.add(v);
            }
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "candidates: cannot read series of {0}: {1}",
                    new Object[]{runId, e.getMessage()});
        }
        return median(vals);
    }

    private double ca2Of(String runId, RunManifest m) {
        List<Double> vals = new ArrayList<>();
        try {
            for (Map<String, Object> row : store.getSeries(runId)) {
                if (asLong(row.get("ts_ms")) < m.analysisStartMs) continue;
                double v = asDouble(row.get("ca2_est"));
                if (Double.isFinite(v)) vals.add(v);
            }
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "candidates: cannot read series of {0}: {1}",
                    new Object[]{runId, e.getMessage()});
        }
        return median(vals);
    }

    public static class FromRunsRequest {
        public List<String> tauAckRuns;
        public List<String> tauE2eRuns;
        public List<String> cs2Runs;
        public List<String> ca2Runs;
        public List<String> muRuns;
        public boolean dryRun;
        public String name;
    }

    public Map<String, Object> computeProfileFromRuns(FromRunsRequest req) {
        if (req == null) throw new ValidationException("empty request");
        Map<String, List<String>> sel = new LinkedHashMap<>();
        sel.put("tauAckMs", req.tauAckRuns);
        sel.put("tauE2eMs", req.tauE2eRuns);
        sel.put("cs2", req.cs2Runs);
        sel.put("ca2", req.ca2Runs);
        sel.put("muMsgs", req.muRuns);
        for (Map.Entry<String, List<String>> e : sel.entrySet()) {
            if (e.getValue() != null && !e.getValue().isEmpty() && e.getValue().size() < 3) {
                throw new ValidationException("parameter " + e.getKey()
                        + " needs at least 3 runs (got " + e.getValue().size() + ")");
            }
        }

        Map<String, Map<String, Double>> byParam = candidateValuesByParam();
        Map<String, Object> stats = new LinkedHashMap<>();
        Map<String, Double> medians = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : sel.entrySet()) {
            List<String> ids = e.getValue();
            if (ids == null || ids.isEmpty()) continue;
            List<Double> vals = new ArrayList<>();
            for (String id : ids) {
                Double v = byParam.getOrDefault(e.getKey(), Map.of()).get(id);
                if (v == null || !Double.isFinite(v)) {
                    throw new ValidationException("run " + id + " is not eligible for " + e.getKey());
                }
                vals.add(v);
            }
            double med = median(vals);
            double sd = sampleSd(vals, med);
            double cov = (med != 0) ? sd / Math.abs(med) : Double.NaN;
            medians.put(e.getKey(), med);
            Map<String, Object> st = new LinkedHashMap<>();
            st.put("value", med);
            st.put("sd", sd);
            st.put("cov", cov);
            st.put("n", (double) vals.size());
            st.put("runs", new ArrayList<>(ids));
            stats.put(e.getKey(), st);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("stats", stats);
        if (!medians.containsKey("ca2") && byParam.getOrDefault("ca2", Map.of()).isEmpty()
                && (!medians.isEmpty())) {
            medians.put("ca2", 1.0);
            LOG.info(() -> "from-runs: no measurable ca2 anywhere, using Poisson fallback 1.0");
            Map<String, Object> st = new LinkedHashMap<>();
            st.put("value", 1.0);
            st.put("sd", 0.0);
            st.put("cov", 0.0);
            st.put("n", 0.0);
            st.put("runs", List.of());
            st.put("fallback", "poisson-1.0");
            stats.put("ca2", st);
        }
        if (req.dryRun) {
            out.put("saved", false);
            return out;
        }
        Profile cur = kingmanEngine.activeProfile();
        String name = (req.name != null && !req.name.isBlank()) ? req.name.strip()
                : "calibration " + countsSuffix(sel);
        Profile next = new Profile(config,
                medians.getOrDefault("muMsgs", cur != null ? cur.getMuMsgs() : Double.NaN),
                medians.getOrDefault("tauAckMs", cur != null ? cur.getTauAckMs() : Double.NaN),
                medians.getOrDefault("tauE2eMs", cur != null ? cur.getTauE2eMs() : Double.NaN),
                medians.getOrDefault("ca2", cur != null ? cur.getCa2() : Double.NaN),
                medians.getOrDefault("cs2", cur != null ? cur.getCs2() : Double.NaN),
                name);
        store.saveProfile(name, next);
        out.put("saved", true);
        out.put("name", name);
        out.put("profile", next);
        out.put("active", false);
        return out;
    }

    public Map<String, Object> profileLibrary() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("config", config);
        out.put("active", store.getActiveProfileName(config));
        out.put("profiles", store.listProfiles());
        return out;
    }

    public Profile activateProfile(String name) {
        if (name == null || name.isBlank()) throw new ValidationException("profile name required");
        String json = store.getProfileJson(name);
        if (json == null) throw new ValidationException("unknown profile: " + name);
        final Profile p;
        try {
            p = MAPPER.readValue(json, Profile.class);
        } catch (Exception e) {
            throw new ValidationException("stored profile " + name + " is unparseable");
        }
        if (p.getConfig() != null && !p.getConfig().isBlank() && !p.getConfig().equals(config)) {
            throw new ValidationException("profile " + name + " is for config " + p.getConfig()
                    + ", this dashboard serves " + config);
        }
        p.setConfig(config);
        kingmanEngine.putProfile(p);
        store.setActiveProfileName(config, name);
        return p;
    }

    private Map<String, Map<String, Double>> candidateValuesByParam() {
        Map<String, Map<String, Double>> out = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> cands = calibrationCandidates();
        for (Map.Entry<String, List<Map<String, Object>>> e : cands.entrySet()) {
            if (e.getKey().startsWith("_")) continue;
            Map<String, Double> byRun = new LinkedHashMap<>();
            for (Map<String, Object> c : e.getValue()) {
                Object v = c.get("value");
                if (c.get("runId") instanceof String id && v instanceof Number n
                        && Double.isFinite(n.doubleValue())) {
                    byRun.put(id, n.doubleValue());
                }
            }
            out.put(e.getKey(), byRun);
        }
        return out;
    }

    private static String countsSuffix(Map<String, List<String>> sel) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> e : sel.entrySet()) {
            if (e.getValue() != null && !e.getValue().isEmpty()) {
                if (sb.length() > 0) sb.append('+');
                sb.append(e.getValue().size()).append('x').append(e.getKey());
            }
        }
        return sb.toString();
    }

    private static double sampleSd(List<Double> vals, double mean) {
        int n = vals.size();
        if (n < 2) return 0.0;
        double sq = 0.0;
        for (double v : vals) sq += (v - mean) * (v - mean);
        return Math.sqrt(sq / (n - 1));
    }

    private static long asLong(Object v) {
        return v instanceof Number ? ((Number) v).longValue() : 0L;
    }

    private double cs2Robust(String runId, RunManifest m) {
        if (m.summary == null) return Double.NaN;
        double mean = m.summary.ackMean;
        if (!Double.isFinite(mean) || mean <= 0) return Double.NaN;
        List<Double> spreads = new ArrayList<>();
        try {
            for (Map<String, Object> row : store.getAck(runId)) {
                if (asLong(row.get("window_end_ms")) < m.analysisStartMs) continue;
                double p50 = asDouble(row.get("ack_p50_ms"));
                double p95 = asDouble(row.get("ack_p95_ms"));
                if (Double.isFinite(p50) && Double.isFinite(p95) && p95 >= p50) {
                    spreads.add((p95 - p50) / 1.645);
                }
            }
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "cs2Robust: cannot read ack of {0}: {1}",
                    new Object[]{runId, e.getMessage()});
            return Double.NaN;
        }
        if (spreads.size() < 3) return Double.NaN;
        double r = median(spreads) / mean;
        return r * r;
    }

    private static double asDouble(Object v) {
        return v instanceof Number ? ((Number) v).doubleValue() : Double.NaN;
    }

    private static double median(List<Double> values) {
        if (values.isEmpty()) return Double.NaN;
        List<Double> sorted = new ArrayList<>(values);
        java.util.Collections.sort(sorted);
        int n = sorted.size();
        if (n % 2 == 1) return sorted.get(n / 2);
        return (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }
}
