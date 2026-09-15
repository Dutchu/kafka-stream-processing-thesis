package com.thesis.dashboard.store;

import com.thesis.dashboard.Tick;
import com.thesis.dashboard.control.AckSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RunStoreTest {

    @TempDir
    Path tempDir;

    private static Tick tick(long tsMs, String runId, double lambda, double e2eP50) {
        return new Tick(
                tsMs, runId, lambda, lambda, Double.NaN, Double.NaN,
                Double.NaN, Double.NaN, 9.5, 8.6, 10.0, 11.9, 20.0,
                40L, 2L, 12.0, e2eP50, 30.0, 45.0, 40L,
                Double.NaN, Double.NaN, Double.NaN, Double.NaN,
                2L, 40L, 40L, 0L, "", Double.NaN, Double.NaN, 120L, 5400.0,
                null, null, null);
    }

    @Test
    void manifestRoundTrip() {
        try (RunStore store = new RunStore(tempDir.resolve("t.db").toString())) {
            String manifest = "{\"runId\":\"E1-3b-rf3-smoke-run1-1\",\"status\":\"completed\"}";
            store.saveRun(manifest);
            assertEquals(manifest, store.getManifest("E1-3b-rf3-smoke-run1-1"));
            assertNull(store.getManifest("no-such-run"));
            assertEquals(List.of("E1-3b-rf3-smoke-run1-1"), store.listRunIds());
        }
    }

    @Test
    void seriesRoundTripWithNanAsNull() {
        String runId = "E1-3b-rf3-smoke-run1-1";
        try (RunStore store = new RunStore(tempDir.resolve("t.db").toString())) {
            store.saveRun("{\"runId\":\"" + runId + "\"}");
            store.insertSeriesBatch(runId, List.of(
                    tick(3000L, runId, 40.0, 9.0),
                    tick(1000L, runId, 0.0, Double.NaN),
                    tick(2000L, runId, 36.0, 8.0)));
            List<Map<String, Object>> rows = store.getSeries(runId);
            assertEquals(3, rows.size());
            assertEquals(1000L, rows.get(0).get("ts_ms"));
            assertEquals(2000L, rows.get(1).get("ts_ms"));
            assertEquals(3000L, rows.get(2).get("ts_ms"));
            assertEquals(runId, rows.get(0).get("run_id"));
            assertEquals(40.0, (Double) rows.get(2).get("lambda_leo"));
            assertTrue(((Double) rows.get(0).get("rho")).isNaN());
            assertTrue(((Double) rows.get(0).get("e2e_p50_ms")).isNaN());
            assertEquals(9.0, (Double) rows.get(2).get("e2e_p50_ms"));
            assertEquals(40L, rows.get(2).get("acked"));
            assertTrue(store.getSeries("no-such-run").isEmpty());
        }
    }

    @Test
    void ackRoundTripWithErrorsObject() {
        String runId = "E1-3b-rf3-smoke-run1-1";
        try (RunStore store = new RunStore(tempDir.resolve("t.db").toString())) {
            store.saveRun("{\"runId\":\"" + runId + "\"}");
            store.insertAckBatch(runId, List.of(
                    new AckSnapshot(2000L, 2L, 40L, 40L, 0L, 5374L,
                            10.0, 9.9, 11.3, 20.0, 20.0, Map.of()),
                    new AckSnapshot(1000L, 2L, 30L, 28L, 2L, 4032L,
                            84.3, 10.4, 457.2, 476.7, 476.7,
                            Map.of("TimeoutException", 2L))));
            List<Map<String, Object>> rows = store.getAck(runId);
            assertEquals(2, rows.size());
            assertEquals(1000L, rows.get(0).get("window_end_ms"));
            assertEquals(30L, rows.get(0).get("sent"));
            assertEquals(28L, rows.get(0).get("acked"));
            assertEquals(2L, rows.get(0).get("failed"));
            @SuppressWarnings("unchecked")
            Map<String, Long> errors = (Map<String, Long>) rows.get(0).get("errors");
            assertEquals(2L, errors.get("TimeoutException"));
            @SuppressWarnings("unchecked")
            Map<String, Long> noErrors = (Map<String, Long>) rows.get(1).get("errors");
            assertTrue(noErrors.isEmpty());
            assertEquals(476.7, (Double) rows.get(0).get("ack_max_ms"));
        }
    }

    @Test
    void watchedTopicsRoundTrip() {
        try (RunStore store = new RunStore(tempDir.resolve("t.db").toString())) {
            assertTrue(store.getWatchedTopics().isEmpty());
            store.setWatchedTopics(List.of("weather-rain", "heavy-topic"));
            assertEquals(List.of("heavy-topic", "weather-rain"), store.getWatchedTopics());
            store.setWatchedTopics(List.of());
            assertTrue(store.getWatchedTopics().isEmpty());
            store.setWatchedTopics(null);
            assertTrue(store.getWatchedTopics().isEmpty());
        }
    }
}
