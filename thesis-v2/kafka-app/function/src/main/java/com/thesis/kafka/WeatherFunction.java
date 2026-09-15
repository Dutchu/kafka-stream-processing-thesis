package com.thesis.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.thesis.kafka.model.RunSummary;
import com.thesis.kafka.producer.LoadRun;
import com.thesis.kafka.producer.RunParams;
import com.thesis.kafka.strategy.EventStrategy;
import com.thesis.kafka.strategy.EventStrategyFactory;

import java.io.BufferedReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

public class WeatherFunction implements HttpFunction {

    private static final Logger LOG = Logger.getLogger(WeatherFunction.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BROKERS", "10.0.0.6:9092,10.0.0.3:9092,10.0.0.4:9092");

    private static final String ACKS =
            System.getenv().getOrDefault("KAFKA_ACKS", "all");

    private static final String CONTROL_TOPIC =
            System.getenv().getOrDefault("CONTROL_TOPIC", "control-metrics");

    private static final long MAX_DURATION_SEC =
            parseLongEnv("MAX_DURATION_SEC", 540L);

    private static final long DEFAULT_REPORT_INTERVAL_MS = 1000L;

    private static long parseLongEnv(String name, long defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Override
    public void service(HttpRequest request, HttpResponse response) throws Exception {
        response.appendHeader("Access-Control-Allow-Origin", "*");
        response.appendHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
        response.appendHeader("Access-Control-Allow-Headers", "Content-Type");

        if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
            response.setStatusCode(204);
            return;
        }

        JsonNode json;
        try {
            String body = readBody(request);
            json = MAPPER.readTree(body);
            if (json == null || json.isMissingNode() || json.isNull()) {
                writeError(response, "Request body must be a JSON object.");
                return;
            }
        } catch (Exception e) {
            writeError(response, "Invalid JSON body: " + e.getMessage());
            return;
        }

        RunParams params;
        EventStrategy strategy;
        try {
            String topicField = textOrNull(json, "topic");
            String eventTypeField = textOrNull(json, "eventType");
            if ((topicField == null || topicField.isBlank()) && (eventTypeField == null || eventTypeField.isBlank())) {
                writeError(response, "Either 'topic' or 'eventType' is required.");
                return;
            }
            strategy = (topicField != null && !topicField.isBlank())
                    ? EventStrategyFactory.byTopic(topicField)
                    : EventStrategyFactory.byEventType(eventTypeField);

            String runId = textOrNull(json, "runId");
            if (runId == null || runId.isBlank()) {
                writeError(response, "'runId' is required.");
                return;
            }

            if (!json.hasNonNull("invocationId")) {
                writeError(response, "'invocationId' is required.");
                return;
            }
            long invocationId = json.get("invocationId").asLong();

            String zoneId = textOrNull(json, "zoneId");
            if (zoneId == null || zoneId.isBlank()) {
                writeError(response, "'zoneId' is required.");
                return;
            }

            Long durationSec = json.hasNonNull("durationSec") ? json.get("durationSec").asLong() : null;
            Long count = json.hasNonNull("count") ? json.get("count").asLong() : null;
            if (durationSec == null && count == null) {
                writeError(response, "Either 'durationSec' or 'count' is required.");
                return;
            }
            if (durationSec != null) {
                count = null;
            }
            if (durationSec != null) {
                if (durationSec <= 0) {
                    writeError(response, "'durationSec' must be > 0.");
                    return;
                }
                if (durationSec > MAX_DURATION_SEC) {
                    writeError(response, "'durationSec' exceeds MAX_DURATION_SEC=" + MAX_DURATION_SEC + ".");
                    return;
                }
            }
            if (count != null && count <= 0) {
                writeError(response, "'count' must be > 0.");
                return;
            }

            double ratePerSec = json.hasNonNull("ratePerSec") ? json.get("ratePerSec").asDouble() : 0.0;
            if (ratePerSec < 0) {
                writeError(response, "'ratePerSec' must be >= 0 (0 = unlimited).");
                return;
            }

            double intensity = json.hasNonNull("intensity") ? json.get("intensity").asDouble() : 0.5;
            if (intensity < 0.0 || intensity > 1.0) {
                writeError(response, "'intensity' must be within [0.0, 1.0].");
                return;
            }

            long reportIntervalMs = json.hasNonNull("reportIntervalMs")
                    ? json.get("reportIntervalMs").asLong()
                    : DEFAULT_REPORT_INTERVAL_MS;
            if (reportIntervalMs <= 0) {
                writeError(response, "'reportIntervalMs' must be > 0.");
                return;
            }

            params = new RunParams(runId, invocationId, zoneId, durationSec, count, ratePerSec,
                    intensity, reportIntervalMs, BOOTSTRAP_SERVERS, ACKS, CONTROL_TOPIC);
        } catch (IllegalArgumentException e) {
            writeError(response, e.getMessage());
            return;
        }

        LoadRun run = new LoadRun(params, strategy);
        RunSummary summary = run.execute();

        String result = MAPPER.writeValueAsString(summary);
        response.setContentType("application/json");
        response.setStatusCode(200);
        response.getWriter().write(result);
    }

    private static String textOrNull(JsonNode json, String field) {
        JsonNode node = json.get(field);
        return (node != null && !node.isNull()) ? node.asText() : null;
    }

    private static String readBody(HttpRequest request) throws Exception {
        StringBuilder body = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line);
            }
        }
        return body.toString();
    }

    private void writeError(HttpResponse response, String message) throws Exception {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("status", "error");
        error.put("message", message);
        response.setContentType("application/json");
        response.setStatusCode(400);
        response.getWriter().write(MAPPER.writeValueAsString(error));
    }
}
