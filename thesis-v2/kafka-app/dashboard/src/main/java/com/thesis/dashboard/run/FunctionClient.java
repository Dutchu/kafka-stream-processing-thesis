package com.thesis.dashboard.run;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FunctionClient {

    private static final Logger LOG = Logger.getLogger(FunctionClient.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    public record InvocationResult(int invocationId, String zoneId, String topic, boolean ok,
                                    int httpStatus, JsonNode body, String error) {
    }

    public CompletableFuture<InvocationResult> invoke(String functionUrl, String runId, int invocationId,
                                                        String topic, String zoneId, Integer durationSec,
                                                        Long count, double ratePerSec, double intensity) {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("runId", runId);
        payload.put("invocationId", invocationId);
        payload.put("topic", topic);
        payload.put("zoneId", zoneId);
        if (durationSec != null) {
            payload.put("durationSec", durationSec);
        } else if (count != null) {
            payload.put("count", count);
        }
        payload.put("ratePerSec", ratePerSec);
        payload.put("intensity", intensity);
        payload.put("reportIntervalMs", 1000);

        String requestBody;
        try {
            requestBody = MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                    new InvocationResult(invocationId, zoneId, topic, false, 0, null,
                            "failed to serialize request: " + e.getMessage()));
        }

        long timeoutSec = (durationSec != null ? durationSec : 540) + 180L;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(functionUrl))
                .timeout(Duration.ofSeconds(timeoutSec))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(resp -> {
                    int status = resp.statusCode();
                    if (status >= 200 && status < 300) {
                        try {
                            JsonNode node = MAPPER.readTree(resp.body());
                            return new InvocationResult(invocationId, zoneId, topic, true, status, node, null);
                        } catch (Exception e) {
                            return new InvocationResult(invocationId, zoneId, topic, false, status, null,
                                    "invalid JSON response: " + e.getMessage());
                        }
                    }
                    return new InvocationResult(invocationId, zoneId, topic, false, status, null,
                            "HTTP " + status + ": " + truncate(resp.body()));
                })
                .exceptionally(ex -> {
                    LOG.log(Level.WARNING, "invocation {0} failed: {1}", new Object[]{invocationId, ex.getMessage()});
                    return new InvocationResult(invocationId, zoneId, topic, false, 0, null, String.valueOf(ex.getMessage()));
                });
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() > 500 ? s.substring(0, 500) : s;
    }
}
