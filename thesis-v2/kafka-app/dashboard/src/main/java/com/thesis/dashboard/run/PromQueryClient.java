package com.thesis.dashboard.run;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PromQueryClient {

    private static final Logger LOG = Logger.getLogger(PromQueryClient.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String promUrl;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public PromQueryClient(String promUrl) {
        this.promUrl = promUrl;
    }

    public boolean isConfigured() {
        return promUrl != null && !promUrl.isBlank();
    }

    public double avgOverTime(String metricExpr, long rangeSec, long evalTimeEpochSec) {
        if (!isConfigured()) return Double.NaN;
        try {
            String query = "avg_over_time(" + metricExpr + "[" + rangeSec + "s])";
            String url = promUrl + "/api/v1/query?query=" + urlEncode(query) + "&time=" + evalTimeEpochSec;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(8))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) return Double.NaN;
            JsonNode root = MAPPER.readTree(response.body());
            JsonNode result = root.path("data").path("result");
            if (!result.isArray() || result.isEmpty()) return Double.NaN;
            JsonNode value = result.get(0).path("value");
            if (!value.isArray() || value.size() < 2) return Double.NaN;
            return Double.parseDouble(value.get(1).asText());
        } catch (Exception e) {
            LOG.log(Level.FINE, "PromQL query failed: {0}", e.getMessage());
            return Double.NaN;
        }
    }

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
