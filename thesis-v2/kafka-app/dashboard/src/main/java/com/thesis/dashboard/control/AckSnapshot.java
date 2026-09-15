package com.thesis.dashboard.control;

import java.util.Map;

public record AckSnapshot(
        long windowEnd,
        long invocations,
        long sent,
        long acked,
        long failed,
        long bytes,
        double mean,
        double p50,
        double p95,
        double p99,
        double max,
        Map<String, Long> errors
) {
    public static AckSnapshot empty() {
        return new AckSnapshot(0, 0, 0, 0, 0, 0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Map.of());
    }
}
