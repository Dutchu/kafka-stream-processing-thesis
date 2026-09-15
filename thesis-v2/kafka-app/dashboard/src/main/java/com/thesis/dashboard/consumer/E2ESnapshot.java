package com.thesis.dashboard.consumer;

import java.util.Map;

public record E2ESnapshot(
        double rate,
        double bytesRate,
        double mean,
        double p50,
        double p99,
        double max,
        long count,
        Map<String, Long> perTopicCount,
        long lag,
        double ca2,
        long negLat
) {
    public static E2ESnapshot empty() {
        return new E2ESnapshot(0, 0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0, Map.of(), 0, Double.NaN, 0);
    }
}
