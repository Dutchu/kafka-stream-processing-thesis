package com.thesis.kafka.producer;

public record RunParams(
        String runId,
        long invocationId,
        String zoneId,
        Long durationSec,
        Long count,
        double ratePerSec,
        double intensity,
        long reportIntervalMs,
        String bootstrapServers,
        String acks,
        String controlTopic
) {
}
