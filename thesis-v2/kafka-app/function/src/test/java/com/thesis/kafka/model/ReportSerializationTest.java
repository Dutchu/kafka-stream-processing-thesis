package com.thesis.kafka.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafka.producer.AckReporter;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportSerializationTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void roundTripsAllContractFields() throws Exception {
        Histogram histogram = new Histogram(
                AckReporter.LOWEST_DISCERNIBLE_VALUE_US,
                AckReporter.HIGHEST_TRACKABLE_VALUE_US,
                AckReporter.SIGNIFICANT_DIGITS);
        histogram.recordValue(9_000);
        histogram.recordValue(11_000);
        histogram.recordValue(48_200);
        histogram.recordValue(120_000);

        AckReport report = new AckReport();
        report.setRunId("E2-3b-rf3-rho50-run1-1725000000");
        report.setInvocationId(7L);
        report.setTopic("weather-rain");
        report.setWindowStartMs(1_725_000_000_000L);
        report.setWindowEndMs(1_725_000_001_000L);
        report.setSent(501L);
        report.setAcked(499L);
        report.setFailed(0L);
        report.setErrors(Map.of());
        report.setBytes(72645L);
        report.setAckMeanMs(histogram.getMean() / 1000.0);
        report.setAckP50Ms(histogram.getValueAtPercentile(50.0) / 1000.0);
        report.setAckP99Ms(histogram.getValueAtPercentile(99.0) / 1000.0);
        report.setAckMaxMs(histogram.getMaxValue() / 1000.0);
        report.setHdrBase64(AckReporter.encodeHistogram(histogram));
        report.setFinal(false);

        String json = mapper.writeValueAsString(report);
        JsonNode node = mapper.readTree(json);

        assertTrue(node.has("runId"));
        assertTrue(node.has("invocationId"));
        assertTrue(node.has("topic"));
        assertTrue(node.has("windowStartMs"));
        assertTrue(node.has("windowEndMs"));
        assertTrue(node.has("sent"));
        assertTrue(node.has("acked"));
        assertTrue(node.has("failed"));
        assertTrue(node.has("errors"));
        assertTrue(node.has("bytes"));
        assertTrue(node.has("ackMeanMs"));
        assertTrue(node.has("ackP50Ms"));
        assertTrue(node.has("ackP99Ms"));
        assertTrue(node.has("ackMaxMs"));
        assertTrue(node.has("hdrBase64"));
        assertTrue(node.has("final"));
        assertFalse(node.get("final").asBoolean());

        AckReport parsed = mapper.readValue(json, AckReport.class);
        assertEquals(report.getRunId(), parsed.getRunId());
        assertEquals(report.getInvocationId(), parsed.getInvocationId());
        assertEquals(report.getTopic(), parsed.getTopic());
        assertEquals(report.getSent(), parsed.getSent());
        assertEquals(report.getAcked(), parsed.getAcked());
        assertEquals(report.getAckP50Ms(), parsed.getAckP50Ms(), 0.0001);
        assertEquals(report.isFinal(), parsed.isFinal());
        assertEquals(report.getHdrBase64(), parsed.getHdrBase64());
    }

    @Test
    void hdrBase64DecodesAndMatchesSourceHistogramPercentiles() throws Exception {
        Histogram original = new Histogram(
                AckReporter.LOWEST_DISCERNIBLE_VALUE_US,
                AckReporter.HIGHEST_TRACKABLE_VALUE_US,
                AckReporter.SIGNIFICANT_DIGITS);
        for (long v : new long[] {5_000, 8_000, 9_000, 9_500, 10_000, 15_000, 30_200, 55_000, 410_000}) {
            original.recordValue(v);
        }

        String encoded = AckReporter.encodeHistogram(original);
        assertFalse(encoded.isBlank());

        byte[] decodedBytes = Base64.getDecoder().decode(encoded);
        Histogram decoded = Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(decodedBytes), 0);

        assertEquals(original.getTotalCount(), decoded.getTotalCount());
        assertEquals(original.getValueAtPercentile(50.0), decoded.getValueAtPercentile(50.0));
        assertEquals(original.getValueAtPercentile(99.0), decoded.getValueAtPercentile(99.0));
        assertEquals(original.getMaxValue(), decoded.getMaxValue());
    }

    @Test
    void finalReportSerializesFinalTrue() throws Exception {
        AckReport report = new AckReport();
        report.setRunId("run-x");
        report.setInvocationId(1L);
        report.setTopic("weather-wind");
        report.setErrors(Map.of("TimeoutException", 2L));
        report.setHdrBase64("");
        report.setFinal(true);

        String json = mapper.writeValueAsString(report);
        JsonNode node = mapper.readTree(json);
        assertTrue(node.get("final").asBoolean());
        assertEquals(2L, node.get("errors").get("TimeoutException").asLong());
    }

    @Test
    void runSummaryRoundTripsContractFields() throws Exception {
        RunSummary summary = new RunSummary();
        summary.setStatus("ok");
        summary.setRunId("run-x");
        summary.setInvocationId(7L);
        summary.setTopic("weather-rain");
        summary.setStartEpochMs(1000L);
        summary.setEndEpochMs(2000L);
        summary.setSent(60000L);
        summary.setAcked(59998L);
        summary.setFailed(2L);
        summary.setErrors(Map.of("TimeoutException", 2L));
        summary.setBytes(8_700_000L);
        summary.setAvgRecordBytes(145L);
        RunSummary.AckStats ack = new RunSummary.AckStats();
        ack.setCount(59998L);
        ack.setMeanMs(12.4);
        ack.setP50Ms(9.1);
        ack.setP95Ms(30.2);
        ack.setP99Ms(55.0);
        ack.setMaxMs(410.0);
        summary.setAck(ack);
        summary.setAchievedRatePerSec(499.8);
        summary.setInstanceId("test-instance");

        String json = mapper.writeValueAsString(summary);
        JsonNode node = mapper.readTree(json);

        assertEquals("ok", node.get("status").asText());
        assertTrue(node.has("runId"));
        assertTrue(node.has("invocationId"));
        assertTrue(node.has("topic"));
        assertTrue(node.has("startEpochMs"));
        assertTrue(node.has("endEpochMs"));
        assertTrue(node.has("sent"));
        assertTrue(node.has("acked"));
        assertTrue(node.has("failed"));
        assertTrue(node.has("errors"));
        assertTrue(node.has("bytes"));
        assertTrue(node.has("avgRecordBytes"));
        assertTrue(node.has("ack"));
        assertEquals(9.1, node.get("ack").get("p50Ms").asDouble(), 0.0001);
        assertTrue(node.has("achievedRatePerSec"));
        assertTrue(node.has("instanceId"));

        RunSummary parsed = mapper.readValue(json, RunSummary.class);
        assertEquals(summary.getSent(), parsed.getSent());
        assertEquals(summary.getAck().getP99Ms(), parsed.getAck().getP99Ms(), 0.0001);
    }
}
