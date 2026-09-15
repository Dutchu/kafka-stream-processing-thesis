package com.thesis.dashboard.control;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AckAggregatorTest {

    private static final long LOWEST_US = 1L;
    private static final long HIGHEST_US = 60_000_000L;
    private static final int SIG_DIGITS = 3;

    private static String encode(Histogram h) {
        ByteBuffer buffer = ByteBuffer.allocate(h.getNeededByteBufferCapacity());
        int len = h.encodeIntoCompressedByteBuffer(buffer);
        byte[] compressed = new byte[len];
        buffer.rewind();
        buffer.get(compressed);
        return Base64.getEncoder().encodeToString(compressed);
    }

    private static Histogram recordUniform(long fromUs, long toUs, long stepUs) {
        Histogram h = new Histogram(LOWEST_US, HIGHEST_US, SIG_DIGITS);
        for (long v = fromUs; v <= toUs; v += stepUs) {
            h.recordValue(v);
        }
        return h;
    }

    private AckReport reportFor(String runId, int invocationId, long windowEndMs, Histogram windowHist) {
        AckReport r = new AckReport();
        r.setRunId(runId);
        r.setInvocationId(invocationId);
        r.setTopic("weather-rain");
        r.setWindowStartMs(windowEndMs - 1000);
        r.setWindowEndMs(windowEndMs);
        r.setSent(windowHist.getTotalCount());
        r.setAcked(windowHist.getTotalCount());
        r.setFailed(0);
        r.setBytes(windowHist.getTotalCount() * 150);
        r.setAckMeanMs(windowHist.getMean() / 1000.0);
        r.setAckP50Ms(windowHist.getValueAtPercentile(50) / 1000.0);
        r.setAckP99Ms(windowHist.getValueAtPercentile(99) / 1000.0);
        r.setAckMaxMs(windowHist.getMaxValue() / 1000.0);
        r.setHdrBase64(encode(windowHist));
        r.setFinal(false);
        return r;
    }

    @Test
    void mergesThreeEncodedHistogramsAndMatchesReferenceP50() {
        AckAggregator aggregator = new AckAggregator("unused:9092", "control-metrics");

        long now = System.currentTimeMillis();
        long windowEndMs = now - 200;
        long bucketSec = Math.floorDiv(windowEndMs, 1000L);

        Histogram h1 = recordUniform(1_000, 10_000, 500);
        Histogram h2 = recordUniform(2_000, 20_000, 1000);
        Histogram h3 = recordUniform(5_000, 15_000, 250);

        aggregator.ingest(reportFor("runX", 1, windowEndMs, h1));
        aggregator.ingest(reportFor("runX", 2, windowEndMs, h2));
        aggregator.ingest(reportFor("runX", 3, windowEndMs, h3));

        aggregator.closeStaleBuckets(bucketSec * 1000L + 10_001L);

        AckSnapshot snapshot = aggregator.snapshot();
        assertEquals(bucketSec * 1000L, snapshot.windowEnd());
        assertEquals(3, snapshot.invocations());

        Histogram reference = new Histogram(LOWEST_US, HIGHEST_US, SIG_DIGITS);
        reference.add(h1);
        reference.add(h2);
        reference.add(h3);

        double expectedP50Ms = reference.getValueAtPercentile(50) / 1000.0;
        assertEquals(expectedP50Ms, snapshot.p50(), 1e-6,
                "merged bucket p50 must equal directly-added reference histogram p50 (Histogram.add, not average of percentiles)");

        long expectedSent = h1.getTotalCount() + h2.getTotalCount() + h3.getTotalCount();
        assertEquals(expectedSent, snapshot.sent());
        assertEquals(expectedSent, snapshot.acked());
    }

    @Test
    void lateReportsOlderThanThresholdAreDroppedAndCounted() {
        AckAggregator aggregator = new AckAggregator("unused:9092", "control-metrics");
        long now = System.currentTimeMillis();
        long veryOldWindowEndMs = now - 60_000;

        Histogram h = recordUniform(1_000, 5_000, 500);
        aggregator.ingest(reportFor("runX", 1, veryOldWindowEndMs, h));

        assertEquals(1L, aggregator.lateReports());
    }

    @Test
    void errorsAndByteCountsAreSummedAcrossInvocations() {
        AckAggregator aggregator = new AckAggregator("unused:9092", "control-metrics");
        long now = System.currentTimeMillis();
        long windowEndMs = now - 200;
        long bucketSec = Math.floorDiv(windowEndMs, 1000L);

        Histogram h1 = recordUniform(1_000, 3_000, 500);
        AckReport r1 = reportFor("runY", 1, windowEndMs, h1);
        r1.setErrors(Map.of("TimeoutException", 2L));
        AckReport r2 = reportFor("runY", 2, windowEndMs, h1);
        r2.setErrors(Map.of("TimeoutException", 1L, "BufferExhaustedException", 3L));

        aggregator.ingest(r1);
        aggregator.ingest(r2);
        aggregator.closeStaleBuckets(bucketSec * 1000L + 10_001L);

        AckSnapshot snapshot = aggregator.snapshot();
        assertTrue(snapshot.errors().get("TimeoutException") == 3L);
        assertTrue(snapshot.errors().get("BufferExhaustedException") == 3L);
    }
}
