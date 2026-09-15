package com.thesis.dashboard.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AckAggregator implements Runnable {

    private static final Logger LOG = Logger.getLogger(AckAggregator.class.getName());
    private static final long LOG_THROTTLE_MS = 10_000L;
    private static final long LATE_THRESHOLD_MS = 10_000L;

    private static final long HIST_LOWEST_US = 1L;
    private static final long HIST_HIGHEST_US = 60_000_000L;
    private static final int HIST_SIG_DIGITS = 3;

    private final String bootstrapServers;
    private final String controlTopic;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private volatile boolean running = true;
    private volatile long lastErrorLogMs = 0L;

    private static final class Bucket {
        final long bucketSec;
        final Histogram merged = new Histogram(HIST_LOWEST_US, HIST_HIGHEST_US, HIST_SIG_DIGITS);
        long sent, acked, failed, bytes;
        final Map<String, Long> errors = new HashMap<>();
        final Set<Integer> invocationIds = new java.util.HashSet<>();

        Bucket(long bucketSec) {
            this.bucketSec = bucketSec;
        }
    }

    private final TreeMap<Long, Bucket> openBuckets = new TreeMap<>();
    private final Object bucketsLock = new Object();

    private final AtomicReference<AckSnapshot> latest = new AtomicReference<>(AckSnapshot.empty());
    private final java.util.concurrent.atomic.AtomicLong lateReports = new java.util.concurrent.atomic.AtomicLong(0);

    private volatile Histogram runHistogram = new Histogram(HIST_LOWEST_US, HIST_HIGHEST_US, HIST_SIG_DIGITS);
    private final Object runHistLock = new Object();

    public AckAggregator(String bootstrapServers, String controlTopic) {
        this.bootstrapServers = bootstrapServers;
        this.controlTopic = controlTopic;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "thesis-dashboard-v2-control");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(controlTopic));
            LOG.info("AckAggregator started on topic " + controlTopic);

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        AckReport report = MAPPER.readValue(record.value(), AckReport.class);
                        ingest(report);
                    } catch (Exception e) {
                        logThrottled("failed to parse AckReport: " + e.getMessage());
                    }
                }
                closeStaleBuckets();
            }
        } catch (Exception e) {
            logThrottled("AckAggregator terminated: " + e.getMessage());
        }
    }

    void ingest(AckReport report) {
        long bucketSec = Math.floorDiv(report.getWindowEndMs(), 1000L);
        long nowSec = Math.floorDiv(System.currentTimeMillis(), 1000L);

        Histogram decoded = decodeHistogram(report.getHdrBase64());
        if (decoded != null) {
            synchronized (runHistLock) {
                runHistogram.add(decoded);
            }
        }

        synchronized (bucketsLock) {
            if (bucketSec < nowSec - (LATE_THRESHOLD_MS / 1000)) {
                lateReports.incrementAndGet();
                return;
            }
            Bucket bucket = openBuckets.computeIfAbsent(bucketSec, Bucket::new);
            if (decoded != null) {
                bucket.merged.add(decoded);
            }
            bucket.sent += report.getSent();
            bucket.acked += report.getAcked();
            bucket.failed += report.getFailed();
            bucket.bytes += report.getBytes();
            bucket.invocationIds.add(report.getInvocationId());
            if (report.getErrors() != null) {
                report.getErrors().forEach((type, count) ->
                        bucket.errors.merge(type, count, Long::sum));
            }
        }
    }

    private static Histogram decodeHistogram(String hdrBase64) {
        if (hdrBase64 == null || hdrBase64.isEmpty()) return null;
        try {
            byte[] raw = Base64.getDecoder().decode(hdrBase64);
            return Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(raw), 0);
        } catch (Exception e) {
            return null;
        }
    }

    void closeStaleBuckets() {
        closeStaleBuckets(System.currentTimeMillis());
    }

    void closeStaleBuckets(long nowMillis) {
        long nowSec = Math.floorDiv(nowMillis, 1000L);
        long cutoff = nowSec - (LATE_THRESHOLD_MS / 1000);
        AckSnapshot lastPublished = null;
        synchronized (bucketsLock) {
            while (!openBuckets.isEmpty() && openBuckets.firstKey() <= cutoff) {
                Bucket bucket = openBuckets.pollFirstEntry().getValue();
                lastPublished = toSnapshot(bucket);
            }
        }
        if (lastPublished != null) {
            latest.set(lastPublished);
        }
    }

    private static AckSnapshot toSnapshot(Bucket bucket) {
        boolean hasData = bucket.merged.getTotalCount() > 0;
        double mean = hasData ? bucket.merged.getMean() / 1000.0 : Double.NaN;
        double p50 = hasData ? bucket.merged.getValueAtPercentile(50) / 1000.0 : Double.NaN;
        double p95 = hasData ? bucket.merged.getValueAtPercentile(95) / 1000.0 : Double.NaN;
        double p99 = hasData ? bucket.merged.getValueAtPercentile(99) / 1000.0 : Double.NaN;
        double max = hasData ? bucket.merged.getMaxValue() / 1000.0 : Double.NaN;
        return new AckSnapshot(
                bucket.bucketSec * 1000L,
                bucket.invocationIds.size(),
                bucket.sent,
                bucket.acked,
                bucket.failed,
                bucket.bytes,
                mean, p50, p95, p99, max,
                Map.copyOf(bucket.errors));
    }

    public void resetRun() {
        synchronized (runHistLock) {
            runHistogram = new Histogram(HIST_LOWEST_US, HIST_HIGHEST_US, HIST_SIG_DIGITS);
        }
    }

    public double runCs2Estimate() {
        synchronized (runHistLock) {
            if (runHistogram.getTotalCount() == 0) return Double.NaN;
            double mean = runHistogram.getMean();
            if (mean <= 0) return Double.NaN;
            double sd = runHistogram.getStdDeviation();
            return (sd / mean) * (sd / mean);
        }
    }

    public double runStdMs() {
        synchronized (runHistLock) {
            if (runHistogram.getTotalCount() == 0) return Double.NaN;
            return runHistogram.getStdDeviation() / 1000.0;
        }
    }

    public double runP50Ms() {
        synchronized (runHistLock) {
            if (runHistogram.getTotalCount() == 0) return Double.NaN;
            return runHistogram.getValueAtPercentile(50) / 1000.0;
        }
    }

    public double runMeanMs() {
        synchronized (runHistLock) {
            if (runHistogram.getTotalCount() == 0) return Double.NaN;
            return runHistogram.getMean() / 1000.0;
        }
    }

    public double runP95Ms() {
        synchronized (runHistLock) {
            if (runHistogram.getTotalCount() == 0) return Double.NaN;
            return runHistogram.getValueAtPercentile(95) / 1000.0;
        }
    }

    public double runP99Ms() {
        synchronized (runHistLock) {
            if (runHistogram.getTotalCount() == 0) return Double.NaN;
            return runHistogram.getValueAtPercentile(99) / 1000.0;
        }
    }

    public double runMaxMs() {
        synchronized (runHistLock) {
            if (runHistogram.getTotalCount() == 0) return Double.NaN;
            return runHistogram.getMaxValue() / 1000.0;
        }
    }

    public AckSnapshot snapshot() {
        return latest.get();
    }

    public long lateReports() {
        return lateReports.get();
    }

    public void stop() {
        running = false;
    }

    private void logThrottled(String message) {
        long now = System.currentTimeMillis();
        if (now - lastErrorLogMs > LOG_THROTTLE_MS) {
            LOG.log(Level.WARNING, "AckAggregator: {0}", message);
            lastErrorLogMs = now;
        }
    }
}
