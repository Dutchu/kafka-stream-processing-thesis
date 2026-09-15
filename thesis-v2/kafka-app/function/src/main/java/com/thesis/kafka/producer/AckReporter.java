package com.thesis.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafka.model.AckReport;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class AckReporter {

    private static final Logger LOG = Logger.getLogger(AckReporter.class.getName());

    public static final long LOWEST_DISCERNIBLE_VALUE_US = 1L;
    public static final long HIGHEST_TRACKABLE_VALUE_US = 60_000_000L;
    public static final int SIGNIFICANT_DIGITS = 3;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Producer<String, String> producer;
    private final String controlTopic;
    private final String runId;
    private final long invocationId;
    private final String topic;
    private final long reportIntervalMs;

    private final Histogram windowHistogram;

    private final LongAdder windowSent = new LongAdder();
    private final LongAdder windowAcked = new LongAdder();
    private final LongAdder windowFailed = new LongAdder();
    private final LongAdder windowBytes = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> windowErrors = new ConcurrentHashMap<>();

    private volatile long windowStartMs;

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledTask;

    public AckReporter(Producer<String, String> producer, String controlTopic, String runId,
                        long invocationId, String topic, long reportIntervalMs) {
        this.producer = producer;
        this.controlTopic = controlTopic;
        this.runId = runId;
        this.invocationId = invocationId;
        this.topic = topic;
        this.reportIntervalMs = reportIntervalMs;
        this.windowHistogram = new Histogram(LOWEST_DISCERNIBLE_VALUE_US, HIGHEST_TRACKABLE_VALUE_US, SIGNIFICANT_DIGITS);
        this.windowStartMs = System.currentTimeMillis();
    }

    public void recordAck(long latencyMicros, long recordBytes) {
        synchronized (windowHistogram) {
            windowHistogram.recordValue(clamp(latencyMicros));
        }
        windowAcked.increment();
        windowBytes.add(recordBytes);
    }

    public void recordSent() {
        windowSent.increment();
    }

    public void recordFailure(String exceptionSimpleName) {
        windowFailed.increment();
        windowErrors.computeIfAbsent(exceptionSimpleName, k -> new LongAdder()).increment();
    }

    private static long clamp(long valueMicros) {
        if (valueMicros < LOWEST_DISCERNIBLE_VALUE_US) {
            return LOWEST_DISCERNIBLE_VALUE_US;
        }
        if (valueMicros > HIGHEST_TRACKABLE_VALUE_US) {
            return HIGHEST_TRACKABLE_VALUE_US;
        }
        return valueMicros;
    }

    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ack-reporter-" + runId);
            t.setDaemon(true);
            return t;
        });
        scheduledTask = executor.scheduleAtFixedRate(
                this::publishWindowReportSafely, reportIntervalMs, reportIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void publishWindowReportSafely() {
        try {
            publishWindowReport(false);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to publish periodic control-metrics report", e);
        }
    }

    public synchronized void publishWindowReport(boolean isFinal) {
        long windowEndMs = System.currentTimeMillis();
        AckReport report = new AckReport();
        report.setRunId(runId);
        report.setInvocationId(invocationId);
        report.setTopic(topic);
        report.setWindowStartMs(windowStartMs);
        report.setWindowEndMs(windowEndMs);
        report.setSent(windowSent.sumThenReset());
        report.setAcked(windowAcked.sumThenReset());
        report.setFailed(windowFailed.sumThenReset());
        report.setBytes(windowBytes.sumThenReset());
        report.setErrors(drainErrors());

        Histogram snapshot;
        synchronized (windowHistogram) {
            snapshot = windowHistogram.copy();
            windowHistogram.reset();
        }
        if (snapshot.getTotalCount() > 0) {
            report.setAckMeanMs(snapshot.getMean() / 1000.0);
            report.setAckP50Ms(snapshot.getValueAtPercentile(50.0) / 1000.0);
            report.setAckP99Ms(snapshot.getValueAtPercentile(99.0) / 1000.0);
            report.setAckMaxMs(snapshot.getMaxValue() / 1000.0);
        } else {
            report.setAckMeanMs(0.0);
            report.setAckP50Ms(0.0);
            report.setAckP99Ms(0.0);
            report.setAckMaxMs(0.0);
        }
        report.setHdrBase64(encodeHistogram(snapshot));
        report.setFinal(isFinal);

        windowStartMs = windowEndMs;

        try {
            String json = mapper.writeValueAsString(report);
            producer.send(new ProducerRecord<>(controlTopic, runId, json));
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to serialize/send control-metrics report", e);
        }
    }

    private Map<String, Long> drainErrors() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        windowErrors.forEach((k, v) -> {
            long value = v.sumThenReset();
            if (value != 0) {
                result.put(k, value);
            }
        });
        return result;
    }

    public static String encodeHistogram(Histogram histogram) {
        ByteBuffer buffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
        int len = histogram.encodeIntoCompressedByteBuffer(buffer);
        byte[] bytes = new byte[len];
        buffer.rewind();
        buffer.get(bytes, 0, len);
        return Base64.getEncoder().encodeToString(bytes);
    }

    public void stop() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }
}
