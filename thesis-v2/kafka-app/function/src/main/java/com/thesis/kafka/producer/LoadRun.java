package com.thesis.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafka.model.RunSummary;
import com.thesis.kafka.model.WeatherEvent;
import com.thesis.kafka.strategy.EventStrategy;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LoadRun {

    private static final Logger LOG = Logger.getLogger(LoadRun.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RunParams params;
    private final EventStrategy strategy;

    private final Histogram overallHistogram = new Histogram(
            AckReporter.LOWEST_DISCERNIBLE_VALUE_US,
            AckReporter.HIGHEST_TRACKABLE_VALUE_US,
            AckReporter.SIGNIFICANT_DIGITS);

    private final LongAdder sentCount = new LongAdder();
    private final LongAdder ackedCount = new LongAdder();
    private final LongAdder failedCount = new LongAdder();
    private final LongAdder totalBytes = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> errorCounts = new ConcurrentHashMap<>();
    private final Set<String> loggedErrorTypes = ConcurrentHashMap.newKeySet();

    public LoadRun(RunParams params, EventStrategy strategy) {
        this.params = params;
        this.strategy = strategy;
    }

    public RunSummary execute() {
        long startEpochMs = System.currentTimeMillis();
        LOG.info(() -> String.format(
                "Starting run runId=%s invocationId=%d topic=%s durationSec=%s count=%s ratePerSec=%.2f intensity=%.2f reportIntervalMs=%d",
                params.runId(), params.invocationId(), strategy.topicName(),
                params.durationSec() != null ? params.durationSec().toString() : "n/a",
                params.count() != null ? params.count().toString() : "n/a",
                params.ratePerSec(), params.intensity(), params.reportIntervalMs()));

        Properties props = buildProducerProperties();
        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            AckReporter reporter = new AckReporter(producer, params.controlTopic(), params.runId(),
                    params.invocationId(), strategy.topicName(), params.reportIntervalMs());
            reporter.start();

            RateLimiter rateLimiter = new RateLimiter(params.ratePerSec());
            runSendLoop(producer, reporter, rateLimiter);

            producer.flush();
            reporter.publishWindowReport(true);
            reporter.stop();
            producer.flush();
        } finally {
            producer.close(Duration.ofSeconds(30));
        }

        long endEpochMs = System.currentTimeMillis();
        RunSummary summary = buildSummary(startEpochMs, endEpochMs);

        LOG.info(() -> String.format(
                "Finished run runId=%s invocationId=%d topic=%s sent=%d acked=%d failed=%d achievedRatePerSec=%.2f",
                params.runId(), params.invocationId(), strategy.topicName(),
                summary.getSent(), summary.getAcked(), summary.getFailed(), summary.getAchievedRatePerSec()));

        return summary;
    }

    private Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, params.acks());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "50");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "all".equals(params.acks()));
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fn-" + params.runId() + "-" + params.invocationId());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        return props;
    }

    private void runSendLoop(Producer<String, String> producer, AckReporter reporter,
                              RateLimiter rateLimiter) {
        Long durationSec = params.durationSec();
        Long count = params.count();
        long deadlineNanos = durationSec != null
                ? System.nanoTime() + Duration.ofSeconds(durationSec).toNanos()
                : Long.MAX_VALUE;
        long remainingCount = durationSec != null ? Long.MAX_VALUE : count;

        long produced = 0;
        while (true) {
            if (durationSec != null) {
                if (System.nanoTime() >= deadlineNanos) {
                    break;
                }
            } else {
                if (produced >= remainingCount) {
                    break;
                }
            }

            rateLimiter.acquire();

            WeatherEvent event = strategy.generate(params.zoneId(), params.intensity());
            final String value;
            try {
                value = MAPPER.writeValueAsString(event);
            } catch (Exception e) {
                onFailure(reporter, e);
                produced++;
                continue;
            }

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(strategy.topicName(), params.zoneId() + "-" + produced, value);

            long t0 = System.nanoTime();
            sentCount.increment();
            reporter.recordSent();

            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                if (exception != null) {
                    onFailure(reporter, exception);
                    return;
                }
                long latencyMicros = (System.nanoTime() - t0) / 1_000L;
                long recordBytes = metadata.serializedKeySize() + metadata.serializedValueSize();
                ackedCount.increment();
                totalBytes.add(recordBytes);
                synchronized (overallHistogram) {
                    overallHistogram.recordValue(clampToHistogram(latencyMicros));
                }
                reporter.recordAck(latencyMicros, recordBytes);
            });

            produced++;
        }
    }

    private void onFailure(AckReporter reporter, Exception exception) {
        String simpleName = exception.getClass().getSimpleName();
        failedCount.increment();
        errorCounts.computeIfAbsent(simpleName, k -> new LongAdder()).increment();
        reporter.recordFailure(simpleName);
        if (loggedErrorTypes.add(simpleName)) {
            LOG.log(Level.WARNING, "First occurrence of error type " + simpleName
                    + " in run " + params.runId(), exception);
        }
    }

    private static long clampToHistogram(long valueMicros) {
        if (valueMicros < AckReporter.LOWEST_DISCERNIBLE_VALUE_US) {
            return AckReporter.LOWEST_DISCERNIBLE_VALUE_US;
        }
        if (valueMicros > AckReporter.HIGHEST_TRACKABLE_VALUE_US) {
            return AckReporter.HIGHEST_TRACKABLE_VALUE_US;
        }
        return valueMicros;
    }

    private RunSummary buildSummary(long startEpochMs, long endEpochMs) {
        RunSummary summary = new RunSummary();
        summary.setStatus("ok");
        summary.setRunId(params.runId());
        summary.setInvocationId(params.invocationId());
        summary.setTopic(strategy.topicName());
        summary.setStartEpochMs(startEpochMs);
        summary.setEndEpochMs(endEpochMs);

        long sent = sentCount.sum();
        long acked = ackedCount.sum();
        long failed = failedCount.sum();
        long bytes = totalBytes.sum();

        summary.setSent(sent);
        summary.setAcked(acked);
        summary.setFailed(failed);
        summary.setBytes(bytes);
        summary.setAvgRecordBytes(acked > 0 ? Math.round((double) bytes / acked) : 0L);

        Map<String, Long> errors = new ConcurrentHashMap<>();
        errorCounts.forEach((k, v) -> errors.put(k, v.sum()));
        summary.setErrors(errors);

        RunSummary.AckStats ackStats = new RunSummary.AckStats();
        synchronized (overallHistogram) {
            ackStats.setCount(overallHistogram.getTotalCount());
            if (overallHistogram.getTotalCount() > 0) {
                ackStats.setMeanMs(overallHistogram.getMean() / 1000.0);
                ackStats.setP50Ms(overallHistogram.getValueAtPercentile(50.0) / 1000.0);
                ackStats.setP95Ms(overallHistogram.getValueAtPercentile(95.0) / 1000.0);
                ackStats.setP99Ms(overallHistogram.getValueAtPercentile(99.0) / 1000.0);
                ackStats.setMaxMs(overallHistogram.getMaxValue() / 1000.0);
            }
        }
        summary.setAck(ackStats);

        double elapsedSec = Math.max(1e-6, (endEpochMs - startEpochMs) / 1000.0);
        summary.setAchievedRatePerSec(acked / elapsedSec);

        String instanceId = System.getenv().getOrDefault("K_REVISION", null);
        if (instanceId == null) {
            try {
                instanceId = java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                instanceId = "unknown";
            }
        }
        summary.setInstanceId(instanceId);

        return summary;
    }
}
