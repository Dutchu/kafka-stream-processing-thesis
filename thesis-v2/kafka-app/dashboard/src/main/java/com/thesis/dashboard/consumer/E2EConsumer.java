package com.thesis.dashboard.consumer;

import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class E2EConsumer implements Runnable {

    private static final Logger LOG = Logger.getLogger(E2EConsumer.class.getName());
    private static final long LOG_THROTTLE_MS = 10_000L;

    private static final long HIST_LOWEST_US = 1L;
    private static final long HIST_HIGHEST_US = 60_000_000L;
    private static final int HIST_SIG_DIGITS = 3;

    private static final long BIN_MS = 100L;
    private static final int BIN_WINDOW = 600;

    private final String bootstrapServers;
    private final List<String> initialTopics;
    private final AtomicReference<List<String>> watched = new AtomicReference<>();
    private final int sampleEvery;
    private volatile boolean running = true;

    private final AtomicReference<E2ESnapshot> latest = new AtomicReference<>(E2ESnapshot.empty());
    private volatile long lastErrorLogMs = 0L;

    private long sampleCounter = 0L;

    public E2EConsumer(String bootstrapServers, List<String> topics, int sampleEvery) {
        this.bootstrapServers = bootstrapServers;
        this.initialTopics = List.copyOf(topics);
        this.watched.set(this.initialTopics);
        this.sampleEvery = Math.max(1, sampleEvery);
    }

    public void retarget(List<String> newTopics) {
        List<String> safe = (newTopics == null) ? List.of() : List.copyOf(newTopics);
        watched.set(safe);
        LOG.info("E2EConsumer retargeted to topics " + safe);
    }

    public List<String> watchedTopics() {
        List<String> w = watched.get();
        return w == null ? initialTopics : w;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "thesis-dashboard-v2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20_000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);

        Histogram windowHist = new Histogram(HIST_LOWEST_US, HIST_HIGHEST_US, HIST_SIG_DIGITS);
        long windowStart = System.currentTimeMillis();
        long windowCount = 0L;
        long windowBytes = 0L;
        long windowNegLat = 0L;
        Map<String, AtomicLong> cumulativePerTopic = new HashMap<>();

        long[] bins = new long[BIN_WINDOW];
        long binOriginMs = -1L;
        long lastOffsetsPollMs = 0L;

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<String> subscribed = watchedTopics();
            consumer.subscribe(subscribed);
            LOG.info("E2EConsumer started on topics " + subscribed);

            while (running) {
                List<String> want = watchedTopics();
                if (!want.equals(subscribed)) {
                    consumer.subscribe(want);
                    subscribed = want;
                    LOG.info("E2EConsumer re-subscribed to topics " + subscribed);
                }
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
                long now = System.currentTimeMillis();

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    windowCount++;
                    cumulativePerTopic.computeIfAbsent(record.topic(), k -> new AtomicLong(0)).incrementAndGet();

                    int keySize = record.serializedKeySize();
                    int valSize = record.serializedValueSize();
                    windowBytes += Math.max(0, keySize) + Math.max(0, valSize);

                    long binIndex10 = record.timestamp() / BIN_MS;
                    if (binOriginMs < 0) {
                        binOriginMs = binIndex10;
                    }
                    long offset = binIndex10 - binOriginMs;
                    if (offset >= 0 && offset < BIN_WINDOW) {
                        bins[(int) (Math.floorMod(binIndex10, BIN_WINDOW))]++;
                    } else if (offset >= BIN_WINDOW) {
                        long shift = offset - BIN_WINDOW + 1;
                        long clears = Math.min(shift, BIN_WINDOW);
                        for (long s = 0; s < clears; s++) {
                            bins[(int) (Math.floorMod(binOriginMs + s, BIN_WINDOW))] = 0L;
                        }
                        binOriginMs += shift;
                        bins[(int) (Math.floorMod(binIndex10, BIN_WINDOW))]++;
                    }

                    if (sampleEvery == 1 || (windowCount % sampleEvery) == 0) {
                        long lat = now - record.timestamp();
                        if (lat < 0) {
                            windowNegLat++;
                            lat = 0;
                        }
                        if (lat > HIST_HIGHEST_US / 1000) lat = HIST_HIGHEST_US / 1000;
                        windowHist.recordValue(lat * 1000L);
                    }
                }

                if (now - windowStart >= 1000) {
                    double elapsedSec = Math.max(0.001, (now - windowStart) / 1000.0);
                    double rate = windowCount / elapsedSec;
                    double bytesRate = windowBytes / elapsedSec;

                    double mean = windowHist.getTotalCount() > 0 ? windowHist.getMean() / 1000.0 : Double.NaN;
                    double p50 = windowHist.getTotalCount() > 0 ? windowHist.getValueAtPercentile(50) / 1000.0 : Double.NaN;
                    double p99 = windowHist.getTotalCount() > 0 ? windowHist.getValueAtPercentile(99) / 1000.0 : Double.NaN;
                    double max = windowHist.getTotalCount() > 0 ? windowHist.getMaxValue() / 1000.0 : Double.NaN;

                    Map<String, Long> perTopicSnapshot = new HashMap<>();
                    cumulativePerTopic.forEach((k, v) -> perTopicSnapshot.put(k, v.get()));

                    long lag = 0L;
                    if (now - lastOffsetsPollMs >= 1000) {
                        lag = computeLag(consumer);
                        lastOffsetsPollMs = now;
                    } else {
                        lag = latest.get().lag();
                    }

                    double ca2 = computeCa2(bins);

                    if (windowCount > 0 && windowNegLat * 2 > windowCount) {
                        logThrottled("e2e SUSPECT: " + windowNegLat + "/" + windowCount
                                + " negative latencies in window (consumer clock behind record timestamps?)"
                                + " — p50/mean are understated");
                    }

                    latest.set(new E2ESnapshot(rate, bytesRate, mean, p50, p99, max, windowCount,
                            Map.copyOf(perTopicSnapshot), lag, ca2, windowNegLat));

                    windowHist.reset();
                    windowStart = now;
                    windowCount = 0L;
                    windowBytes = 0L;
                    windowNegLat = 0L;
                }
            }
        } catch (Exception e) {
            logThrottled("E2EConsumer terminated: " + e.getMessage());
        }
    }

    private long computeLag(KafkaConsumer<byte[], byte[]> consumer) {
        try {
            Set<TopicPartition> assignment = consumer.assignment();
            if (assignment.isEmpty()) return 0L;
            Map<TopicPartition, Long> ends = consumer.endOffsets(assignment, Duration.ofSeconds(5));
            long total = 0L;
            for (TopicPartition tp : assignment) {
                long end = ends.getOrDefault(tp, 0L);
                long pos;
                try {
                    pos = consumer.position(tp, Duration.ofSeconds(2));
                } catch (Exception e) {
                    pos = end;
                }
                total += Math.max(0L, end - pos);
            }
            return total;
        } catch (Exception e) {
            logThrottled("endOffsets failed: " + e.getMessage());
            return latest.get().lag();
        }
    }

    static double computeCa2(long[] bins) {
        int n = bins.length;
        double sum = 0;
        for (long b : bins) sum += b;
        double mean = sum / n;
        if (mean < 5) return Double.NaN;
        double sqDiff = 0;
        for (long b : bins) {
            double d = b - mean;
            sqDiff += d * d;
        }
        double variance = sqDiff / n;
        return variance / mean;
    }

    private void logThrottled(String message) {
        long now = System.currentTimeMillis();
        if (now - lastErrorLogMs > LOG_THROTTLE_MS) {
            LOG.log(Level.WARNING, "E2EConsumer: {0}", message);
            lastErrorLogMs = now;
        }
    }

    public E2ESnapshot snapshot() {
        return latest.get();
    }

    public void stop() {
        running = false;
    }
}
