package com.thesis.dashboard.cluster;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterInspector implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ClusterInspector.class.getName());
    private static final long LOG_THROTTLE_MS = 10_000L;

    private final AdminClient admin;
    private final AtomicReference<List<String>> topics;
    private final long adminPollMs;
    private final long topicDescribeMs;

    private volatile List<ClusterSnapshot.BrokerInfo> lastBrokers = List.of();
    private volatile Map<TopicPartition, PartitionMeta> lastTopology = Map.of();

    private volatile long lastOffsetsTs = 0L;
    private volatile long lastOffsetsSum = 0L;

    private final AtomicReference<ClusterSnapshot> latest = new AtomicReference<>(ClusterSnapshot.empty());
    private final AtomicLong adminErrors = new AtomicLong(0);
    private volatile long lastErrorLogMs = 0L;

    private record PartitionMeta(int leader, List<Integer> replicas, List<Integer> isr) {
    }

    public ClusterInspector(String bootstrapServers, List<String> topics, long adminPollMs, long topicDescribeMs) {
        this.topics = new AtomicReference<>(List.copyOf(topics));
        this.adminPollMs = adminPollMs;
        this.topicDescribeMs = topicDescribeMs;
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 8_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 9_000);
        this.admin = AdminClient.create(props);
    }

    public void setTopics(List<String> newTopics) {
        List<String> safe = (newTopics == null) ? List.of() : List.copyOf(newTopics);
        topics.set(safe);
        LOG.info("ClusterInspector now watching topics " + safe);
    }

    public List<String> watchedTopics() {
        return topics.get();
    }

    public void refreshTopology() {
        try {
            DescribeClusterResult clusterResult = admin.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get(8, TimeUnit.SECONDS);
            List<ClusterSnapshot.BrokerInfo> brokers = new ArrayList<>();
            for (Node n : nodes) {
                brokers.add(new ClusterSnapshot.BrokerInfo(n.id(), n.host() + ":" + n.port()));
            }
            brokers.sort((a, b) -> Integer.compare(a.brokerId(), b.brokerId()));
            this.lastBrokers = List.copyOf(brokers);

            List<String> topics = this.topics.get();
            DescribeTopicsResult topicsResult = admin.describeTopics(topics);
            Map<String, KafkaFuture<TopicDescription>> byName = topicsResult.topicNameValues();
            Map<TopicPartition, PartitionMeta> topology = new HashMap<>();
            for (String topic : topics) {
                KafkaFuture<TopicDescription> future = byName.get(topic);
                if (future == null) continue;
                try {
                    TopicDescription desc = future.get(8, TimeUnit.SECONDS);
                    for (TopicPartitionInfo p : desc.partitions()) {
                        int leader = p.leader() != null ? p.leader().id() : -1;
                        List<Integer> replicas = new ArrayList<>();
                        for (Node n : p.replicas()) replicas.add(n.id());
                        List<Integer> isr = new ArrayList<>();
                        for (Node n : p.isr()) isr.add(n.id());
                        topology.put(new TopicPartition(topic, p.partition()),
                                new PartitionMeta(leader, replicas, isr));
                    }
                } catch (ExecutionException | TimeoutException | InterruptedException e) {
                    logThrottled("describeTopics(" + topic + ") failed: " + e.getMessage());
                }
            }
            this.lastTopology = Map.copyOf(topology);
        } catch (Exception e) {
            adminErrors.incrementAndGet();
            logThrottled("refreshTopology failed: " + e.getMessage());
        }
    }

    public void refreshOffsetsAndPublish() {
        try {
            Map<TopicPartition, PartitionMeta> topology = this.lastTopology;
            if (topology.isEmpty()) {
                latest.set(new ClusterSnapshot(System.currentTimeMillis(), lastBrokers, List.of(), List.of(),
                        0L, Double.NaN, adminErrors.get()));
                return;
            }

            Map<TopicPartition, OffsetSpec> earliestSpecs = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
            for (TopicPartition tp : topology.keySet()) {
                earliestSpecs.put(tp, OffsetSpec.earliest());
                latestSpecs.put(tp, OffsetSpec.latest());
            }

            ListOffsetsResult earliestResult = admin.listOffsets(earliestSpecs);
            ListOffsetsResult latestResult = admin.listOffsets(latestSpecs);

            Map<TopicPartition, Long> earliest = new HashMap<>();
            Map<TopicPartition, Long> latestMap = new HashMap<>();
            for (TopicPartition tp : topology.keySet()) {
                try {
                    earliest.put(tp, earliestResult.partitionResult(tp).get(8, TimeUnit.SECONDS).offset());
                } catch (Exception e) {
                    earliest.put(tp, 0L);
                    logThrottled("listOffsets(earliest," + tp + ") failed: " + e.getMessage());
                }
                try {
                    latestMap.put(tp, latestResult.partitionResult(tp).get(8, TimeUnit.SECONDS).offset());
                } catch (Exception e) {
                    latestMap.put(tp, earliest.getOrDefault(tp, 0L));
                    logThrottled("listOffsets(latest," + tp + ") failed: " + e.getMessage());
                }
            }

            List<ClusterSnapshot.PartitionInfo> partitions = new ArrayList<>();
            long totalMsgs = 0L;
            long sumLatest = 0L;
            for (Map.Entry<TopicPartition, PartitionMeta> e : topology.entrySet()) {
                TopicPartition tp = e.getKey();
                PartitionMeta meta = e.getValue();
                long earl = earliest.getOrDefault(tp, 0L);
                long last = latestMap.getOrDefault(tp, earl);
                sumLatest += last;
                long msgs = Math.max(0L, last - earl);
                totalMsgs += msgs;
                partitions.add(new ClusterSnapshot.PartitionInfo(
                        tp.topic(), tp.partition(), meta.leader(), meta.replicas(), meta.isr(), earl, last));
            }

            long now = System.currentTimeMillis();
            double lambdaLeo = Double.NaN;
            if (lastOffsetsTs > 0) {
                double dtSec = (now - lastOffsetsTs) / 1000.0;
                if (dtSec > 0) {
                    long delta = sumLatest - lastOffsetsSum;
                    lambdaLeo = delta / dtSec;
                    if (lambdaLeo < 0) lambdaLeo = 0;
                }
            }
            lastOffsetsTs = now;
            lastOffsetsSum = sumLatest;

            List<ClusterSnapshot.PerBrokerAgg> perBroker = aggregatePerBroker(lastBrokers, partitions);

            latest.set(new ClusterSnapshot(now, lastBrokers, partitions, perBroker, totalMsgs, lambdaLeo, adminErrors.get()));
        } catch (Exception e) {
            adminErrors.incrementAndGet();
            logThrottled("refreshOffsets failed: " + e.getMessage());
        }
    }

    static List<ClusterSnapshot.PerBrokerAgg> aggregatePerBroker(
            List<ClusterSnapshot.BrokerInfo> brokers, List<ClusterSnapshot.PartitionInfo> partitions) {
        Map<Integer, Map<String, long[]>> acc = new HashMap<>();
        for (ClusterSnapshot.BrokerInfo b : brokers) {
            acc.put(b.brokerId(), new HashMap<>());
        }
        for (ClusterSnapshot.PartitionInfo p : partitions) {
            for (int replicaId : p.replicas()) {
                Map<String, long[]> byTopic = acc.computeIfAbsent(replicaId, k -> new HashMap<>());
                long[] counts = byTopic.computeIfAbsent(p.topic(), k -> new long[4]);
                boolean isLeader = replicaId == p.leader();
                if (isLeader) {
                    counts[0] += p.msgs();
                    counts[2] += 1;
                } else {
                    counts[1] += p.msgs();
                    counts[3] += 1;
                }
            }
        }
        List<ClusterSnapshot.PerBrokerAgg> result = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, long[]>> e : acc.entrySet()) {
            Map<String, ClusterSnapshot.TopicRole> byTopic = new HashMap<>();
            for (Map.Entry<String, long[]> te : e.getValue().entrySet()) {
                long[] c = te.getValue();
                byTopic.put(te.getKey(), new ClusterSnapshot.TopicRole(c[0], c[1], (int) c[2], (int) c[3]));
            }
            result.add(new ClusterSnapshot.PerBrokerAgg(e.getKey(), byTopic));
        }
        result.sort((a, b) -> Integer.compare(a.brokerId(), b.brokerId()));
        return result;
    }

    private void logThrottled(String message) {
        long now = System.currentTimeMillis();
        if (now - lastErrorLogMs > LOG_THROTTLE_MS) {
            LOG.log(Level.WARNING, "ClusterInspector: {0}", message);
            lastErrorLogMs = now;
        }
    }

    public ClusterSnapshot snapshot() {
        return latest.get();
    }

    public Map<String, Object> purgeTopic(String name) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (name == null || name.isBlank()) {
            out.put("status", "error");
            out.put("message", "topic name required");
            return out;
        }
        if (name.startsWith("__")) {
            out.put("status", "error");
            out.put("message", "refusing to purge system topic " + name);
            return out;
        }
        try {
            TopicDescription desc = admin.describeTopics(List.of(name))
                    .topicNameValues().get(name).get(10, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
            for (TopicPartitionInfo p : desc.partitions()) {
                specs.put(new TopicPartition(name, p.partition()), OffsetSpec.latest());
            }
            ListOffsetsResult leo = admin.listOffsets(specs);
            Map<TopicPartition, Long> ends = new HashMap<>();
            for (TopicPartition tp : specs.keySet()) {
                ends.put(tp, leo.partitionResult(tp).get(10, TimeUnit.SECONDS).offset());
            }
            Map<TopicPartition, org.apache.kafka.clients.admin.RecordsToDelete> del = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
                del.put(e.getKey(), org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset(e.getValue()));
            }
            admin.deleteRecords(del).all().get(30, TimeUnit.SECONDS);
            Map<String, Long> truncated = new LinkedHashMap<>();
            for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
                truncated.put(String.valueOf(e.getKey().partition()), e.getValue());
            }
            out.put("status", "ok");
            out.put("truncated", truncated);
        } catch (Exception e) {
            out.put("status", "error");
            out.put("message", String.valueOf(e.getMessage()));
        }
        return out;
    }

    public Map<String, Object> deleteTopic(String name) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (name == null || name.isBlank()) {
            out.put("status", "error");
            out.put("message", "topic name required");
            return out;
        }
        if (name.startsWith("__")) {
            out.put("status", "error");
            out.put("message", "refusing to delete system topic " + name);
            return out;
        }
        try {
            admin.deleteTopics(List.of(name)).all().get(30, TimeUnit.SECONDS);
            out.put("status", "ok");
            out.put("deleted", name);
        } catch (Exception e) {
            out.put("status", "error");
            out.put("message", String.valueOf(e.getMessage()));
        }
        return out;
    }

    public List<Map<String, Object>> listAllTopics() {
        List<Map<String, Object>> out = new ArrayList<>();
        final Collection<String> names;
        try {
            names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", "(listTopics failed)");
            row.put("error", String.valueOf(e.getMessage()));
            out.add(row);
            return out;
        }
        Map<String, TopicDescription> descs = new HashMap<>();
        try {
            Map<String, KafkaFuture<TopicDescription>> futures =
                    admin.describeTopics(new ArrayList<>(names)).topicNameValues();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> e : futures.entrySet()) {
                try {
                    descs.put(e.getKey(), e.getValue().get(8, TimeUnit.SECONDS));
                } catch (Exception ex) {
                    LOG.log(Level.WARNING, "listAllTopics: describe {0} failed: {1}",
                            new Object[]{e.getKey(), ex.getMessage()});
                }
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "listAllTopics: describeTopics failed: {0}", e.getMessage());
        }
        Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
        for (Map.Entry<String, TopicDescription> e : descs.entrySet()) {
            for (TopicPartitionInfo p : e.getValue().partitions()) {
                latestSpecs.put(new TopicPartition(e.getKey(), p.partition()), OffsetSpec.latest());
            }
        }
        Map<TopicPartition, Long> leo = new HashMap<>();
        Map<TopicPartition, Long> earliest = new HashMap<>();
        if (!latestSpecs.isEmpty()) {
            try {
                Map<TopicPartition, OffsetSpec> earliestSpecs = new HashMap<>();
                for (TopicPartition tp : latestSpecs.keySet()) {
                    earliestSpecs.put(tp, OffsetSpec.earliest());
                }
                ListOffsetsResult resLatest = admin.listOffsets(latestSpecs);
                ListOffsetsResult resEarliest = admin.listOffsets(earliestSpecs);
                for (TopicPartition tp : latestSpecs.keySet()) {
                    try {
                        leo.put(tp, resLatest.partitionResult(tp).get(8, TimeUnit.SECONDS).offset());
                    } catch (Exception ex) {
                        LOG.log(Level.WARNING, "listAllTopics: LEO {0} failed: {1}",
                                new Object[]{tp, ex.getMessage()});
                    }
                    try {
                        earliest.put(tp, resEarliest.partitionResult(tp).get(8, TimeUnit.SECONDS).offset());
                    } catch (Exception ex) {
                        LOG.log(Level.WARNING, "listAllTopics: earliest {0} failed: {1}",
                                new Object[]{tp, ex.getMessage()});
                    }
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "listAllTopics: listOffsets failed: {0}", e.getMessage());
            }
        }
        List<String> sorted = new ArrayList<>(descs.keySet());
        Collections.sort(sorted);
        for (String name : sorted) {
            TopicDescription d = descs.get(name);
            long msgs = 0;
            Map<Integer, Integer> leaders = new HashMap<>();
            for (TopicPartitionInfo p : d.partitions()) {
                TopicPartition tp = new TopicPartition(name, p.partition());
                Long latest = leo.get(tp);
                Long first = earliest.get(tp);
                if (latest != null && first != null) {
                    msgs += Math.max(0L, latest - first);
                } else if (latest != null) {
                    msgs += latest;
                }
                if (p.leader() != null) leaders.merge(p.leader().id(), 1, Integer::sum);
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", name);
            row.put("partitions", d.partitions().size());
            row.put("rf", d.partitions().isEmpty() ? 0
                    : d.partitions().get(0).replicas().size());
            row.put("msgs", msgs);
            row.put("leaders", leaders);
            out.add(row);
        }
        return out;
    }

    public long adminErrors() {
        return adminErrors.get();
    }

    public long adminPollMs() {
        return adminPollMs;
    }

    public long topicDescribeMs() {
        return topicDescribeMs;
    }

    @Override
    public void close() {
        try {
            admin.close(Duration.ofSeconds(5));
        } catch (Exception ignored) {
        }
    }
}
