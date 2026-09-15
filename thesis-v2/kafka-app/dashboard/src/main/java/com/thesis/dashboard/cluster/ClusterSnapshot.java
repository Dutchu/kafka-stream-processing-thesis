package com.thesis.dashboard.cluster;

import java.util.List;
import java.util.Map;

public record ClusterSnapshot(
        long ts,
        List<BrokerInfo> brokers,
        List<PartitionInfo> partitions,
        List<PerBrokerAgg> perBroker,
        long totalMsgs,
        double lambdaLeo,
        long adminErrors
) {

    public record BrokerInfo(int brokerId, String host) {
    }

    public record PartitionInfo(
            String topic,
            int partition,
            int leader,
            List<Integer> replicas,
            List<Integer> isr,
            long earliest,
            long latest
    ) {
        public long msgs() {
            return latest - earliest;
        }
    }

    public record PerBrokerAgg(int brokerId, Map<String, TopicRole> byTopic) {
    }

    public record TopicRole(
            long leaderMsgs,
            long followerMsgs,
            int leaderPartitions,
            int replicaPartitions
    ) {
    }

    public static ClusterSnapshot empty() {
        return new ClusterSnapshot(System.currentTimeMillis(), List.of(), List.of(), List.of(), 0L, Double.NaN, 0L);
    }
}
