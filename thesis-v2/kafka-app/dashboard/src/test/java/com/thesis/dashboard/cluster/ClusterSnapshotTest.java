package com.thesis.dashboard.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterSnapshotTest {

    @Test
    void aggregatesLeaderAndFollowerMessagesPerBrokerAndTopic() {
        List<ClusterSnapshot.BrokerInfo> brokers = List.of(
                new ClusterSnapshot.BrokerInfo(1, "kafka-1:9092"),
                new ClusterSnapshot.BrokerInfo(2, "kafka-2:9092"),
                new ClusterSnapshot.BrokerInfo(3, "kafka-3:9092")
        );

        List<ClusterSnapshot.PartitionInfo> partitions = List.of(
                new ClusterSnapshot.PartitionInfo("weather-rain", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 1000),
                new ClusterSnapshot.PartitionInfo("weather-rain", 1, 2, List.of(2, 3, 1), List.of(2, 3, 1), 0, 500),
                new ClusterSnapshot.PartitionInfo("weather-temp", 0, 3, List.of(3, 1, 2), List.of(3, 1, 2), 0, 200)
        );

        List<ClusterSnapshot.PerBrokerAgg> result = ClusterInspector.aggregatePerBroker(brokers, partitions);
        assertEquals(3, result.size());

        Map<Integer, ClusterSnapshot.PerBrokerAgg> byId = new java.util.HashMap<>();
        result.forEach(agg -> byId.put(agg.brokerId(), agg));

        ClusterSnapshot.TopicRole b1Rain = byId.get(1).byTopic().get("weather-rain");
        assertEquals(1000L, b1Rain.leaderMsgs());
        assertEquals(500L, b1Rain.followerMsgs());
        assertEquals(1, b1Rain.leaderPartitions());
        assertEquals(1, b1Rain.replicaPartitions());

        ClusterSnapshot.TopicRole b1Temp = byId.get(1).byTopic().get("weather-temp");
        assertEquals(0L, b1Temp.leaderMsgs());
        assertEquals(200L, b1Temp.followerMsgs());
        assertEquals(0, b1Temp.leaderPartitions());
        assertEquals(1, b1Temp.replicaPartitions());

        ClusterSnapshot.TopicRole b2Rain = byId.get(2).byTopic().get("weather-rain");
        assertEquals(500L, b2Rain.leaderMsgs());
        assertEquals(1000L, b2Rain.followerMsgs());

        ClusterSnapshot.TopicRole b3Rain = byId.get(3).byTopic().get("weather-rain");
        assertEquals(0L, b3Rain.leaderMsgs());
        assertEquals(1500L, b3Rain.followerMsgs());
        assertEquals(0, b3Rain.leaderPartitions());
        assertEquals(2, b3Rain.replicaPartitions());

        ClusterSnapshot.TopicRole b3Temp = byId.get(3).byTopic().get("weather-temp");
        assertEquals(200L, b3Temp.leaderMsgs());
        assertEquals(0L, b3Temp.followerMsgs());
        assertEquals(1, b3Temp.leaderPartitions());
        assertEquals(0, b3Temp.replicaPartitions());
    }

    @Test
    void singleBrokerConfigHasOnlyLeaderRoleAndNoMissingBrokerErrors() {
        List<ClusterSnapshot.BrokerInfo> brokers = List.of(new ClusterSnapshot.BrokerInfo(1, "kafka-1:9092"));
        List<ClusterSnapshot.PartitionInfo> partitions = List.of(
                new ClusterSnapshot.PartitionInfo("weather-rain", 0, 1, List.of(1), List.of(1), 0, 300)
        );

        List<ClusterSnapshot.PerBrokerAgg> result = ClusterInspector.aggregatePerBroker(brokers, partitions);
        assertEquals(1, result.size());
        ClusterSnapshot.TopicRole role = result.get(0).byTopic().get("weather-rain");
        assertEquals(300L, role.leaderMsgs());
        assertEquals(0L, role.followerMsgs());
        assertEquals(1, role.leaderPartitions());
        assertEquals(0, role.replicaPartitions());
    }

    @Test
    void emptyPartitionsProduceEmptyPerBrokerTopicMaps() {
        List<ClusterSnapshot.BrokerInfo> brokers = List.of(new ClusterSnapshot.BrokerInfo(1, "kafka-1:9092"));
        List<ClusterSnapshot.PerBrokerAgg> result = ClusterInspector.aggregatePerBroker(brokers, List.of());
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).byTopic().size());
    }
}
