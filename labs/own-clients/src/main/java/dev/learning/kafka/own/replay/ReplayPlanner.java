package dev.learning.kafka.own.replay;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReplayPlanner {

    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private final Instant from;

    public ReplayPlanner(String topic,
                         Instant from,
                         KafkaConsumer<String, String> consumer) {
        this.topic = topic;
        this.consumer = consumer;
        this.from = from;
    }

    public List<PartitionPlan> buildPlans() {
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        if (partitions.isEmpty()) {
            return List.of();
        }
        List<PartitionPlan> plans = new ArrayList<>();

        Map<TopicPartition, Long> tpTimestamps = partitions.stream().collect(Collectors.toMap(
                pi -> new TopicPartition(pi.topic(), pi.partition()),
                pi -> from.toEpochMilli()));
        Map<TopicPartition, OffsetAndTimestamp> startIntervals = consumer.offsetsForTimes(tpTimestamps);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tpTimestamps.keySet());
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(tpTimestamps.keySet());
        for (PartitionInfo pi : partitions) {
            TopicPartition tp = new TopicPartition(pi.topic(), pi.partition());
            PartitionPlan pp = PartitionPlan.of(tp,
                                                startIntervals.get(tp),
                                                endOffsets.get(tp),
                                                beginningOffsets.get(tp),
                                                from.toEpochMilli());
            plans.add(pp);
        }
        return plans;
    }
}
