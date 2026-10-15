package dev.learning.kafka.own.processing;

import dev.learning.kafka.own.common.Constants;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetTracker {

    private final Map<Integer, Long> partitionOffsets = new HashMap<>();

    public void done(int partition, long offset) {
        partitionOffsets.put(partition, offset);
    }

    public Map<TopicPartition, OffsetAndMetadata> snapshot() {
        Map<TopicPartition, OffsetAndMetadata> snap = new HashMap<>();
        partitionOffsets.forEach((part, off) -> {
            snap.put(new TopicPartition(Constants.ORDERS_TOPIC, part), new OffsetAndMetadata(off));
        });
        return snap;
    }

    public Map<TopicPartition, OffsetAndMetadata> forPartitions(List<Integer> partitions) {
        Map<TopicPartition, OffsetAndMetadata> snap = new HashMap<>();
        partitions.forEach(part -> {
            Long offset = partitionOffsets.get(part);
            if (offset != null) {
                snap.put(new TopicPartition(Constants.ORDERS_TOPIC, part), new OffsetAndMetadata(offset));
            }
        });
        return snap;
    }

    public void remove(List<Integer> partitions) {
        partitions.forEach(partitionOffsets::remove);
    }
}
