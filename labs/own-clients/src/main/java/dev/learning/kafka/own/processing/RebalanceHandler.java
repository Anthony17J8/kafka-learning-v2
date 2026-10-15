package dev.learning.kafka.own.processing;

import dev.learning.kafka.own.common.Constants;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RebalanceHandler implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(RebalanceHandler.class);

    private final KafkaConsumer<String, String> consumer;
    private final OffsetTracker tracker;

    public RebalanceHandler(KafkaConsumer<String, String> consumer, OffsetTracker tracker) {
        this.consumer = consumer;
        this.tracker = tracker;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Map<String, List<TopicPartition>> groups = partitions.stream()
                                                             .collect(Collectors.groupingBy(TopicPartition::topic));
        groups.forEach((topic, tp) -> {
            log.info("ОТОЗВАНЫ(topic={}), [{}]: ", topic, tp.stream()
                                                            .map(TopicPartition::partition)
                                                            .toList());
        });
        List<TopicPartition> topicPartitions = groups.get(Constants.ORDERS_TOPIC);
        if (topicPartitions != null) {
            List<Integer> revoked = topicPartitions.stream()
                                                   .map(TopicPartition::partition)
                                                   .toList();
            if (consumer != null) {
                try {
                    consumer.commitSync(tracker.forPartitions(revoked));
                } catch (WakeupException we) {
                    throw we;
                } catch (RuntimeException exc) {
                    log.error("Ошибка при коммите при отзыве партиций {} для топика {}",
                              revoked,
                              Constants.ORDERS_TOPIC,
                              exc);
                }
                tracker.remove(revoked);
            }
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        partitions.forEach(tp -> {
            log.info("НАЗНАЧЕНЫ: Topic={}, partition={}", tp.topic(), tp.partition());
        });

    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        Map<String, List<TopicPartition>> groups = partitions.stream()
                                                             .collect(Collectors.groupingBy(TopicPartition::topic));
        groups.forEach((topic, tp) -> {
            log.info("ПОТЕРЯНЫ(topic={}), [{}]: ", topic, tp.stream()
                                                            .map(TopicPartition::partition)
                                                            .toList());
        });

        List<TopicPartition> topicPartitions = groups.get(Constants.ORDERS_TOPIC);
        if (topicPartitions != null) {
            List<Integer> lost = topicPartitions.stream()
                                                .map(TopicPartition::partition)
                                                .toList();
            tracker.remove(lost);
        }
    }
}
