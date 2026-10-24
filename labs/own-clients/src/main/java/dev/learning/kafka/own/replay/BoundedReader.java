package dev.learning.kafka.own.replay;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BoundedReader {
    private static final Logger log = LoggerFactory.getLogger(BoundedReader.class);

    private final Consumer<String, String> kafkaConsumer;
    private final List<PartitionPlan> plans;
    private final Map<TopicPartition, PartitionPlan> partitionToPlan;
    private final long maxWaitMs;
    private final long from;
    private final long to;
    private final ReplayWriter writer;
    private final Map<TopicPartition, Long> uploaded = new HashMap<>();
    private final Map<TopicPartition, Long> read = new HashMap<>();

    public BoundedReader(ReplayWriter writer,
                         Consumer<String, String> kafkaConsumer,
                         List<PartitionPlan> plans,
                         long maxWaitMs,
                         long from,
                         long to) {
        this.kafkaConsumer = kafkaConsumer;
        this.plans = plans;
        this.partitionToPlan = plans.stream()
                                    .collect(Collectors.toMap(PartitionPlan::partition, Function.identity()));
        this.maxWaitMs = maxWaitMs;
        this.from = from;
        this.to = to;
        this.writer = writer;
    }

    public ReadResult getResult() {
        return new ReadResult(read, uploaded);
    }

    /**
     * @return rue, если все назначенные партиции дочитаны до своих границ»
     */
    public boolean read() {

        List<TopicPartition> partitions = plans.stream()
                                               .filter(p -> p.state() != PartitionPlan.PartitionState.EMPTY)
                                               .map(PartitionPlan::partition)
                                               .toList();
        kafkaConsumer.assign(partitions);
        plans.stream()
             .filter(p -> p.state() != PartitionPlan.PartitionState.EMPTY)
             .forEach(plan -> kafkaConsumer.seek(
                              plan.partition(),
                              plan.startInterval().offset())
                     );
        Set<TopicPartition> readable = new HashSet<>(partitions);
        long start = System.currentTimeMillis();
        try {
            while (!readable.isEmpty() && start + maxWaitMs > System.currentTimeMillis()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(200, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> record : records) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    long offset = record.offset();
                    PartitionPlan pp = partitionToPlan.get(partition);
                    if (pp.startInterval().offset() <= offset
                            && (from <= record.timestamp() && record.timestamp() < to)
                            && pp.endOffset() > offset) {
                        writer.upload(new ReplayWriter.ReplayOrderRecord(partition.partition(),
                                                                         offset,
                                                                         record.timestamp(),
                                                                         record.value())
                                     );
                        uploaded.merge(partition, 1L, Long::sum);
                    }
                    read.merge(partition, 1L, Long::sum);
                }
                readable.removeIf(retain ->
                                  {
                                      boolean toRemove = kafkaConsumer.position(retain) >= partitionToPlan.get(retain)
                                                                                                          .endOffset();
                                      if (toRemove) {
                                          kafkaConsumer.pause(List.of(retain));
                                      }
                                      return toRemove;
                                  });
            }
        } catch (WakeupException we) {
            log.error("Kafka consumer poll() был остановлен");
        }

        if (!readable.isEmpty()) {
            log.warn("Остались недочитанные партиции {}", readable.size());
        }
        return readable.isEmpty();
    }

    public record ReadResult(Map<TopicPartition, Long> read,
                             Map<TopicPartition, Long> uploaded) {

    }
}
