package dev.learning.kafka.consumer;

import dev.learning.kafka.common.Env;
import dev.learning.kafka.common.JsonSerde;
import dev.learning.kafka.common.OrderEvent;
import dev.learning.kafka.common.Topics;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Модуль 4: at-least-once с ручным коммитом и корректным поведением при ребалансировке.
 *
 * Запуск нескольких инстансов в одной группе:
 *   mvn -pl lab04-consumer -am compile exec:java \
 *     -Dexec.mainClass=dev.learning.kafka.consumer.ManualCommitConsumer
 *
 * Эксперименты (задания 2-4 модуля 4):
 *   PROCESSING_MS=5000 — обработка дольше max.poll.interval.ms, ловим rebalance storm
 *   GROUP_PROTOCOL=classic vs consumer — сравнить время недоступности при добавлении инстанса
 *   GROUP_INSTANCE_ID=c1 — static membership, сравнить поведение при рестарте
 */
public class ManualCommitConsumer {

    private static final Logger log = LoggerFactory.getLogger(ManualCommitConsumer.class);

    public static void main(String[] args) {
        Properties props = Env.base();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Env.get("GROUP_ID", "group.id", "labs.orders.processor"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                Integer.parseInt(Env.get("MAX_POLL_RECORDS", "max.poll.records", "500")));
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                Integer.parseInt(Env.get("MAX_POLL_INTERVAL_MS", "max.poll.interval.ms", "300000")));

        // KIP-848: новый протокол групп. По умолчанию в Kafka 4.x — "consumer".
        // Переключите на "classic" и сравните время stop-the-world ребалансировки.
        props.put("group.protocol", Env.get("GROUP_PROTOCOL", "group.protocol", "consumer"));

        String instanceId = Env.get("GROUP_INSTANCE_ID", "group.instance.id", "");
        if (!instanceId.isBlank()) {
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        }

        long processingMs = Long.parseLong(Env.get("PROCESSING_MS", "processing.ms", "0"));

        KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(
                props, new StringDeserializer(), JsonSerde.deserializer(OrderEvent.class));

        Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(Topics.ORDERS), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Ключевой момент модуля 4: коммитим ДО того, как партиции уйдут.
                    // Без этого при каждой ребалансировке будет переобработка.
                    log.info("Отзываются партиции: {}", partitions);
                    if (!pending.isEmpty()) {
                        consumer.commitSync(pending);
                        pending.clear();
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("Назначены партиции: {}", partitions);
                }

                @Override
                public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    // Партиции уже отобраны, коммитить бессмысленно и вредно.
                    log.warn("Партиции потеряны без graceful revoke: {}", partitions);
                    pending.clear();
                }
            });

            while (true) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, OrderEvent> record : records) {
                    process(record, processingMs);
                    pending.put(
                            new TopicPartition(record.topic(), record.partition()),
                            // +1: коммитим позицию СЛЕДУЮЩЕГО сообщения, а не текущего.
                            new OffsetAndMetadata(record.offset() + 1));
                }

                if (!records.isEmpty()) {
                    consumer.commitAsync(pending, (offsets, ex) -> {
                        if (ex != null) {
                            log.warn("Асинхронный коммит не прошёл, повторим в следующем цикле", ex);
                        }
                    });
                }
            }
        } catch (WakeupException expectedOnShutdown) {
            log.info("Останов по сигналу");
        } finally {
            try {
                if (!pending.isEmpty()) {
                    consumer.commitSync(pending);
                }
            } finally {
                consumer.close();
            }
        }
    }

    private static void process(ConsumerRecord<String, OrderEvent> record, long processingMs) {
        // Обработчик обязан быть идемпотентным: at-least-once означает,
        // что это сообщение может прийти повторно. Модуль 15.
        log.debug("p{} @{} key={} value={}", record.partition(), record.offset(), record.key(), record.value());
        if (processingMs > 0) {
            try {
                Thread.sleep(processingMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
