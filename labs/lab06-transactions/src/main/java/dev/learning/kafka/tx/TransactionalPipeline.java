package dev.learning.kafka.tx;

import dev.learning.kafka.common.Env;
import dev.learning.kafka.common.JsonSerde;
import dev.learning.kafka.common.OrderEvent;
import dev.learning.kafka.common.Topics;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Модуль 6, задание 4: exactly-once в паттерне read-process-write.
 *
 * Три обязательных элемента, без любого из которых EOS не работает:
 *   1. transactional.id у producer (уникальный и СТАБИЛЬНЫЙ между рестартами);
 *   2. sendOffsetsToTransaction — оффсеты коммитятся внутри той же транзакции;
 *   3. isolation.level=read_committed у потребителя результата.
 *
 * Запуск:
 *   mvn -pl lab06-transactions -am compile exec:java \
 *     -Dexec.mainClass=dev.learning.kafka.tx.TransactionalPipeline \
 *     -Dexec.args="tx-worker-1"
 *
 * Эксперимент с zombie fencing: запустите два инстанса с ОДИНАКОВЫМ
 * transactional.id и посмотрите, как первый получит ProducerFencedException.
 *
 * Эксперимент с атомарностью: раскомментируйте CRASH_AFTER_SEND и убедитесь,
 * что потребитель с read_committed не видит частичный результат.
 */
public class TransactionalPipeline {

    private static final Logger log = LoggerFactory.getLogger(TransactionalPipeline.class);

    public static void main(String[] args) {
        String transactionalId = args.length > 0 ? args[0] : "tx-worker-1";
        String groupId = "labs.tx.pipeline";
        boolean crashAfterSend = Boolean.parseBoolean(Env.get("CRASH_AFTER_SEND", "crash.after.send", "false"));

        Properties consumerProps = Env.base();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Properties producerProps = Env.base();
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(
                     consumerProps, new StringDeserializer(), JsonSerde.deserializer(OrderEvent.class));
             Producer<String, OrderEvent> producer = new KafkaProducer<>(
                     producerProps, new StringSerializer(), JsonSerde.<OrderEvent>serializer())) {

            producer.initTransactions();
            consumer.subscribe(List.of(Topics.ORDERS_INPUT));

            while (true) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    continue;
                }

                producer.beginTransaction();
                try {
                    for (ConsumerRecord<String, OrderEvent> record : records) {
                        OrderEvent enriched = transform(record.value());
                        producer.send(new ProducerRecord<>(Topics.ORDERS_OUTPUT, record.key(), enriched));
                    }

                    if (crashAfterSend) {
                        // Симуляция падения между send и commit.
                        // Потребитель с read_committed не должен увидеть НИЧЕГО из этой партии.
                        throw new IllegalStateException("Симуляция падения до commitTransaction");
                    }

                    producer.sendOffsetsToTransaction(offsetsOf(records), consumer.groupMetadata());
                    producer.commitTransaction();
                    log.info("Транзакция закоммичена, записей: {}", records.count());

                } catch (ProducerFencedException fenced) {
                    // Другой инстанс с тем же transactional.id перехватил роль.
                    // Восстановление невозможно, корректный выход — умереть.
                    log.error("Producer fenced, инстанс устарел", fenced);
                    return;
                } catch (Exception e) {
                    log.error("Ошибка внутри транзакции, откатываемся", e);
                    producer.abortTransaction();
                    if (crashAfterSend) {
                        return;
                    }
                }
            }
        }
    }

    private static OrderEvent transform(OrderEvent in) {
        // Учебная трансформация: комиссия 3%.
        return new OrderEvent(in.orderId(), in.sellerId(),
                Math.round(in.amount() * 0.97), in.tenant(), in.createdAt());
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsetsOf(ConsumerRecords<String, OrderEvent> records) {
        return records.partitions().stream().collect(java.util.stream.Collectors.toMap(
                tp -> tp,
                tp -> {
                    var forPartition = records.records(tp);
                    long lastOffset = forPartition.get(forPartition.size() - 1).offset();
                    return new OffsetAndMetadata(lastOffset + 1);
                }));
    }
}
