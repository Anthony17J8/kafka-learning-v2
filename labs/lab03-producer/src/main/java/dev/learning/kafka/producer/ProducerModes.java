package dev.learning.kafka.producer;

import dev.learning.kafka.common.Env;
import dev.learning.kafka.common.JsonSerde;
import dev.learning.kafka.common.OrderEvent;
import dev.learning.kafka.common.Topics;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Модуль 3, задание 1: три режима отправки и их стоимость.
 *
 * Запуск:
 *   mvn -pl lab03-producer -am compile exec:java \
 *     -Dexec.mainClass=dev.learning.kafka.producer.ProducerModes \
 *     -Dexec.args="sync 100000"
 *
 * Режимы: sync | async | fire-and-forget
 *
 * Ожидаемый результат: sync медленнее на порядок и более, потому что каждый
 * вызов ждёт подтверждения и убивает батчинг. Зафиксируйте цифры в benchmarks/.
 */
public class ProducerModes {

    private static final Logger log = LoggerFactory.getLogger(ProducerModes.class);

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "async";
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 100_000;

        // Сериализаторы передаются объектами в конструктор KafkaProducer ниже,
        // поэтому в props их указывать не нужно.
        Properties props = Env.base();
        props.put(ProducerConfig.ACKS_CONFIG, Env.get("ACKS", "acks", "all"));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(Env.get("LINGER_MS", "linger.ms", "5")));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(Env.get("BATCH_SIZE", "batch.size", "16384")));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, Env.get("COMPRESSION", "compression.type", "none"));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        AtomicLong errors = new AtomicLong();
        long start = System.nanoTime();

        try (Producer<String, OrderEvent> producer = new KafkaProducer<>(
                props, new StringSerializer(), JsonSerde.<OrderEvent>serializer())) {

            for (int i = 0; i < count; i++) {
                OrderEvent event = OrderEvent.sample(
                        "order-" + i,
                        "seller-" + (i % 50),
                        100 + (i % 9_900),
                        (i % 10 == 0) ? "premium" : "standard");

                ProducerRecord<String, OrderEvent> record =
                        new ProducerRecord<>(Topics.ORDERS, event.orderId(), event);

                switch (mode) {
                    case "sync" -> producer.send(record).get();
                    case "async" -> producer.send(record, (meta, ex) -> {
                        if (ex != null) {
                            errors.incrementAndGet();
                            log.error("Ошибка отправки", ex);
                        }
                    });
                    case "fire-and-forget" -> producer.send(record);
                    default -> throw new IllegalArgumentException("Неизвестный режим: " + mode);
                }
            }
            producer.flush();
        }

        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        log.info("режим={} сообщений={} время={} мс throughput={} msg/s ошибок={}",
                mode, count, elapsed.toMillis(),
                elapsed.toMillis() == 0 ? "n/a" : count * 1000L / elapsed.toMillis(),
                errors.get());
    }
}
