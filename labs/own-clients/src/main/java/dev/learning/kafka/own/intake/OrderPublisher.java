package dev.learning.kafka.own.intake;

import dev.learning.kafka.common.JsonSerde;
import dev.learning.kafka.common.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

public class OrderPublisher {
    private static final Logger log = LoggerFactory.getLogger(OrderPublisher.class);
    private static final String ORDERS_TOPIC = "orders.created.v1";

    private final KafkaProducer<String, OrderEvent> kafkaProducer;
    private final Duration waitForFinishMs;
    private final DeliveryJournal deliveryJournal;

    public OrderPublisher(Map<String, Object> producerProps, Duration waitForFinishMs,
                          DeliveryJournal deliveryJournal) {
        this.waitForFinishMs = waitForFinishMs;
        this.kafkaProducer = new KafkaProducer<>(producerProps, new StringSerializer(), JsonSerde.serializer());
        this.deliveryJournal = deliveryJournal;
    }

    public void accept(OrderEvent event) {
        if (!deliveryJournal.isHealthy()) {
            log.error("Поток обработки журнала unhealthy. Отправка сообщений невозможна");
            throw new IllegalStateException("Журнал не работоспособен");
        }
        ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(ORDERS_TOPIC, event.sellerId(), event);
        deliveryJournal.put(new JournalEvent.AcceptedEvent(event.orderId()));
        try {
            PublishCallback callback = new PublishCallback(event.orderId(), deliveryJournal);
            kafkaProducer.send(record, callback);
            callback.setSendExecuted(true);
        } catch (RuntimeException exc) {
            deliveryJournal.put(
                    new JournalEvent.FailedEvent(event.orderId(), exc, exc.getMessage(), false));
        }
    }

    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close(waitForFinishMs);
        }
    }
}
