package dev.learning.kafka.own.intake;

import dev.learning.kafka.common.JsonSerde;
import dev.learning.kafka.common.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;

public class OrderPublisher {

    private final KafkaProducer<String, OrderEvent> kafkaProducer;
    private final Duration waitForFinishMs;

    public OrderPublisher(Map<String, Object> producerProps, Duration waitForFinishMs) {
        this.waitForFinishMs = waitForFinishMs;
        this.kafkaProducer = new KafkaProducer<>(producerProps, new StringSerializer(), JsonSerde.serializer());
    }

    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close(waitForFinishMs);
        }
    }
}
