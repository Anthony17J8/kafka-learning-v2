package dev.learning.kafka.own.fraud;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class FraudApp {

    public static void main(String[] args) {

        FraudConfig config = new FraudConfig();
        config.initProps(args);

        CheckJournal journal = new CheckJournal();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.getProperties())) {
            registerHook(consumer);
        }
    }

    private static void registerHook(KafkaConsumer<String, String> kafkaConsumer) {
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumer.wakeup();
            try {
                main.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }
}
