package dev.learning.kafka.own.intake;

import java.time.Duration;

public class IntakeApp {

    public static void main(String[] args) {
        PropertiesHolder pi = PropertiesHolder.create(args);
        OrderPublisher publisher = new OrderPublisher(pi.getProducerProps(),
                                                      Duration.ofMillis(pi.getWaitForFinishMs()));
        publisher.close();
    }
}
