package dev.learning.kafka.own.intake;

import java.io.IOException;
import java.time.Duration;

public class IntakeApp {

    public static void main(String[] args) throws InterruptedException, IOException {

        Thread main = Thread.currentThread();
        PropertiesHolder pi = PropertiesHolder.create(args);
        DeliveryJournal journal = new DeliveryJournal(pi.getJournalFolder());
        journal.open();
        OrderPublisher publisher = new OrderPublisher(pi.getProducerProps(),
                                                      Duration.ofMillis(pi.getWaitForFinishMs()),
                                                      journal);
        OrderGenerator generator = new OrderGenerator(publisher::accept, pi.getEventsRate(), pi.getDurationMs());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            generator.stop();
            try {
                main.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Поток hook был прерван");
            }
        }));
        Thread threadGen = new Thread(generator);
        threadGen.start();
        threadGen.join();
        if (generator.getStatus() == OrderGenerator.Status.FAILED) {
            throw new RuntimeException("При генерации событий возникла ошибка");
        }
        publisher.close();
        journal.close();
    }
}
