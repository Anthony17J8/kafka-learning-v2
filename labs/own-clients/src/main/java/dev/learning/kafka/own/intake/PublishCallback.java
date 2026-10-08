package dev.learning.kafka.own.intake;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class PublishCallback implements Callback {
    private final String orderId;
    private final DeliveryJournal journal;
    private volatile boolean sendExecuted = false;

    public PublishCallback(String orderId, DeliveryJournal journal) {
        this.orderId = orderId;
        this.journal = journal;
    }

    public void setSendExecuted(boolean sendExecuted) {
        this.sendExecuted = sendExecuted;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            journal.put(new JournalEvent.FailedEvent(orderId, exception, exception.getMessage(), sendExecuted));
        } else {
            journal.put(new JournalEvent.AckedEvent(orderId, metadata.partition(), metadata.offset()));
        }
    }
}
