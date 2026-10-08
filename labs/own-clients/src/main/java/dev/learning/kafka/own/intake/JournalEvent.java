package dev.learning.kafka.own.intake;

public sealed interface JournalEvent permits JournalEvent.AcceptedEvent,
                                             JournalEvent.AckedEvent,
                                             JournalEvent.FailedEvent {

    record AcceptedEvent(String orderId) implements JournalEvent {

    }

    record AckedEvent(String orderId, int partition, long offset) implements JournalEvent {

    }

    record FailedEvent(String orderId, Exception exc, String reason, boolean sendExecuted) implements JournalEvent {
    }
}
