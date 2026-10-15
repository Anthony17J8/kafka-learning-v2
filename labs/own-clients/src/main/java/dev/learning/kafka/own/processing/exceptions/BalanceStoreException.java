package dev.learning.kafka.own.processing.exceptions;

public class BalanceStoreException extends RuntimeException {

    public BalanceStoreException() {
    }

    public BalanceStoreException(String message) {
        super(message);
    }

    public BalanceStoreException(Throwable cause) {
        super(cause);
    }

    public BalanceStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
