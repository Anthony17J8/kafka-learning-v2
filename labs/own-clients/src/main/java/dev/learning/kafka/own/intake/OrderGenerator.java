package dev.learning.kafka.own.intake;

import dev.learning.kafka.common.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class OrderGenerator implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OrderGenerator.class);
    private static final Random RAND = new Random(System.currentTimeMillis());
    private long idx = 0;
    private volatile boolean stop = false;
    private Status status;

    /**
     * Orders in seconds, o/s
     */
    private final long rate;
    private final long duration;
    private final Consumer<OrderEvent> eventConsumer;

    public OrderGenerator(Consumer<OrderEvent> eventConsumer, long rate, long durationMs) {
        this.eventConsumer = eventConsumer;
        this.rate = rate;
        this.duration = durationMs;
    }

    @Override
    public void run() {
        status = Status.ACTIVE;
        long start = System.nanoTime();
        long end = start + (duration * 1_000_000);
        long ti = start + (idx * 1_000_000_000L / rate);
        while (ti < end) {
            if (stop || Thread.currentThread().isInterrupted()) {
                break;
            }
            long tleft = ti - System.nanoTime();

            while (tleft > 0) {
                LockSupport.parkNanos(tleft);
                if (stop) {
                    break;
                }
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                tleft = ti - System.nanoTime();
            }
            try {
                if (stop || Thread.currentThread().isInterrupted()) {
                    break;
                }
                eventConsumer.accept(createOrder());
            } catch (RuntimeException exc) {
                status = Status.FAILED;
                throw new IllegalStateException("Ошибка при публикации события: ", exc);
            }
            idx++;
            ti = start + (idx * 1_000_000_000L / rate);
        }
        status = Status.STOPPED;
        log.info("Всего было создано {} событий со скоростью {} в течение {}ms", idx, rate, duration);
    }

    public void stop() {
        stop = true;
    }

    private OrderEvent createOrder() {
        int r = RAND.nextInt(100);
        String seller = (r < 5) ? "seller-1000" : "seller-" + RAND.nextInt(990);
        return OrderEvent.sample(
                UUID.randomUUID().toString(),
                seller,
                100 + idx,
                "standard");
    }

    public Status getStatus() {
        return status;
    }

    public enum Status {
        ACTIVE, FAILED, STOPPED
    }
}
