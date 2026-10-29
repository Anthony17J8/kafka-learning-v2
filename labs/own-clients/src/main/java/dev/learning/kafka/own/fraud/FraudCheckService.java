package dev.learning.kafka.own.fraud;

import dev.learning.kafka.common.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class FraudCheckService {

    private static final Logger log = LoggerFactory.getLogger(FraudCheckService.class);
    private static final Function<Long, Verdict> LESS_THAN_RULE = amount -> amount < 99_999 ? Verdict.SUCCESS :
                                                                              Verdict.DECLINED;

    private final long startMs;
    private final long checkMinMs;
    private final long checkMaxMs;
    private final Delay delay;

    public FraudCheckService(long checkMinMs,
                             long checkMaxMs,
                             Delay delay) {
        this.startMs = System.currentTimeMillis();
        this.checkMinMs = checkMinMs;
        this.checkMaxMs = checkMaxMs;
        this.delay = delay;
    }

    public Verdict check(OrderEvent event) {
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        long checkMs = tlr.nextLong(checkMinMs, checkMaxMs + 1);
        long delayVal = getDelay(delay);
        long totalTime = delayVal == 0 ? checkMs : delayVal;
        try {
            Thread.sleep(totalTime);
            return verdict(event.amount());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("Thread has been interrupted during fraud check");
            throw new IllegalStateException("Fraud check has been interrupted");
        }
    }

    private long getDelay(Delay delay) {
        long startDelay = startMs + delay.afterMs;
        long current = System.currentTimeMillis();
        if (startDelay <= current && current < startDelay + delay.durationMs()) {
            return delay.delayMs();
        }
        return 0;
    }

    private Verdict verdict(long amount) {
        return LESS_THAN_RULE.apply(amount);
    }

    public enum Verdict {
        SUCCESS, DECLINED
    }

    public record Delay(long afterMs, long durationMs, long delayMs) {

    }
}
