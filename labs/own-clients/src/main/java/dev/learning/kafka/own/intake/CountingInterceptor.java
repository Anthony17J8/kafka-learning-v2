package dev.learning.kafka.own.intake;

import dev.learning.kafka.common.OrderEvent;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class CountingInterceptor implements ProducerInterceptor<String, OrderEvent> {
    private static final Logger log = LoggerFactory.getLogger(CountingInterceptor.class);
    private LongAdder accepted = new LongAdder();
    private LongAdder acked = new LongAdder();
    private LongAdder failed = new LongAdder();
    private long periodMs = 1000;
    private Thread timer;
    private volatile boolean stop;

    public CountingInterceptor() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        if (props.containsKey(IntakeConfig.COUNTING_PERIOD_MS)) {
            String sVal = props.get(IntakeConfig.COUNTING_PERIOD_MS).toString();
            try {
                this.periodMs = Long.parseLong(sVal);
                if (periodMs <= 0) {
                    throw new IllegalArgumentException("Значение периода счетчика должно быть больше 0");
                }
            } catch (NumberFormatException exc) {
                throw new IllegalArgumentException(
                        "Incorrect value for key: " + IntakeConfig.COUNTING_PERIOD_MS + "=" + sVal);
            }
        }

        if (timer == null) {
            timer = new Thread(this::printInfo);
            timer.start();
        }
    }

    public void printInfo() {
        while (!stop) {
            log();
            try {
                Thread.sleep(periodMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Поток печати счетчика был прерван", e);
            }
        }
    }

    @Override
    public ProducerRecord<String, OrderEvent> onSend(ProducerRecord<String, OrderEvent> record) {
        accepted.increment();
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            failed.increment();
        } else {
            acked.increment();
        }
    }

    @Override
    public void close() {
        stop = true;
        log();
    }

    private void log() {
        log.info("отправлено={}, подтверждено={}, ошибок={}",
                 accepted.longValue(),
                 acked.longValue(),
                 failed.longValue());
    }
}
