package dev.learning.kafka.own.processing;

import dev.learning.kafka.own.common.Constants;
import dev.learning.kafka.own.processing.exceptions.BalanceStoreException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class ProcessingLoop {
    private static final Logger log = LoggerFactory.getLogger(ProcessingLoop.class);

    private final OffsetTracker offsetTracker;
    private final KafkaConsumer<String, String> consumer;
    private final PoisonLog poisonLog;
    private final BalanceStore balanceStore;
    private final OrderParser orderParser;
    private final long printPeriodMs;
    private final long emptyPollCounterLimit;
    private final long processTimeoutMs;

    private final LongAdder processed = new LongAdder();
    private final LongAdder duplicated = new LongAdder();
    private final LongAdder invalid = new LongAdder();

    private Long previousPrint = null;

    private volatile boolean stop = false;

    private long emptyPollCounter = 0;

    public ProcessingLoop(OffsetTracker offsetTracker,
                          Map<String, Object> consumerProperties,
                          PoisonLog poisonLog,
                          BalanceStore balanceStore,
                          long printPeriodMs,
                          long processTimeoutMs,
                          long emptyPollCounterLimit) {
        this.offsetTracker = offsetTracker;
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.poisonLog = poisonLog;
        this.balanceStore = balanceStore;
        this.orderParser = new OrderParser();
        this.printPeriodMs = printPeriodMs;
        this.processTimeoutMs = processTimeoutMs;
        this.emptyPollCounterLimit = emptyPollCounterLimit;
        this.consumer.subscribe(Collections.singleton(Constants.ORDERS_TOPIC),
                                new RebalanceHandler(consumer, offsetTracker));
    }

    public void poll() {
        try {
            while (!stop) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            if (processTimeoutMs > 0) Thread.sleep(processTimeoutMs);
                        } catch (InterruptedException exc) {
                            log.error("poll() был прерван другим потоком");
                            stop = true;
                        }
                        OrderParser.ParseResult parseResult = orderParser.parse(record.value());
                        handleResult(record.partition(), record.offset(), parseResult, record.value());
                        offsetTracker.done(record.partition(), record.offset() + 1);
                    }
                    consumer.commitAsync(offsetTracker.snapshot(), (offsets, exception) -> {
                        if (exception != null) {
                            log.error("Ошибка при коммите offset", exception);
                        }
                    });
                    emptyPollCounter = 0;
                } else {
                    emptyPollCounter++;
                }
                printInPeriod();

                if (emptyPollCounter == emptyPollCounterLimit) {
                    stop = true;
                }
            }
        } catch (WakeupException exc) {
            log.info("Цикл poll() консьюмера был прерван. Был вызван wakeup()");
        } finally {
            try {
                consumer.commitSync(offsetTracker.snapshot());
            } catch (RuntimeException exc) {
                log.error("Ошибка при sync коммите", exc);
            } finally {
                consumer.close();
            }
        }
    }

    private void printInPeriod() {
        long current = System.nanoTime();
        if (previousPrint == null) {
            previousPrint = current;
        }
        if (current - previousPrint > printPeriodMs * 1_000_000) {
            log.info("Обработано: {}, дубликатов: {}, битых: {}",
                     processed.longValue(), duplicated.longValue(), invalid.longValue());
            log.info("=".repeat(20));
            Map<MetricName, ? extends Metric> metrics = consumer.metrics();
            log.info("Метрики консьюмера: ");
            metrics.forEach((key, val) -> {
                log.info("{} : {}", key, val);
            });
            previousPrint = current;
        }
    }

    private void handleResult(int partition,
                              long offset,
                              OrderParser.ParseResult parseResult,
                              String value) {
        switch (parseResult) {
            case OrderParser.Fail f: {
                poisonLog.handle(partition, offset, f.reason(), value);
                invalid.increment();
                break;
            }
            case OrderParser.Success s: {
                try {
                    BalanceStore.BalanceOperationResult dbResult = balanceStore.apply(s.orderEvent());
                    if (BalanceStore.BalanceOperationResult.APPLIED == dbResult) {
                        processed.increment();
                    }
                    if (BalanceStore.BalanceOperationResult.DUPLICATE == dbResult) {
                        duplicated.increment();
                    }
                } catch (BalanceStoreException exc) {
                    log.error("Ошибка при запросе к BalanceStore.");
                    throw new RuntimeException(exc);
                }
            }
        }
    }

    public void wakeup() {
        consumer.wakeup();
    }
}
