package dev.learning.kafka.own.fraud;

import dev.learning.kafka.common.Env;
import dev.learning.kafka.own.common.PropertiesHolder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FraudConfig extends PropertiesHolder {

    private static final String POOL_SIZE = "pool.size";
    private static final String QUEUE_CAPACITY = "queue.capacity";
    private static final String PAUSE_HIGH = "pause.high";
    private static final String PAUSE_LOW = "pause.low";
    private static final String CHECK_MIN_MS = "check.min.ms";
    private static final String CHECK_MAX_MS = "check.max.ms";
    private static final String SLOW_AFTER_MS = "slow.after.ms";
    private static final String SLOW_DURATION_MS = "slow.duration.ms";
    private static final String SLOW_DELAY_MS = "slow.delay.ms";
    private static final String COMMIT_INTERVAL_MS = "commit.interval.ms";
    private static final String DRAIN_TIMEOUT_MS = "drain.timeout.ms";
    private static final String PRINT_PERIOD = "print.period";
    private static final String JOURNAL_LOG = "journal.dir";

    private static final Set<String> OWN = Set.of(POOL_SIZE,
                                                  QUEUE_CAPACITY,
                                                  PAUSE_LOW,
                                                  PAUSE_HIGH,
                                                  CHECK_MIN_MS,
                                                  CHECK_MAX_MS,
                                                  SLOW_AFTER_MS,
                                                  SLOW_DELAY_MS,
                                                  SLOW_DURATION_MS,
                                                  COMMIT_INTERVAL_MS,
                                                  DRAIN_TIMEOUT_MS,
                                                  PRINT_PERIOD,
                                                  JOURNAL_LOG);

    private final Map<String, Object> consumerProperties = new HashMap<>();

    {
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.bootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "fraud-check");
        consumerProperties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        consumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "45000");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    private int poolSize = 96;
    private int queueCapacity = 300;
    private int pauseHigh = 200;
    private int pauseLow = 80;
    private long checkMinMs = 50;
    private long checkMaxMs = 200;
    /**
     * через сколько начинается деградация сервиса проверок
     */
    private long slowAfterMs;
    /**
     * продолжительность деградации
     */
    private long slowDurationMs;
    /**
     * какая задержка проверок при деградации
     */
    private long slowDelayMs;
    private long commitIntervalMs = 1000;
    private long drainTimeoutMs = 10000;
    private long printPeriodMs = 5000;
    private String journalLogDir = "/tmp/fraud";


    @Override
    protected Set<String> ownProperties() {
        return OWN;
    }

    @Override
    protected Map<String, Object> getProperties() {
        return consumerProperties;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public int getPauseHigh() {
        return pauseHigh;
    }

    public int getPauseLow() {
        return pauseLow;
    }

    public long getCheckMinMs() {
        return checkMinMs;
    }

    public long getCheckMaxMs() {
        return checkMaxMs;
    }

    public long getSlowAfterMs() {
        return slowAfterMs;
    }

    public long getSlowDurationMs() {
        return slowDurationMs;
    }

    public long getSlowDelayMs() {
        return slowDelayMs;
    }

    public long getCommitIntervalMs() {
        return commitIntervalMs;
    }

    public long getDrainTimeoutMs() {
        return drainTimeoutMs;
    }

    public long getPrintPeriodMs() {
        return printPeriodMs;
    }

    public String getJournalLogDir() {
        return journalLogDir;
    }

    public Map<String, Object> getConsumerProperties() {
        return Map.copyOf(consumerProperties);
    }

    @Override
    protected void handleCustoms(String[] keyVal) {
        String key = keyVal[0];
        String val = keyVal[1];
        if (POOL_SIZE.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            poolSize = validateIntOverflow(key, num);
        }
        if (QUEUE_CAPACITY.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            queueCapacity = validateIntOverflow(key, num);
        }
        if (PAUSE_LOW.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            pauseLow = validateIntOverflow(key, num);
        }
        if (PAUSE_HIGH.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            pauseHigh = validateIntOverflow(key, num);
        }
        if (CHECK_MIN_MS.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            checkMinMs = num;
        }
        if (CHECK_MAX_MS.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            checkMaxMs = num;
        }
        if (SLOW_DELAY_MS.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateNotNegative(key, num);
            slowDelayMs = num;
        }
        if (SLOW_AFTER_MS.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateNotNegative(key, num);
            slowAfterMs = num;
        }
        if (SLOW_DURATION_MS.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            slowDurationMs = num;
        }
        if (DRAIN_TIMEOUT_MS.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            drainTimeoutMs = num;
        }
        if (COMMIT_INTERVAL_MS.equals(key)) {
            Long num = validateNumberAndGet(key, val);
            validateMoreThan0(key, num);
            commitIntervalMs = num;
        }
        if (PRINT_PERIOD.equals(key)) {
            printPeriodMs = validateNumberAndGet(key, val);
            validateMoreThan0(key, printPeriodMs);
        }
        if (JOURNAL_LOG.equals(key)) {
            journalLogDir = validateFolder(val);
        }
    }

    @Override
    protected void postInit() {
        super.postInit();
        if (pauseHigh <= pauseLow) {
            throw new IllegalArgumentException("pause.high must be more than pause.low");
        }
        long maxPollRecords = Long.parseLong(consumerProperties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG).toString());
        // чередь должна вмещать pause.high + max.poll.records − pool.size, потому что занятые потоки пула держат часть задач вне очереди.
        if (pauseHigh + maxPollRecords > queueCapacity + poolSize) {
            throw new IllegalArgumentException(
                    "Invalid configuration, check pause.high, queue.capacity, max.poll.records and pool.size");
        }
        if (checkMaxMs < checkMinMs) {
            throw new IllegalArgumentException("check.max.ms must be more than check.min.ms");
        }
        if (slowAfterMs > 0 && slowDurationMs == 0) {
            throw new IllegalArgumentException("slow.duration.ms must be > 0 if slow.after.ms > 0");
        }
        if (slowDurationMs > 0 && slowDelayMs == 0) {
            throw new IllegalArgumentException("slow.delay.ms must be > 0 if slow.duration.ms > 0");
        }

        long maxPollInterval = Long.parseLong(
                consumerProperties.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG).toString());
        if (drainTimeoutMs >= maxPollInterval) {
            throw new IllegalArgumentException("max.poll.interval.ms must more than drain.timeout.ms");
        }

    }
}
