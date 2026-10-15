package dev.learning.kafka.own.processing;

import dev.learning.kafka.common.Env;
import dev.learning.kafka.own.common.PropertiesHolder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ProcessorConfig extends PropertiesHolder {
    private static final String DB_URL = "db.url";
    private static final String TIMEOUT = "process.timeout";
    private static final String PRINT_PERIOD = "print.period";
    private static final String POISON_LOG_DIR = "poison.log.dir";
    private static final String EMPTY_POLL_LIMIT = "empty.poll.limit";
    private static final Set<String> OWN = Set.of(DB_URL, TIMEOUT, PRINT_PERIOD, POISON_LOG_DIR, EMPTY_POLL_LIMIT);


    private final Map<String, Object> consumerProps = new HashMap<>();

    private String dbUrl = "jdbc:h2:file:/tmp/balances/db;AUTO_SERVER=TRUE;LOCK_TIMEOUT=10000";
    private long processTimeoutMs = 0;
    private long printPeriodMs = 5000;
    private long emptyPollLimit = Long.MAX_VALUE;
    /**
     * Должна быть создана заранее
     */
    private String poisonLogDir = "/tmp/poison";

    {
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-balance");
        consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "own-client");
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "45000");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public long getProcessTimeoutMs() {
        return processTimeoutMs;
    }

    public long getPrintPeriodMs() {
        return printPeriodMs;
    }

    public long getEmptyPollLimit() {
        return emptyPollLimit;
    }

    public Path getPoisonLogDir() {
        return Path.of(poisonLogDir);
    }

    @Override
    protected Set<String> ownProperties() {
        return OWN;
    }

    @Override
    protected Map<String, Object> getProperties() {
        return this.consumerProps;
    }

    public Map<String, Object> getConsumerProps() {
        return Map.copyOf(this.consumerProps);
    }

    public static ProcessorConfig create(String[] args) {
        ProcessorConfig config = new ProcessorConfig();
        config.initProps(args);
        return config;
    }

    @Override
    public void handleCustoms(String[] keyVal) {
        String key = keyVal[0];
        String val = keyVal[1];

        if (DB_URL.equals(key)) {
            dbUrl = validateNotEmpty(key, val);
        }
        if (TIMEOUT.equals(key)) {
            processTimeoutMs = validateNumberAndGet(key, val);
            validateNotNegative(key, processTimeoutMs);
        }
        if (PRINT_PERIOD.equals(key)) {
            printPeriodMs = validateNumberAndGet(key, val);
            validateMoreThan0(key, printPeriodMs);
        }
        if (POISON_LOG_DIR.equals(key)) {
            poisonLogDir = validateFolder(val);
        }
        if (EMPTY_POLL_LIMIT.equals(key)) {
            emptyPollLimit = validateNumberAndGet(key, val);
            validateMoreThan0(key, emptyPollLimit);
        }
    }
}
