package dev.learning.kafka.own.intake;

import dev.learning.kafka.common.Env;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PropertiesHolder {
    public static final String WAIT_FINISH_MS = "wait.finish.ms";
    public static final String JOURNAL_FOLDER = "journal.folder";
    public static final String COUNTING_PERIOD_MS = "counting.period";
    public static final String EVENTS_RATE = "rate";
    public static final String DURATION_GEN_MS = "duration.ms";
    private static final Set<String> OWN = Set.of(WAIT_FINISH_MS,
                                                  JOURNAL_FOLDER,
                                                  EVENTS_RATE,
                                                  DURATION_GEN_MS);

    private long waitForFinishMs = 5000L;
    private long eventsRate = 500;
    private long durationMs = 60_000;
    /**
     * Должна быть создана заранее
     */
    private String journalFolder = "/tmp/delivery-journal";

    private final Map<String, Object> producerProps = new HashMap<>();

    {
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.bootstrapServers());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        producerProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CountingInterceptor.class.getName());
    }

    public static PropertiesHolder create(String[] args) {
        PropertiesHolder ph = new PropertiesHolder();
        ph.initProps(args);
        return ph;
    }

    public long getWaitForFinishMs() {
        return waitForFinishMs;
    }

    public Path getJournalFolder() {
        return Path.of(journalFolder);
    }

    public long getEventsRate() {
        return eventsRate;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public Map<String, Object> getProducerProps() {
        return Map.copyOf(producerProps);
    }

    private void initProps(String[] args) {
        if (args == null) {
            return;
        }
        for (String arg : args) {
            String[] keyVal = arg.split("=", 2);
            if (keyVal.length != 2) {
                throw new IllegalArgumentException("Incorrect format for parameters: " + arg);
            }
            if (keyVal[0].isEmpty()) {
                throw new IllegalArgumentException("Key is empty");
            }
            if (keyVal[1].isEmpty()) {
                throw new IllegalArgumentException("Value is empty: " + keyVal[0] + "=");
            }
            if (OWN.contains(keyVal[0])) {
                handleCustoms(keyVal);
            } else {
                producerProps.put(keyVal[0], keyVal[1]);
            }
        }
    }

    private void handleCustoms(String[] keyVal) {
        String key = keyVal[0];
        String val = keyVal[1];

        if (key.equals(WAIT_FINISH_MS)) {
            waitForFinishMs = validateNumberAndGet(val, key);
            validatePositiveNumber(key, waitForFinishMs);
        }

        if (key.equals(JOURNAL_FOLDER)) {
            Path folderPath = Path.of(val);
            boolean exists = Files.exists(folderPath);
            if (!exists) {
                throw new IllegalArgumentException(JOURNAL_FOLDER + " doesn't exist");
            }
            boolean isDir = Files.isDirectory(folderPath);
            if (!isDir) {
                throw new IllegalArgumentException(JOURNAL_FOLDER + " is not directory");
            }
            journalFolder = val;
        }

        if (key.equals(EVENTS_RATE)) {
            eventsRate = validateNumberAndGet(val, key);
            validatePositiveNumber(key, eventsRate);
        }
        if (key.equals(DURATION_GEN_MS)) {
            durationMs = validateNumberAndGet(val, key);
            validatePositiveNumber(key, durationMs);
        }
    }

    private Long validateNumberAndGet(String val, String key) {
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException exc) {
            throw new IllegalArgumentException("Incorrect value for key " + key + "=" + val);
        }
    }

    private void validatePositiveNumber(String key, Long val) {
        if (val <= 0) {
            throw new IllegalArgumentException("Value for key must be positive(" + key + "=" + val + ")");
        }
    }
}
