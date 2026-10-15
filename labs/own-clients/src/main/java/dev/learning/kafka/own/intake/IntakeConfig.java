package dev.learning.kafka.own.intake;

import dev.learning.kafka.common.Env;
import dev.learning.kafka.own.common.PropertiesHolder;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IntakeConfig extends PropertiesHolder {
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

    public static IntakeConfig create(String[] args) {
        IntakeConfig ph = new IntakeConfig();
        ph.initProps(args);
        return ph;
    }

    @Override
    protected Set<String> ownProperties() {
        return OWN;
    }

    @Override
    protected Map<String, Object> getProperties() {
        return this.producerProps;
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

    @Override
    public void handleCustoms(String[] keyVal) {
        String key = keyVal[0];
        String val = keyVal[1];

        if (key.equals(WAIT_FINISH_MS)) {
            waitForFinishMs = validateNumberAndGet(key, val);
            validateMoreThan0(key, waitForFinishMs);
        }

        if (key.equals(JOURNAL_FOLDER)) {
            journalFolder = validateFolder(val);
        }

        if (key.equals(EVENTS_RATE)) {
            eventsRate = validateNumberAndGet(key, val);
            validateMoreThan0(key, eventsRate);
        }
        if (key.equals(DURATION_GEN_MS)) {
            durationMs = validateNumberAndGet(key, val);
            validateMoreThan0(key, durationMs);
        }
    }

}
