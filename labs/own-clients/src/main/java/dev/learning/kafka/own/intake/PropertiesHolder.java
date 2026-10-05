package dev.learning.kafka.own.intake;

import dev.learning.kafka.common.Env;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PropertiesHolder {
    public static final String WAIT_FINISH_MS = "wait.finish.ms";
    private long waitForFinishMs = 5000L;
    private static final Set<String> OWN = Set.of(WAIT_FINISH_MS);
    private final Map<String, Object> producerProps = new HashMap<>();

    {
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.bootstrapServers());
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    }

    public static PropertiesHolder create(String[] args) {
        PropertiesHolder ph = new PropertiesHolder();
        ph.initProps(args);
        return ph;
    }

    public long getWaitForFinishMs() {
        return waitForFinishMs;
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
            try {
                waitForFinishMs = Long.parseLong(val);
            } catch (NumberFormatException exc) {
                throw new IllegalArgumentException("Incorrect value for key " + key + "=" + val);
            }
        }
    }
}
