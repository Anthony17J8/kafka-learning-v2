package dev.learning.kafka.own.replay;

import dev.learning.kafka.common.Env;
import dev.learning.kafka.own.common.Constants;
import dev.learning.kafka.own.common.PropertiesHolder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ReplayConfig extends PropertiesHolder {
    // чтобы исключить возможность случайного переопределения
    private static final String GROUP_ID = "group.id";
    private static final String FROM = "from";
    private static final String TO = "to";
    private static final String OUT = "out";
    private static final String TOPIC = "topic";
    private static final String MAX_WAIT_MS = "max.wait.ms";

    private static final Set<String> OWN = Set.of(FROM, TO, OUT, TOPIC, MAX_WAIT_MS, GROUP_ID);

    private final Map<String, Object> consumerProperties = new HashMap<>();

    {
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Env.bootstrapServers());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");
    }

    private Instant from;
    private Instant to;
    private String out;
    private String topic = Constants.ORDERS_TOPIC;
    private long maxWaitMs = 5000;

    public Instant getFrom() {
        return from;
    }

    public Instant getTo() {
        return to;
    }

    public Path getOut() {
        return Path.of(out);
    }

    public String getTopic() {
        return topic;
    }

    public long getMaxWaitMs() {
        return maxWaitMs;
    }

    @Override
    protected Set<String> ownProperties() {
        return OWN;
    }

    @Override
    protected Map<String, Object> getProperties() {
        return this.consumerProperties;
    }

    public Map<String, Object> getConsumerProperties() {
        return Map.copyOf(this.consumerProperties);
    }

    @Override
    protected void postInit() {
        super.postInit();
        if (from == null || to == null || out == null) {
            throw new IllegalArgumentException(
                    "from, to, out params cannot be null [" + from + ", " + to + ", " + out + "]");

        }
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException("FROM date can't be after TO");
        }
    }

    @Override
    protected void handleCustoms(String[] keyVal) {
        String key = keyVal[0];
        String val = keyVal[1];
        if (FROM.equals(key)) {
            from = validateInstant(val);
        }
        if (TO.equals(key)) {
            to = validateInstant(val);
        }
        if (OUT.equals(key)) {
            String outVal = validateNotEmpty(key, val);
            Path fPath = Path.of(outVal);
            Path absolutePath = fPath.toAbsolutePath();
            Path folder = absolutePath.getParent();
            validateFolder(folder.toString());
            out = validateNoFileExist(outVal);
        }
        if (TOPIC.equals(key)) {
            topic = validateNotEmpty(key, val);
        }
        if (MAX_WAIT_MS.equals(key)) {
            maxWaitMs = validateNumberAndGet(key, val);
            validateMoreThan0(key, maxWaitMs);
        }
        if (GROUP_ID.equals(key)) {
            throw new IllegalArgumentException("Ошибочно задан group.id. Группы не существует");
        }
    }
}
