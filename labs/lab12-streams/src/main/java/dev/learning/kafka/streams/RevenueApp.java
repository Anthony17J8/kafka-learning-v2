package dev.learning.kafka.streams;

import dev.learning.kafka.common.Env;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Запуск:
 *   mvn -pl lab12-streams -am compile exec:java \
 *     -Dexec.mainClass=dev.learning.kafka.streams.RevenueApp
 *
 * Задание 9 модуля 12: запустите два инстанса, убейте один во время нагрузки,
 * посмотрите в логах restore из changelog и замерьте время восстановления.
 * Затем поставьте NUM_STANDBY=1 и сравните.
 */
public class RevenueApp {

    private static final Logger log = LoggerFactory.getLogger(RevenueApp.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "labs.revenue-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Env.bootstrapServers());
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG,
                Integer.parseInt(Env.get("NUM_STANDBY", "num.standby.replicas", "0")));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                Env.get("PROCESSING_GUARANTEE", "processing.guarantee", StreamsConfig.AT_LEAST_ONCE));
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                Env.get("STATE_DIR", "state.dir", System.getProperty("java.io.tmpdir") + "/kafka-streams-labs"));

        Topology topology = RevenueTopology.build();
        // Печать графа топологии — задание 1 модуля 12.
        log.info("Топология:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Необработанное исключение в стриме", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
