package dev.learning.kafka.streams;

import static org.assertj.core.api.Assertions.assertThat;

import dev.learning.kafka.common.OrderEvent;
import dev.learning.kafka.common.Topics;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Модуль 12, задание 7: тесты топологии без брокера.
 *
 * TopologyTestDriver прогоняет топологию in-memory и позволяет управлять
 * временем вручную. Это единственный вменяемый способ тестировать оконную
 * логику: реальные 5 минут в тесте никто ждать не будет.
 */
class RevenueTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderEvent> input;
    private TestOutputTopic<Windowed<String>, Long> output;

    private static final Instant BASE = Instant.parse("2026-01-01T10:00:00Z");

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-revenue");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(RevenueTopology.build(), props, BASE);

        input = driver.createInputTopic(
                Topics.ORDERS,
                Serdes.String().serializer(),
                RevenueTopology.orderSerde().serializer(),
                BASE,
                Duration.ZERO);

        output = driver.createOutputTopic(
                Topics.REVENUE_BY_SELLER,
                WindowedSerdes.timeWindowedSerdeFrom(String.class, RevenueTopology.WINDOW.toMillis())
                        .deserializer(),
                Serdes.Long().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("Суммирует выручку одного продавца внутри окна")
    void aggregatesWithinWindow() {
        input.pipeInput("o1", order("o1", "seller-A", 100), BASE);
        input.pipeInput("o2", order("o2", "seller-A", 250), BASE.plusSeconds(60));

        var records = output.readKeyValuesToList();

        assertThat(records).isNotEmpty();
        assertThat(records.get(records.size() - 1).value).isEqualTo(350L);
    }

    @Test
    @DisplayName("Разные окна не смешиваются")
    void separatesWindows() {
        input.pipeInput("o1", order("o1", "seller-A", 100), BASE);
        // Следующее событие за пределами 5-минутного окна
        input.pipeInput("o2", order("o2", "seller-A", 100), BASE.plus(Duration.ofMinutes(6)));

        var records = output.readKeyValuesToList();
        long distinctWindows = records.stream()
                .map(kv -> kv.key.window().start())
                .distinct()
                .count();

        assertThat(distinctWindows).isEqualTo(2);
    }

    @Test
    @DisplayName("Продавцы агрегируются независимо")
    void separatesSellers() {
        input.pipeInput("o1", order("o1", "seller-A", 100), BASE);
        input.pipeInput("o2", order("o2", "seller-B", 700), BASE.plusSeconds(30));

        var records = output.readKeyValuesToList();
        assertThat(records).anyMatch(kv -> kv.key.key().equals("seller-A") && kv.value == 100L);
        assertThat(records).anyMatch(kv -> kv.key.key().equals("seller-B") && kv.value == 700L);
    }

    private static OrderEvent order(String id, String seller, long amount) {
        return new OrderEvent(id, seller, amount, "standard", BASE);
    }
}
