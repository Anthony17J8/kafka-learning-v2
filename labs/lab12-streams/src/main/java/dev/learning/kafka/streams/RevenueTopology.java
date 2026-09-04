package dev.learning.kafka.streams;

import dev.learning.kafka.common.JsonSerde;
import dev.learning.kafka.common.OrderEvent;
import dev.learning.kafka.common.Topics;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;

/**
 * Модуль 12, задание 3: выручка по продавцу за 5-минутные tumbling-окна.
 *
 * Топология вынесена в отдельный статический метод специально: так её можно
 * протестировать через TopologyTestDriver без брокера. Смотрите
 * {@code RevenueTopologyTest}.
 *
 * На что обратить внимание:
 *   - selectKey меняет ключ, поэтому Streams вставит repartition-топик;
 *   - grace period определяет, сколько ждём опоздавшие события;
 *   - агрегат хранится в state store с changelog-топиком (compacted).
 *
 * Задание: вызовите topology().describe() и нарисуйте граф. Найдите
 * repartition- и changelog-топики в списке топиков кластера.
 */
public final class RevenueTopology {

    public static final Duration WINDOW = Duration.ofMinutes(5);
    public static final Duration GRACE = Duration.ofMinutes(1);
    public static final String STORE_NAME = "revenue-by-seller-store";

    private RevenueTopology() {
    }

    public static Serde<OrderEvent> orderSerde() {
        return Serdes.serdeFrom(JsonSerde.serializer(), JsonSerde.deserializer(OrderEvent.class));
    }

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<OrderEvent> orderSerde = orderSerde();

        builder.stream(Topics.ORDERS, Consumed.with(stringSerde, orderSerde))
                .filter((key, order) -> order != null && order.amount() > 0)
                // Ключ был orderId, агрегируем по продавцу -> нужен repartition.
                .selectKey((key, order) -> order.sellerId())
                .groupByKey(Grouped.with(stringSerde, orderSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW, GRACE))
                .aggregate(
                        () -> 0L,
                        (sellerId, order, total) -> total + order.amount(),
                        Materialized.<String, Long, org.apache.kafka.streams.state.WindowStore<
                                org.apache.kafka.common.utils.Bytes, byte[]>>as(STORE_NAME)
                                .withKeySerde(stringSerde)
                                .withValueSerde(Serdes.Long()))
                .toStream()
                .to(Topics.REVENUE_BY_SELLER,
                        Produced.with(
                                WindowedSerdes.timeWindowedSerdeFrom(String.class, WINDOW.toMillis()),
                                Serdes.Long()));

        return builder.build();
    }
}
