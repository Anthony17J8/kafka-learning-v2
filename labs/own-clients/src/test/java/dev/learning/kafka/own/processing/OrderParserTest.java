package dev.learning.kafka.own.processing;

import dev.learning.kafka.common.JsonSerde;
import dev.learning.kafka.common.OrderEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class OrderParserTest {

    private final OrderParser parser = new OrderParser();

    @Test
    @DisplayName("заказ, сериализованный продюсером UC-1, разбирается без потерь")
    void parsesWhatProducerWrites() {
        OrderEvent sent = OrderEvent.sample("o-1", "s-1", 1234, "standard");
        String json = new String(JsonSerde.<OrderEvent>serializer().serialize("orders.created.v1", sent),
                                 StandardCharsets.UTF_8);

        OrderParser.ParseResult result = parser.parse(json);

        assertThat(result).isInstanceOf(OrderParser.Success.class);
        assertThat(((OrderParser.Success) result).orderEvent()).isEqualTo(sent);
    }

    @Test
    @DisplayName("не JSON — Fail")
    void garbage() {
        assertThat(parser.parse("это не json")).isInstanceOf(OrderParser.Fail.class);
    }

    @Test
    @DisplayName("пустая строка — Fail")
    void emptyString() {
        assertThat(parser.parse("")).isInstanceOf(OrderParser.Fail.class);
    }

    @Test
    @DisplayName("null вместо значения — Fail, а не исключение")
    void nullValue() {
        assertThat(parser.parse(null)).isInstanceOf(OrderParser.Fail.class);
    }

    @Test
    @DisplayName("JSON-массив вместо объекта — Fail")
    void jsonArray() {
        assertThat(parser.parse("[1, 2, 3]")).isInstanceOf(OrderParser.Fail.class);
    }

    @Test
    @DisplayName("нет orderId — Fail")
    void missingOrderId() {
        assertThat(parser.parse("{\"sellerId\":\"s-1\",\"amount\":10,\"tenant\":\"standard\"}"))
                .isInstanceOf(OrderParser.Fail.class);
    }

    @Test
    @DisplayName("пустой orderId — Fail")
    void blankOrderId() {
        assertThat(parser.parse("{\"orderId\":\"  \",\"sellerId\":\"s-1\",\"amount\":10,\"tenant\":\"standard\"}"))
                .isInstanceOf(OrderParser.Fail.class);
    }

    @Test
    @DisplayName("нет sellerId — Fail")
    void missingSellerId() {
        assertThat(parser.parse("{\"orderId\":\"o-1\",\"amount\":10,\"tenant\":\"standard\"}"))
                .isInstanceOf(OrderParser.Fail.class);
    }

    @Test
    @DisplayName("Fail содержит причину")
    void failHasReason() {
        OrderParser.ParseResult result = parser.parse("это не json");

        assertThat(((OrderParser.Fail) result).reason()).isNotBlank();
    }
}