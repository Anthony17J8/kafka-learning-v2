package dev.learning.kafka.own.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.learning.kafka.common.JsonSerde;
import dev.learning.kafka.common.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderParser {
    private static final Logger log = LoggerFactory.getLogger(OrderParser.class);

    public ParseResult parse(String orderVal) {
        if (orderVal== null || orderVal.trim().isEmpty()) {
            return new Fail("Заказ не может быть null или пустым");
        }

        OrderEvent oe;
        try {
            oe = JsonSerde.MAPPER.readValue(orderVal, OrderEvent.class);
        } catch (JsonProcessingException exc) {
            log.error("Ошибка парсинга сообщения: {}", orderVal, exc);
            return new Fail(exc.getMessage());
        }
        if (oe.orderId() == null || oe.orderId().trim().isEmpty()) {
            return new Fail("Заказ должен иметь orderId");
        }
        if (oe.sellerId() == null || oe.sellerId().trim().isEmpty()) {
            return new Fail("Заказ должен иметь sellerId");
        }

        return new Success(oe);
    }


    public sealed interface ParseResult permits Success, Fail {
    }

    public record Success(OrderEvent orderEvent) implements ParseResult {
    }

    public record Fail(String reason) implements ParseResult {
    }
}
