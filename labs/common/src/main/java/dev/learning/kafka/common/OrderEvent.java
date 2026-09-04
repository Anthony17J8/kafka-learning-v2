package dev.learning.kafka.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/**
 * Учебное событие. Намеренно простое: в модуле 5 вы замените JSON на Avro
 * и увидите разницу в эволюции схемы.
 *
 * @param orderId  ключ сообщения; определяет партицию и, следовательно, порядок
 * @param sellerId ключ агрегации в Kafka Streams (модуль 12)
 * @param amount   сумма в минорных единицах, чтобы не связываться с double
 * @param tenant   пример поля для кастомного partitioner (модуль 3)
 * @param createdAt event time; используется TimestampExtractor в модуле 12
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderEvent(
        String orderId,
        String sellerId,
        long amount,
        String tenant,
        Instant createdAt
) {
    public static OrderEvent sample(String orderId, String sellerId, long amount, String tenant) {
        return new OrderEvent(orderId, sellerId, amount, tenant, Instant.now());
    }
}
