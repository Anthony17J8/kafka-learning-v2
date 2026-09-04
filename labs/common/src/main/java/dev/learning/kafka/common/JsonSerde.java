package dev.learning.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Простейший JSON serde без реестра схем.
 *
 * Модуль 5 начинается с того, что вы ломаете именно его: добавляете поле,
 * удаляете поле, меняете тип — и смотрите, что происходит у потребителей,
 * которые ещё не обновлены. После этого Schema Registry перестаёт
 * выглядеть избыточной сущностью.
 */
public final class JsonSerde {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private JsonSerde() {
    }

    public static <T> Serializer<T> serializer() {
        return new Serializer<>() {
            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) {
                    return null; // tombstone
                }
                try {
                    return MAPPER.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new SerializationException("Не удалось сериализовать в топик " + topic, e);
                }
            }
        };
    }

    public static <T> Deserializer<T> deserializer(Class<T> type) {
        return new Deserializer<>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // no-op
            }

            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                try {
                    return MAPPER.readValue(data, type);
                } catch (Exception e) {
                    // Эта ошибка — та самая poison pill из модуля 10.
                    // Обработайте её осознанно, а не глушите try/catch в poll loop.
                    throw new SerializationException("Не удалось разобрать сообщение из " + topic, e);
                }
            }
        };
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }
}
