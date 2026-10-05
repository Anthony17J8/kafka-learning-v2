package dev.learning.kafka.own.intake;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static dev.learning.kafka.own.intake.PropertiesHolder.WAIT_FINISH_MS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PropertiesHolderTest {

    @Test
    @DisplayName("без аргументов — значения по умолчанию для продюсера и сервиса")
    void defaults() {
        PropertiesHolder holder = PropertiesHolder.create(new String[0]);

        assertThat(holder.getProducerProps())
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertThat(holder.getWaitForFinishMs()).isEqualTo(5000L);
    }

    @Test
    @DisplayName("null вместо массива аргументов — как без аргументов")
    void nullArgs() {
        PropertiesHolder holder = PropertiesHolder.create(null);

        assertThat(holder.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
        assertThat(holder.getWaitForFinishMs()).isEqualTo(5000L);
    }

    @Test
    @DisplayName("настройка продюсера из аргумента перекрывает значение по умолчанию")
    void producerArgOverridesDefault() {
        PropertiesHolder holder = PropertiesHolder.create(new String[]{"acks=1"});

        assertThat(holder.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "1");
    }

    @Test
    @DisplayName("параметр сервиса попадает в параметры сервиса как число и не попадает в продюсер")
    void ownArgIsTypedAndNotPassedToProducer() {
        PropertiesHolder holder = PropertiesHolder.create(new String[]{"wait.finish.ms=3000"});

        assertThat(holder.getWaitForFinishMs()).isEqualTo(3000L);
        assertThat(holder.getProducerProps()).doesNotContainKey(WAIT_FINISH_MS);
    }

    @Test
    @DisplayName("нечисловое значение параметра сервиса — ошибка при создании, а не позже")
    void invalidOwnValueFailsFast() {
        assertThatThrownBy(() -> PropertiesHolder.create(new String[]{"wait.finish.ms=abc"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(WAIT_FINISH_MS);
    }

    @Test
    @DisplayName("значение со знаком = внутри сохраняется целиком")
    void valueWithEqualsSignIsKept() {
        PropertiesHolder holder = PropertiesHolder.create(new String[]{"sasl.jaas.config=user=admin"});

        assertThat(holder.getProducerProps()).containsEntry("sasl.jaas.config", "user=admin");
    }

    @Test
    @DisplayName("аргумент без = — ошибка с текстом аргумента")
    void argWithoutEqualsSign() {
        assertThatThrownBy(() -> PropertiesHolder.create(new String[]{"acks"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("acks");
    }

    @Test
    @DisplayName("пустой ключ — ошибка")
    void emptyKey() {
        assertThatThrownBy(() -> PropertiesHolder.create(new String[]{"=1"}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("пустое значение — ошибка с именем ключа")
    void emptyValue() {
        assertThatThrownBy(() -> PropertiesHolder.create(new String[]{"acks="}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("acks");
    }

    @Test
    @DisplayName("повтор ключа — побеждает последнее значение")
    void lastValueWins() {
        PropertiesHolder holder = PropertiesHolder.create(new String[]{"acks=1", "acks=all"});

        assertThat(holder.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
    }

    @Test
    @DisplayName("два экземпляра независимы: аргументы одного не протекают в другой")
    void instancesAreIndependent() {
        PropertiesHolder first = PropertiesHolder.create(new String[]{"acks=1", "wait.finish.ms=3000"});
        PropertiesHolder second = PropertiesHolder.create(new String[0]);

        assertThat(second.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
        assertThat(second.getWaitForFinishMs()).isEqualTo(5000L);
        assertThat(first.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "1");
    }

    @Test
    @DisplayName("карта настроек продюсера неизменяема снаружи")
    void producerPropsAreImmutable() {
        PropertiesHolder holder = PropertiesHolder.create(new String[0]);
        Map<String, Object> props = holder.getProducerProps();

        assertThatThrownBy(() -> props.put(ProducerConfig.ACKS_CONFIG, "0"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}