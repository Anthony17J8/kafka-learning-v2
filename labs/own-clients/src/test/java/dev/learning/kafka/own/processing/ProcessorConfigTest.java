package dev.learning.kafka.own.processing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcessorConfigTest {

    @Test
    @DisplayName("настройки потребителя по умолчанию заданы явно")
    void consumerDefaults() {
        Map<String, Object> props = ProcessorConfig.create(new String[0]).getConsumerProps();

        assertThat(props)
                .containsEntry(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer")
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .containsKey(ConsumerConfig.GROUP_ID_CONFIG)
                .containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
                .containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)
                .containsKey(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        assertThat(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isNotEqualTo("latest");
        assertThat(props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).toString())
                .contains(StringDeserializer.class.getName());
    }

    @Test
    @DisplayName("значения по умолчанию параметров сервиса пригодны к работе")
    void serviceDefaults() {
        ProcessorConfig config = ProcessorConfig.create(new String[0]);

        assertThat(config.getDbUrl()).isNotBlank();
        assertThat(config.getPoisonLogDir()).as("каталог битых записей по умолчанию").isNotNull();
        assertThat(config.getEmptyPollLimit()).as("по умолчанию сервис не останавливается на простое").isEqualTo(Long.MAX_VALUE);
        assertThat(config.getPrintPeriodMs()).as("период вывода счётчиков по умолчанию").isPositive();
        assertThat(config.getProcessTimeoutMs()).isNotNegative();
    }

    @Test
    @DisplayName("client.id из аргументов доходит до потребителя")
    void clientIdReachesConsumer() {
        Map<String, Object> props = ProcessorConfig.create(new String[] {"client.id=p1"}).getConsumerProps();

        assertThat(props).containsEntry(ConsumerConfig.CLIENT_ID_CONFIG, "p1");
    }

    @Test
    @DisplayName("group.id и неизвестные настройки потребителя передаются как есть")
    void consumerPassThrough() {
        Map<String, Object> props = ProcessorConfig
                .create(new String[] {"group.id=balance-b", "fetch.min.bytes=1024"}).getConsumerProps();

        assertThat(props)
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "balance-b")
                .containsEntry("fetch.min.bytes", "1024");
    }

    @Test
    @DisplayName("db.url — параметр сервиса, не настройка потребителя")
    void dbUrl() {
        ProcessorConfig config = ProcessorConfig.create(new String[] {"db.url=jdbc:h2:file:/tmp/x/db"});

        assertThat(config.getDbUrl()).isEqualTo("jdbc:h2:file:/tmp/x/db");
        assertThat(config.getConsumerProps()).doesNotContainKey("db.url");
    }

    @Test
    @DisplayName("poison.log.dir — параметр сервиса: каталог, не настройка потребителя")
    void poisonLogDir(@TempDir Path dir) {
        ProcessorConfig config = ProcessorConfig.create(new String[] {"poison.log.dir=" + dir});

        assertThat(config.getPoisonLogDir()).isEqualTo(dir);
        assertThat(config.getConsumerProps()).doesNotContainKey("poison.log.dir");
    }

    @Test
    @DisplayName("несуществующий каталог битых записей — ошибка при создании")
    void missingPoisonLogDir(@TempDir Path dir) {
        assertThatThrownBy(() -> ProcessorConfig.create(new String[] {"poison.log.dir=" + dir.resolve("nope")}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("empty.poll.limit — параметр сервиса")
    void emptyPollLimit() {
        ProcessorConfig config = ProcessorConfig.create(new String[] {"empty.poll.limit=10"});

        assertThat(config.getEmptyPollLimit()).isEqualTo(10);
        assertThat(config.getConsumerProps()).doesNotContainKey("empty.poll.limit");
    }

    @Test
    @DisplayName("print.period должен быть больше нуля")
    void printPeriodPositive() {
        assertThatThrownBy(() -> ProcessorConfig.create(new String[] {"print.period=0"}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("process.timeout не может быть отрицательным")
    void processTimeoutNotNegative() {
        assertThatThrownBy(() -> ProcessorConfig.create(new String[] {"process.timeout=-1"}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(ProcessorConfig.create(new String[] {"process.timeout=0"}).getProcessTimeoutMs()).isZero();
    }

    @Test
    @DisplayName("нечисловое значение — ошибка при создании")
    void notANumber() {
        assertThatThrownBy(() -> ProcessorConfig.create(new String[] {"print.period=abc"}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("карта настроек потребителя неизменяема снаружи")
    void consumerPropsImmutable() {
        Map<String, Object> props = ProcessorConfig.create(new String[0]).getConsumerProps();

        assertThatThrownBy(() -> props.put("group.id", "x")).isInstanceOf(UnsupportedOperationException.class);
    }
}