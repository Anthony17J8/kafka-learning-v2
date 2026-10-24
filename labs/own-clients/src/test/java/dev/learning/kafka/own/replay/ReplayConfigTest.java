package dev.learning.kafka.own.replay;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplayConfigTest {

    @TempDir
    Path dir;

    private ReplayConfig create(String... args) {
        ReplayConfig config = new ReplayConfig();
        config.initProps(args);
        return config;
    }

    private String[] valid(String... extra) {
        String[] base = {
                "from=2026-10-16T14:00:00Z",
                "to=2026-10-16T14:30:00Z",
                "out=" + dir.resolve("out.jsonl")
        };
        String[] all = new String[base.length + extra.length];
        System.arraycopy(base, 0, all, 0, base.length);
        System.arraycopy(extra, 0, all, base.length, extra.length);
        return all;
    }

    @Test
    @DisplayName("параметры в нижнем регистре, как в описании: from, to, out")
    void lowerCaseParams() {
        ReplayConfig config = create(valid());

        assertThat(config.getFrom()).isEqualTo(Instant.parse("2026-10-16T14:00:00Z"));
        assertThat(config.getTo()).isEqualTo(Instant.parse("2026-10-16T14:30:00Z"));
        assertThat(config.getOut()).isEqualTo(dir.resolve("out.jsonl"));
    }

    @Test
    @DisplayName("порядок аргументов не важен: to раньше from")
    void orderIndependent() {
        ReplayConfig config = create("to=2026-10-16T14:30:00Z", "from=2026-10-16T14:00:00Z",
                                     "out=" + dir.resolve("out.jsonl"));

        assertThat(config.getFrom()).isBefore(config.getTo());
    }

    @Test
    @DisplayName("from позже to — ошибка")
    void fromAfterTo() {
        assertThatThrownBy(() -> create("from=2026-10-16T15:00:00Z", "to=2026-10-16T14:00:00Z",
                                        "out=" + dir.resolve("out.jsonl")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("from равен to — пустой интервал, ошибка")
    void emptyInterval() {
        assertThatThrownBy(() -> create("from=2026-10-16T14:00:00Z", "to=2026-10-16T14:00:00Z",
                                        "out=" + dir.resolve("out.jsonl")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("без from, to или out — ошибка при создании, а не NullPointerException позже")
    void requiredParams() {
        assertThatThrownBy(() -> create("to=2026-10-16T14:30:00Z", "out=" + dir.resolve("a.jsonl")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> create("from=2026-10-16T14:00:00Z", "out=" + dir.resolve("b.jsonl")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> create("from=2026-10-16T14:00:00Z", "to=2026-10-16T14:30:00Z"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("момент не в ISO-8601 — ошибка")
    void badInstant() {
        assertThatThrownBy(() -> create("from=16.10.2026 14:00", "to=2026-10-16T14:30:00Z",
                                        "out=" + dir.resolve("out.jsonl")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("group.id запрещён")
    void groupIdRejected() {
        assertThatThrownBy(() -> create(valid("group.id=g1"))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("существующий файл вывода — ошибка: прошлая выгрузка не перезаписывается")
    void existingOutFile() throws IOException {
        Path existing = Files.createFile(dir.resolve("old.jsonl"));

        assertThatThrownBy(() -> create("from=2026-10-16T14:00:00Z", "to=2026-10-16T14:30:00Z", "out=" + existing))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("каталог файла вывода не существует — ошибка")
    void outDirMissing() {
        assertThatThrownBy(() -> create("from=2026-10-16T14:00:00Z", "to=2026-10-16T14:30:00Z",
                                        "out=" + dir.resolve("nope").resolve("out.jsonl")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("значения по умолчанию: топик заказов и положительный max.wait.ms")
    void defaults() {
        ReplayConfig config = create(valid());

        assertThat(config.getTopic()).isEqualTo("orders.created.v1");
        assertThat(config.getMaxWaitMs()).isPositive();
    }

    @Test
    @DisplayName("topic и max.wait.ms из аргументов")
    void topicAndMaxWait() {
        ReplayConfig config = create(valid("topic=replay.retention.v1", "max.wait.ms=30000"));

        assertThat(config.getTopic()).isEqualTo("replay.retention.v1");
        assertThat(config.getMaxWaitMs()).isEqualTo(30_000);
    }

    @Test
    @DisplayName("настройки потребителя: bootstrap задан, без group.id, без автокоммита, десериализатор строк Kafka")
    void consumerProps() {
        Map<String, Object> props = create(valid()).getConsumerProperties();

        assertThat(props).containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertThat(props).doesNotContainKey(ConsumerConfig.GROUP_ID_CONFIG);
        assertThat(props).containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        assertThat(props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).toString())
                .contains(StringDeserializer.class.getName());
        assertThat(props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG).toString())
                .contains(StringDeserializer.class.getName());
    }

    @Test
    @DisplayName("неизвестная настройка уходит в потребитель, а не падает")
    void consumerPassThrough() {
        Map<String, Object> props = create(valid("fetch.min.bytes=1024")).getConsumerProperties();

        assertThat(props).containsEntry("fetch.min.bytes", "1024");
    }

    @Test
    @DisplayName("параметры инструмента не попадают в настройки потребителя")
    void ownParamsNotInConsumer() {
        Map<String, Object> props = create(valid("topic=t", "max.wait.ms=1000")).getConsumerProperties();

        assertThat(props).doesNotContainKeys("from", "to", "out", "topic", "max.wait.ms");
    }

    @Test
    @DisplayName("out — относительный путь без каталога: файл в текущем каталоге, без исключения разбора")
    void relativeOutPath() {
        ReplayConfig config = create("from=2026-10-16T14:00:00Z", "to=2026-10-16T14:30:00Z",
                                     "out=replay-config-test-out.jsonl");

        assertThat(config.getOut().getFileName().toString()).isEqualTo("replay-config-test-out.jsonl");
    }

    @Test
    @DisplayName("карта настроек потребителя снаружи неизменяема")
    void consumerPropsImmutable() {
        Map<String, Object> props = create(valid()).getConsumerProperties();

        assertThatThrownBy(() -> props.put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }
}