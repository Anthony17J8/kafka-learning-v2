package dev.learning.kafka.own.intake;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntakeConfigTest {

    @Test
    @DisplayName("без аргументов — значения по умолчанию для продюсера и сервиса")
    void defaults() {
        IntakeConfig holder = IntakeConfig.create(new String[0]);

        assertThat(holder.getProducerProps())
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertThat(holder.getWaitForFinishMs()).isEqualTo(5000L);
    }

    @Test
    @DisplayName("null вместо массива аргументов — как без аргументов")
    void nullArgs() {
        IntakeConfig holder = IntakeConfig.create(null);

        assertThat(holder.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
        assertThat(holder.getWaitForFinishMs()).isEqualTo(5000L);
    }

    @Test
    @DisplayName("настройка продюсера из аргумента перекрывает значение по умолчанию")
    void producerArgOverridesDefault() {
        IntakeConfig holder = IntakeConfig.create(new String[] {"acks=1"});

        assertThat(holder.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "1");
    }

    @Test
    @DisplayName("параметр сервиса попадает в параметры сервиса как число и не попадает в продюсер")
    void ownArgIsTypedAndNotPassedToProducer() {
        IntakeConfig holder = IntakeConfig.create(new String[] {"wait.finish.ms=3000"});

        assertThat(holder.getWaitForFinishMs()).isEqualTo(3000L);
        assertThat(holder.getProducerProps()).doesNotContainKey("wait.finish.ms");
    }

    @Test
    @DisplayName("нечисловое значение параметра сервиса — ошибка при создании, а не позже")
    void invalidOwnValueFailsFast() {
        assertThatThrownBy(() -> IntakeConfig.create(new String[] {"wait.finish.ms=abc"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wait.finish.ms");
    }

    @Test
    @DisplayName("отрицательный или нулевой таймаут закрытия — ошибка при создании")
    void nonPositiveWaitFailsFast() {
        assertThatThrownBy(() -> IntakeConfig.create(new String[] {"wait.finish.ms=-1"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wait.finish.ms");
        assertThatThrownBy(() -> IntakeConfig.create(new String[] {"wait.finish.ms=0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wait.finish.ms");
    }

    @Test
    @DisplayName("значение со знаком = внутри сохраняется целиком")
    void valueWithEqualsSignIsKept() {
        IntakeConfig holder = IntakeConfig.create(new String[] {"sasl.jaas.config=user=admin"});

        assertThat(holder.getProducerProps()).containsEntry("sasl.jaas.config", "user=admin");
    }

    @Test
    @DisplayName("аргумент без = — ошибка с текстом аргумента")
    void argWithoutEqualsSign() {
        assertThatThrownBy(() -> IntakeConfig.create(new String[] {"acks"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("acks");
    }

    @Test
    @DisplayName("пустой ключ — ошибка")
    void emptyKey() {
        assertThatThrownBy(() -> IntakeConfig.create(new String[] {"=1"}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("пустое значение — ошибка с именем ключа")
    void emptyValue() {
        assertThatThrownBy(() -> IntakeConfig.create(new String[] {"acks="}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("acks");
    }

    @Test
    @DisplayName("повтор ключа — побеждает последнее значение")
    void lastValueWins() {
        IntakeConfig holder = IntakeConfig.create(new String[] {"acks=1", "acks=all"});

        assertThat(holder.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
    }

    @Test
    @DisplayName("два экземпляра независимы: аргументы одного не протекают в другой")
    void instancesAreIndependent() {
        IntakeConfig first = IntakeConfig.create(new String[] {"acks=1", "wait.finish.ms=3000"});
        IntakeConfig second = IntakeConfig.create(new String[0]);

        assertThat(second.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
        assertThat(second.getWaitForFinishMs()).isEqualTo(5000L);
        assertThat(first.getProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, "1");
    }

    @Test
    @DisplayName("карта настроек продюсера неизменяема снаружи")
    void producerPropsAreImmutable() {
        IntakeConfig holder = IntakeConfig.create(new String[0]);
        Map<String, Object> props = holder.getProducerProps();

        assertThatThrownBy(() -> props.put(ProducerConfig.ACKS_CONFIG, "0"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ---------- параметры генератора ----------

    @Test
    @DisplayName("значения по умолчанию для генератора: 500 заказов/с в течение минуты")
    void generatorDefaults() {
        IntakeConfig holder = IntakeConfig.create(new String[0]);

        assertThat(holder.getEventsRate()).isEqualTo(500L);
        assertThat(holder.getDurationMs()).isEqualTo(60_000L);
    }

    @Test
    @DisplayName("rate и duration.ms разбираются в числа и не попадают в продюсер")
    void generatorParams() {
        IntakeConfig holder = IntakeConfig.create(new String[] {"rate=2000", "duration.ms=300000"});

        assertThat(holder.getEventsRate()).isEqualTo(2_000L);
        assertThat(holder.getDurationMs()).isEqualTo(300_000L);
        assertThat(holder.getProducerProps()).doesNotContainKeys("rate", "duration.ms");
    }

    @Test
    @DisplayName("нечисловая, нулевая или отрицательная скорость — ошибка при создании")
    void invalidRateFailsFast() {
        for (String value : new String[] {"abc", "0", "-5"}) {
            assertThatThrownBy(() -> IntakeConfig.create(new String[] {"rate=" + value}))
                    .as("rate=" + value)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("rate");
        }
    }

    @Test
    @DisplayName("нечисловая или отрицательная длительность — ошибка при создании")
    void invalidDurationFailsFast() {
        for (String value : new String[] {"abc", "-1"}) {
            assertThatThrownBy(() -> IntakeConfig.create(new String[] {"duration.ms=" + value}))
                    .as("duration.ms=" + value)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("duration.ms");
        }
    }

    // ---------- каталог журнала ----------

    @Test
    @DisplayName("существующий каталог журнала принимается")
    void journalFolderAccepted(@TempDir Path dir) {
        IntakeConfig holder = IntakeConfig.create(new String[] {"journal.folder=" + dir});

        assertThat(holder.getJournalFolder()).isEqualTo(dir);
        assertThat(holder.getProducerProps()).doesNotContainKey("journal.folder");
    }

    @Test
    @DisplayName("несуществующий каталог журнала — ошибка при создании с именем параметра")
    void missingJournalFolderFails(@TempDir Path dir) {
        Path missing = dir.resolve("no-such-dir");

        assertThatThrownBy(() -> IntakeConfig.create(new String[] {"journal.folder=" + missing}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no-such-dir");
    }

    @Test
    @DisplayName("файл вместо каталога журнала — ошибка при создании")
    void journalFolderIsFile(@TempDir Path dir) throws IOException {
        Path file = Files.createFile(dir.resolve("file.txt"));

        assertThatThrownBy(() -> IntakeConfig.create(new String[] {"journal.folder=" + file}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file.txt");
    }

    // ---------- настройка интерсептора ----------

    @Test
    @DisplayName("интерсептор подключён по умолчанию")
    void interceptorConfigured() {
        IntakeConfig holder = IntakeConfig.create(new String[0]);

        assertThat(holder.getProducerProps().get("interceptor.classes").toString())
                .contains(CountingInterceptor.class.getName());
    }

    @Test
    @DisplayName("период интерсептора уходит в настройки продюсера — оттуда его получит configure()")
    void interceptorPeriodGoesToProducer() {
        IntakeConfig holder = IntakeConfig.create(new String[] {"counting.period=2000"});

        assertThat(holder.getProducerProps()).containsEntry("counting.period", "2000");
    }
}