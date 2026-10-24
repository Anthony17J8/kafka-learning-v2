package dev.learning.kafka.own.replay;

import com.fasterxml.jackson.databind.JsonNode;
import dev.learning.kafka.common.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Тесты читателя на MockConsumer из kafka-clients — без брокера.
 *
 * read() возвращает true, если все назначенные партиции дочитаны до своих границ.
 * getResult() — счётчики прочитанных и выгруженных записей по партициям.
 * Файл открывает и закрывает вызывающий код, поэтому тест закрывает writer перед чтением файла.
 *
 * Таймстемп записи с оффсетом i в тестах — BASE + i * 1000, поэтому интервал задаётся через оффсеты.
 */
class BoundedReaderTest {

    private static final String TOPIC = "orders.created.v1";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);
    private static final long BASE = 1_792_000_000_000L;

    @TempDir
    Path dir;

    private static long ts(long offset) {
        return BASE + offset * 1000;
    }

    private static ConsumerRecord<String, String> rec(TopicPartition tp, long offset) {
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, ts(offset), TimestampType.CREATE_TIME,
                                    0, 0, "k", "{\"orderId\":\"" + tp.partition() + "-" + offset + "\"}", new RecordHeaders(), Optional.empty());
    }

    private static PartitionPlan readPlan(TopicPartition tp, long start, long end) {
        return new PartitionPlan(tp, new OffsetAndTimestamp(start, ts(start)), end, 0L,
                                 PartitionPlan.PartitionState.READ, ts(start));
    }

    private static PartitionPlan emptyPlan(TopicPartition tp) {
        return new PartitionPlan(tp, null, 0L, 0L, PartitionPlan.PartitionState.EMPTY, null);
    }

    private MockConsumer<String, String> consumerWith(List<ConsumerRecord<String, String>> records) {
        MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
        // записи добавляются при первом poll(): MockConsumer принимает их только для уже назначенных партиций
        consumer.schedulePollTask(() -> records.forEach(consumer::addRecord));
        return consumer;
    }

    private static List<ConsumerRecord<String, String>> range(TopicPartition tp, long fromOffset, long toOffsetExclusive) {
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        for (long o = fromOffset; o < toOffsetExclusive; o++) {
            list.add(rec(tp, o));
        }
        return list;
    }

    private List<Long> exportedOffsets(Path file) throws Exception {
        List<Long> offsets = new ArrayList<>();
        for (String line : Files.readAllLines(file)) {
            JsonNode node = JsonSerde.MAPPER.readTree(line);
            offsets.add(node.get("offset").asLong());
        }
        return offsets;
    }

    private ReplayWriter writer;

    private ReplayWriter openWriter(Path file) {
        writer = new ReplayWriter(file);
        writer.open();
        return writer;
    }

    /** Выполняет чтение и закрывает файл — как это делает ReplayApp. */
    private boolean run(BoundedReader reader) {
        try {
            return reader.read();
        } finally {
            writer.close();
        }
    }

    @Test
    @DisplayName("выгружаются записи с таймстемпом в [from, to), включая запись ровно на from")
    void exportsInterval() throws Exception {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 2, 10));
        // план: начало 2, граница 10; интервал — таймстемпы оффсетов 2..5
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 2, 10)), 5_000, ts(2), ts(6));

        assertThat(run(reader)).as("выгрузка полная").isTrue();
        assertThat(exportedOffsets(file)).containsExactly(2L, 3L, 4L, 5L);
    }

    @Test
    @DisplayName("to не включается: запись ровно на to не выгружается")
    void toIsExclusive() throws Exception {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 5));
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 5)), 5_000, ts(0), ts(3));

        run(reader);

        assertThat(exportedOffsets(file)).containsExactly(0L, 1L, 2L);
    }

    @Test
    @DisplayName("C2: записи за границей не выгружаются, и чтение завершается, хотя записи продолжают поступать")
    void stopsAtBoundary() throws Exception {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 20));
        // граница 10, интервал покрывает всё — записи 10..19 появились «после запуска»
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 10)), 5_000, ts(0), ts(1_000));

        long started = System.currentTimeMillis();
        assertThat(run(reader)).as("выгрузка полная").isTrue();
        assertThat(System.currentTimeMillis() - started).as("не ждёт до max.wait.ms").isLessThan(4_000);
        assertThat(exportedOffsets(file)).hasSize(10).allMatch(o -> o < 10);
    }

    @Test
    @DisplayName("C1: партиции EMPTY не назначаются и не ломают чтение")
    void emptyPartitionsSkipped() throws Exception {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 3));
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 3), emptyPlan(P1)), 5_000, ts(0), ts(1_000));

        assertThat(run(reader)).as("выгрузка полная").isTrue();
        assertThat(consumer.assignment()).doesNotContain(P1);
        assertThat(exportedOffsets(file)).containsExactly(0L, 1L, 2L);
    }

    @Test
    @DisplayName("две партиции читаются обе")
    void twoPartitions() throws Exception {
        Path file = dir.resolve("out.jsonl");
        List<ConsumerRecord<String, String>> all = new ArrayList<>(range(P0, 0, 4));
        all.addAll(range(P1, 0, 3));
        MockConsumer<String, String> consumer = consumerWith(all);
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 4), readPlan(P1, 0, 3)), 5_000, ts(0), ts(1_000));

        assertThat(run(reader)).as("выгрузка полная").isTrue();
        assertThat(Files.readAllLines(file)).hasSize(7);
    }

    @Test
    @DisplayName("записи раньше from в прочитанном диапазоне не выгружаются")
    void beforeFromSkipped() throws Exception {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 6));
        // читаем с 0, но интервал начинается с таймстемпа оффсета 3
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 6)), 5_000, ts(3), ts(1_000));

        run(reader);

        assertThat(exportedOffsets(file)).containsExactly(3L, 4L, 5L);
    }

    @Test
    @DisplayName("граница недостижима за max.wait.ms — read() возвращает false")
    void timeoutReturnsFalse() {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 5));
        // граница 20, а записей только до 4
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 20)), 700, ts(0), ts(1_000));

        assertThat(run(reader)).as("выгрузка неполная").isFalse();
    }

    @Test
    @DisplayName("пустой план — сразу true")
    void emptyPlanList() {
        BoundedReader reader = new BoundedReader(openWriter(dir.resolve("out.jsonl")),
                                                 new MockConsumer<>("earliest"), List.of(), 5_000, ts(0), ts(1));

        assertThat(run(reader)).isTrue();
    }

    @Test
    @DisplayName("граница: последняя выгружаемая запись — end - 1, запись с оффсетом end уже нет")
    void boundaryIsExclusive() throws Exception {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 8));
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 5)), 5_000, ts(0), ts(1_000));

        run(reader);

        assertThat(exportedOffsets(file)).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    @DisplayName("wakeup() прерывает чтение: недочитанные партиции возвращаются, исключение наружу не выходит")
    void wakeupStopsReading() {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 3));
        consumer.wakeup();
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 100)), 5_000, ts(0), ts(1_000));

        assertThat(run(reader)).as("выгрузка неполная").isFalse();
    }

    /** Значение счётчика партиции; отсутствие ключа — ноль. Значение null внутри карты — ошибка. */
    private static long count(java.util.Map<TopicPartition, Long> m, TopicPartition tp) {
        if (!m.containsKey(tp)) {
            return 0;
        }
        assertThat(m.get(tp)).as("счётчик партиции " + tp + " не должен быть null").isNotNull();
        return m.get(tp);
    }

    @Test
    @DisplayName("счётчики: прочитано всё, что пришло по партиции, выгружено — только интервал")
    void countersReadVsUploaded() {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 2, 10));
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 2, 10)), 5_000, ts(2), ts(6));

        run(reader);
        BoundedReader.ReadResult result = reader.getResult();

        assertThat(count(result.read(), P0)).isEqualTo(8);
        assertThat(count(result.uploaded(), P0)).isEqualTo(4);
    }

    @Test
    @DisplayName("счётчики: записи за границей прочитаны, но не выгружены")
    void countersBeyondBoundary() {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 20));
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 10)), 5_000, ts(0), ts(1_000));

        run(reader);
        BoundedReader.ReadResult result = reader.getResult();

        assertThat(count(result.read(), P0)).isEqualTo(20);
        assertThat(count(result.uploaded(), P0)).isEqualTo(10);
    }

    @Test
    @DisplayName("счётчики: по партиции EMPTY ничего не прочитано и не выгружено")
    void countersEmptyPartition() {
        Path file = dir.resolve("out.jsonl");
        MockConsumer<String, String> consumer = consumerWith(range(P0, 0, 3));
        BoundedReader reader = new BoundedReader(openWriter(file), consumer,
                                                 List.of(readPlan(P0, 0, 3), emptyPlan(P1)), 5_000, ts(0), ts(1_000));

        run(reader);
        BoundedReader.ReadResult result = reader.getResult();

        assertThat(count(result.read(), P1)).isZero();
        assertThat(count(result.uploaded(), P1)).isZero();
        assertThat(count(result.uploaded(), P0)).isEqualTo(3);
    }
}