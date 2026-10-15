package dev.learning.kafka.own.processing;

import dev.learning.kafka.own.common.Constants;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetTrackerTest {

    private static TopicPartition tp(int partition) {
        return new TopicPartition(Constants.ORDERS_TOPIC, partition);
    }

    @Test
    @DisplayName("snapshot отдаёт последний оффсет каждой партиции")
    void snapshotHasLatestOffsets() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.done(0, 10);
        tracker.done(0, 11);
        tracker.done(3, 5);

        Map<TopicPartition, OffsetAndMetadata> snapshot = tracker.snapshot();

        assertThat(snapshot).hasSize(2);
        assertThat(snapshot.get(tp(0)).offset()).isEqualTo(11);
        assertThat(snapshot.get(tp(3)).offset()).isEqualTo(5);
    }

    @Test
    @DisplayName("snapshot — копия: изменения трекера после снимка его не меняют")
    void snapshotIsCopy() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.done(0, 10);

        Map<TopicPartition, OffsetAndMetadata> snapshot = tracker.snapshot();
        tracker.done(0, 20);
        tracker.done(1, 1);

        assertThat(snapshot).hasSize(1);
        assertThat(snapshot.get(tp(0)).offset()).isEqualTo(10);
    }

    @Test
    @DisplayName("пустой трекер — пустой snapshot")
    void emptySnapshot() {
        assertThat(new OffsetTracker().snapshot()).isEmpty();
    }

    @Test
    @DisplayName("forPartitions отдаёт только запрошенные партиции")
    void forPartitionsFilters() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.done(0, 10);
        tracker.done(1, 20);
        tracker.done(2, 30);

        Map<TopicPartition, OffsetAndMetadata> subset = tracker.forPartitions(List.of(0, 2));

        assertThat(subset).containsOnlyKeys(tp(0), tp(2));
        assertThat(subset.get(tp(2)).offset()).isEqualTo(30);
    }

    @Test
    @DisplayName("forPartitions не выдумывает оффсет для партиции без обработанных записей")
    void forPartitionsSkipsUnknown() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.done(0, 10);

        Map<TopicPartition, OffsetAndMetadata> subset = tracker.forPartitions(List.of(0, 5));

        assertThat(subset)
                .as("коммит оффсета 0 для партиции 5 откатил бы группу к началу партиции")
                .containsOnlyKeys(tp(0));
    }

    @Test
    @DisplayName("remove забывает партиции")
    void removeForgets() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.done(0, 10);
        tracker.done(1, 20);

        tracker.remove(List.of(0));

        assertThat(tracker.snapshot()).containsOnlyKeys(tp(1));
        assertThat(tracker.forPartitions(List.of(0))).isEmpty();
    }
}