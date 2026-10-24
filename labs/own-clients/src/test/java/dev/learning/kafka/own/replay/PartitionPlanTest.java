package dev.learning.kafka.own.replay;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Решение о состоянии партиции: PartitionPlan.of(tp, начало интервала, граница, начало лога, from).
 */
class PartitionPlanTest {

    private static final TopicPartition TP = new TopicPartition("orders.created.v1", 0);
    private static final long FROM = 1_792_000_000_000L;

    private static PartitionPlan.PartitionState state(OffsetAndTimestamp start, long end, long beginning) {
        return PartitionPlan.of(TP, start, end, beginning, FROM).state();
    }

    @Test
    @DisplayName("C1: offsetsForTimes вернул null — EMPTY")
    void nullStartIsEmpty() {
        assertThat(state(null, 100, 0)).isEqualTo(PartitionPlan.PartitionState.EMPTY);
    }

    @Test
    @DisplayName("начало равно границе — читать нечего, EMPTY")
    void startAtBoundaryIsEmpty() {
        assertThat(state(new OffsetAndTimestamp(100, FROM + 10), 100, 0)).isEqualTo(PartitionPlan.PartitionState.EMPTY);
    }

    @Test
    @DisplayName("начало за границей — EMPTY")
    void startBeyondBoundaryIsEmpty() {
        assertThat(state(new OffsetAndTimestamp(150, FROM + 10), 100, 0)).isEqualTo(PartitionPlan.PartitionState.EMPTY);
    }

    @Test
    @DisplayName("обычный случай: начало меньше границы, ничего не удалено — READ")
    void normalRead() {
        assertThat(state(new OffsetAndTimestamp(10, FROM + 5), 100, 0)).isEqualTo(PartitionPlan.PartitionState.READ);
    }

    @Test
    @DisplayName("часть лога удалена, но начало интервала позже начала лога — READ: до начала интервала только записи раньше from")
    void deletedButStartAfterLogStartIsRead() {
        assertThat(state(new OffsetAndTimestamp(50, FROM + 5), 100, 20)).isEqualTo(PartitionPlan.PartitionState.READ);
    }

    @Test
    @DisplayName("C3: интервал начинается с первой сохранившейся записи, лог обрезан, запись позже from — READ_TRUNCATED")
    void truncatedStart() {
        assertThat(state(new OffsetAndTimestamp(20, FROM + 60_000), 100, 20))
                .isEqualTo(PartitionPlan.PartitionState.READ_TRUNCATED);
    }

    @Test
    @DisplayName("лог не обрезан (начало 0) — READ, даже если первая запись позже from")
    void notTruncatedWhenLogStartIsZero() {
        assertThat(state(new OffsetAndTimestamp(0, FROM + 60_000), 100, 0)).isEqualTo(PartitionPlan.PartitionState.READ);
    }

    @Test
    @DisplayName("первая сохранившаяся запись ровно на from — начало не потеряно, READ")
    void firstRecordExactlyAtFromIsRead() {
        assertThat(state(new OffsetAndTimestamp(20, FROM), 100, 20)).isEqualTo(PartitionPlan.PartitionState.READ);
    }

    @Test
    @DisplayName("план хранит переданные координаты")
    void planKeepsCoordinates() {
        PartitionPlan plan = PartitionPlan.of(TP, new OffsetAndTimestamp(10, FROM + 5), 100L, 3L, FROM);

        assertThat(plan.partition()).isEqualTo(TP);
        assertThat(plan.startInterval().offset()).isEqualTo(10);
        assertThat(plan.endOffset()).isEqualTo(100);
        assertThat(plan.beginningOffset()).isEqualTo(3);
    }
}