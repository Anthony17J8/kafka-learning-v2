package dev.learning.kafka.own.replay;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

public record PartitionPlan(TopicPartition partition,
                            OffsetAndTimestamp startInterval,
                            Long endOffset,
                            Long beginningOffset,
                            PartitionState state,
                            Long timestamp) {

    public static PartitionPlan of(TopicPartition tp,
                                   OffsetAndTimestamp startInterval,
                                   Long endOffset,
                                   Long beginningOffset,
                                   Long from) {


        PartitionState pState = evalState(startInterval, endOffset, beginningOffset, from);
        return new PartitionPlan(tp, startInterval, endOffset, beginningOffset, pState, pState == PartitionState.EMPTY ? null : startInterval.timestamp());
    }

    private static PartitionState evalState(OffsetAndTimestamp startInterval,
                                            Long endOffset,
                                            Long beginningOffset,
                                            Long from) {
        if (startInterval == null || startInterval.offset() >= endOffset) {
            return PartitionState.EMPTY;
        }
        if (startInterval.offset() != 0
                && startInterval.offset() == beginningOffset
                && from < startInterval.timestamp()) {
            return PartitionState.READ_TRUNCATED;
        }
        return PartitionState.READ;
    }

    public enum PartitionState {
        /**
         * начало найдено и меньше границы
         */
        READ,
        /**
         * offsetsForTimes вернул null, или начало не меньше границы
         */
        EMPTY,
        /**
         * как READ, но начало интервала, возможно, удалено retention
         */
        READ_TRUNCATED
    }
}
