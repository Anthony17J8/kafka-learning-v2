package dev.learning.kafka.consumer;

import dev.learning.kafka.common.Env;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Модуль 4, задание 7: свой lag-мониторинг.
 *
 * Lag = latest offset партиции − закоммиченный offset группы.
 * Именно это число, а не CPU брокера, обычно означает «система не справляется».
 *
 * Запуск:
 *   mvn -pl lab04-consumer -am compile exec:java \
 *     -Dexec.mainClass=dev.learning.kafka.consumer.LagReporter \
 *     -Dexec.args="labs.orders.processor"
 *
 * Дальше: превратите это в экспортер для Prometheus (модуль 10).
 */
public class LagReporter {

    public static void main(String[] args) throws Exception {
        String groupId = args.length > 0 ? args[0] : "labs.orders.processor";

        Properties props = new Properties();
        props.put("bootstrap.servers", Env.bootstrapServers());

        try (Admin admin = Admin.create(props)) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();

            if (committed.isEmpty()) {
                System.out.println("У группы " + groupId + " нет закоммиченных оффсетов");
                return;
            }

            Map<TopicPartition, OffsetSpec> latestSpec = new HashMap<>();
            committed.keySet().forEach(tp -> latestSpec.put(tp, OffsetSpec.latest()));

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(latestSpec).all().get();

            long total = 0;
            System.out.printf("%-40s %5s %12s %12s %10s%n",
                    "topic", "part", "committed", "end", "lag");

            for (var entry : committed.entrySet()) {
                TopicPartition tp = entry.getKey();
                long committedOffset = entry.getValue().offset();
                long endOffset = endOffsets.get(tp).offset();
                long lag = endOffset - committedOffset;
                total += lag;
                System.out.printf("%-40s %5d %12d %12d %10d%n",
                        tp.topic(), tp.partition(), committedOffset, endOffset, lag);
            }
            System.out.println("Суммарный lag группы " + groupId + ": " + total);
        }
    }
}
