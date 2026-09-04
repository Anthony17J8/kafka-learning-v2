package dev.learning.kafka.producer;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

/**
 * Модуль 3, задание 3: изоляция «шумных соседей».
 *
 * Премиум-трафик направляется в первые {@code premium.partitions} партиций,
 * остальной — во все прочие. Так медленный обычный трафик не блокирует
 * критичный.
 *
 * Настройка producer:
 *   partitioner.class = dev.learning.kafka.producer.TenantPartitioner
 *   premium.partitions = 2
 *
 * Подумайте над последствиями: вы вручную ломаете равномерность распределения.
 * Что произойдёт с этими двумя партициями при всплеске премиум-трафика?
 * Ответ запишите в notes/03-producer.md.
 */
public class TenantPartitioner implements Partitioner {

    private static final String PREMIUM_PREFIX = "premium";
    private int premiumPartitions = 1;

    @Override
    public void configure(Map<String, ?> configs) {
        Object value = configs.get("premium.partitions");
        if (value != null) {
            premiumPartitions = Integer.parseInt(value.toString());
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int total = partitions.size();
        if (total <= premiumPartitions) {
            return 0;
        }

        boolean premium = key instanceof String s && s.startsWith(PREMIUM_PREFIX);
        if (premium) {
            return keyBytes == null ? 0 : Utils.toPositive(Utils.murmur2(keyBytes)) % premiumPartitions;
        }

        int regular = total - premiumPartitions;
        int offset = keyBytes == null
                ? Utils.toPositive(Utils.murmur2(String.valueOf(System.nanoTime()).getBytes())) % regular
                : Utils.toPositive(Utils.murmur2(keyBytes)) % regular;
        return premiumPartitions + offset;
    }

    @Override
    public void close() {
        // no-op
    }
}
