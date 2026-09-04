package dev.learning.kafka.admin;

import dev.learning.kafka.common.Env;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * Модуль 7, задание 5: диагностика кластера через Admin API.
 *
 * Проверки:
 *   - партиции без лидера (offline) — запись и чтение недоступны;
 *   - under-replicated партиции — реплики отстают, отказоустойчивость снижена;
 *   - топики с RF=1 — одна поломка диска означает потерю данных;
 *   - партиции, у которых лидер не является preferred-репликой — нагрузка перекошена.
 *
 * Запуск:
 *   mvn -pl lab07-admin -am compile exec:java \
 *     -Dexec.mainClass=dev.learning.kafka.admin.ClusterDoctor
 *
 * Развитие: добавьте поиск неактивных consumer-групп, экспорт конфигураций
 * топиков в YAML и создание топиков из этого YAML.
 */
public class ClusterDoctor {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Env.bootstrapServers());

        try (Admin admin = Admin.create(props)) {
            var cluster = admin.describeCluster();
            System.out.println("Кластер: " + cluster.clusterId().get());
            System.out.println("Брокеры: " + cluster.nodes().get());
            System.out.println("Контроллер: " + cluster.controller().get());
            System.out.println();

            Set<String> topics = admin.listTopics(new ListTopicsOptions().listInternal(false))
                    .names().get();

            var descriptions = admin.describeTopics(topics, new DescribeTopicsOptions())
                    .allTopicNames().get();

            List<String> offline = new ArrayList<>();
            List<String> underReplicated = new ArrayList<>();
            List<String> rfOne = new ArrayList<>();
            List<String> notPreferred = new ArrayList<>();

            for (TopicDescription topic : descriptions.values()) {
                for (TopicPartitionInfo p : topic.partitions()) {
                    String id = topic.name() + "-" + p.partition();

                    if (p.leader() == null) {
                        offline.add(id);
                        continue;
                    }
                    if (p.isr().size() < p.replicas().size()) {
                        underReplicated.add(id + " (isr=" + p.isr().size()
                                + " из " + p.replicas().size() + ")");
                    }
                    if (p.replicas().size() == 1) {
                        rfOne.add(id);
                    }
                    if (!p.replicas().isEmpty()
                            && p.leader().id() != p.replicas().get(0).id()) {
                        notPreferred.add(id + " (лидер=" + p.leader().id()
                                + ", preferred=" + p.replicas().get(0).id() + ")");
                    }
                }
            }

            report("Партиции без лидера (КРИТИЧНО)", offline);
            report("Under-replicated партиции", underReplicated);
            report("Топики/партиции с RF=1", rfOne);
            report("Лидер не preferred (нужен preferred leader election)", notPreferred);

            if (offline.isEmpty() && underReplicated.isEmpty()) {
                System.out.println("Критичных проблем не найдено.");
            }
        }
    }

    private static void report(String title, List<String> items) {
        System.out.println("== " + title + ": " + items.size());
        items.stream().limit(30).forEach(i -> System.out.println("   " + i));
        if (items.size() > 30) {
            System.out.println("   ... и ещё " + (items.size() - 30));
        }
        System.out.println();
    }
}
