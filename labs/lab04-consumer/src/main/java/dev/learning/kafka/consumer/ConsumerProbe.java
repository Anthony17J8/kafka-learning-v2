package dev.learning.kafka.consumer;

import dev.learning.kafka.common.Env;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Модуль 4: потребитель-зонд для экспериментов с коммитом, группами и ребалансировкой.
 *
 * Все аргументы — пары key=value. Собственные опции зонда перечислены ниже,
 * всё остальное передаётся в конфигурацию потребителя как есть:
 *
 *   mvn -q -pl lab04-consumer exec:java \
 *     -Dexec.mainClass=dev.learning.kafka.consumer.ConsumerProbe \
 *     -Dexec.args="id=c1 topic=m4.probe.v1 group.id=m4.probe group.protocol=consumer auto.offset.reset=earliest"
 *
 * Опции зонда:
 *   topic           топик, по умолчанию m4.probe.v1
 *   id              метка экземпляра в выводе, по умолчанию c
 *   commit          auto   — автокоммит внутри poll();
 *                   sync   — commitSync после обработки пачки;
 *                   async  — commitAsync после обработки пачки;
 *                   before — commitSync сразу после poll(), до обработки (at-most-once);
 *                   none   — не коммитить вообще.
 *                   По умолчанию sync.
 *   commit.every.ms для sync и async: коммитить не чаще, чем раз в столько мс; 0 — после каждой пачки
 *   revoke.commit   true/false — коммитить обработанное в onPartitionsRevoked (для sync, async, before)
 *   process.ms      время обработки одной записи
 *   crash.after     после стольких обработанных записей процесс завершается мгновенно,
 *                   без коммита и без close() — как kill -9
 *   processed.file  файл, куда дописываются номера обработанных записей (часть значения до '|')
 *   pause.at        после стольких обработанных записей поставить все партиции на паузу
 *   pause.ms        длительность паузы; poll() при этом продолжает вызываться
 *   idle.stop.ms    остановиться, если столько мс не приходило новых записей; 0 — не останавливаться
 *   report          период строки состояния в секундах, по умолчанию 1
 *
 * Номера записей совпадают с теми, что пишет ProducerProbe из модуля 3,
 * поэтому дубли и пропуски проверяются обычными sort, uniq, comm.
 */
public final class ConsumerProbe {

    private static final Set<String> OWN_OPTIONS = Set.of(
            "topic", "id", "commit", "commit.every.ms", "revoke.commit", "process.ms",
            "crash.after", "processed.file", "pause.at", "pause.ms", "idle.stop.ms", "report");

    private static final Set<String> COMMIT_MODES = Set.of("auto", "sync", "async", "before", "none");

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static volatile boolean stop = false;

    private ConsumerProbe() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opt = new HashMap<>();
        Properties props = Env.base();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "m4.probe");
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("Аргумент должен быть вида key=value: " + arg);
            }
            String k = arg.substring(0, eq);
            String v = arg.substring(eq + 1);
            if (OWN_OPTIONS.contains(k)) {
                opt.put(k, v);
            } else {
                props.put(k, v);
            }
        }

        String topic = opt.getOrDefault("topic", "m4.probe.v1");
        String id = opt.getOrDefault("id", "c");
        String commit = opt.getOrDefault("commit", "sync");
        if (!COMMIT_MODES.contains(commit)) {
            throw new IllegalArgumentException("commit должен быть одним из " + COMMIT_MODES);
        }
        long commitEveryMs = Long.parseLong(opt.getOrDefault("commit.every.ms", "0"));
        boolean revokeCommit = Boolean.parseBoolean(opt.getOrDefault("revoke.commit", "true"));
        long processMs = Long.parseLong(opt.getOrDefault("process.ms", "0"));
        long crashAfter = Long.parseLong(opt.getOrDefault("crash.after", "0"));
        String processedFile = opt.get("processed.file");
        long pauseAt = Long.parseLong(opt.getOrDefault("pause.at", "0"));
        long pauseMs = Long.parseLong(opt.getOrDefault("pause.ms", "0"));
        long idleStopMs = Long.parseLong(opt.getOrDefault("idle.stop.ms", "0"));
        long reportMs = Long.parseLong(opt.getOrDefault("report", "1")) * 1000;

        boolean manual = commit.equals("sync") || commit.equals("async") || commit.equals("before");
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(commit.equals("auto")));

        event(id, String.format("старт: топик=%s группа=%s протокол=%s коммит=%s",
                topic, props.get(ConsumerConfig.GROUP_ID_CONFIG),
                props.getOrDefault("group.protocol", "classic (по умолчанию)"), commit));

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());

        // Оффсеты следующих записей по тому, что уже обработано, — для назначенных партиций.
        Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
        Map<String, Integer> failures = new TreeMap<>();
        int[] events = new int[3]; // отзыв, назначение, потеря

        BufferedWriter out = processedFile == null ? null : Files.newBufferedWriter(
                Path.of(processedFile), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop = true;
            consumer.wakeup();
            try {
                mainThread.join(30_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));

        consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                events[0]++;
                event(id, "ОТЗЫВ " + nums(partitions));
                if (manual && revokeCommit) {
                    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
                    for (TopicPartition tp : partitions) {
                        OffsetAndMetadata o = done.get(tp);
                        if (o != null) {
                            toCommit.put(tp, o);
                        }
                    }
                    if (!toCommit.isEmpty()) {
                        try {
                            consumer.commitSync(toCommit);
                            event(id, "коммит при отзыве " + offsets(toCommit));
                        } catch (KafkaException e) {
                            fail(failures, id, "коммит при отзыве", e);
                        }
                    }
                }
                partitions.forEach(done::remove);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                events[1]++;
                event(id, "НАЗНАЧЕНИЕ " + nums(partitions) + ", теперь " + nums(consumer.assignment()));
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                events[2]++;
                event(id, "ПОТЕРЯ " + nums(partitions));
                partitions.forEach(done::remove);
            }
        });

        long start = System.currentTimeMillis();
        long lastReport = start;
        long lastPollReturn = start;
        long lastRecordAt = 0;
        long lastCommitAt = start;
        long resumeAt = 0;
        boolean pausedOnce = false;
        long processed = 0;
        long windowProcessed = 0;
        long windowMaxE2e = 0;
        long windowMaxPollGap = 0;

        try {
            while (!stop) {
                windowMaxPollGap = Math.max(windowMaxPollGap, System.currentTimeMillis() - lastPollReturn);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                lastPollReturn = System.currentTimeMillis();

                if (commit.equals("before") && !records.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
                    for (TopicPartition tp : records.partitions()) {
                        positions.put(tp, new OffsetAndMetadata(consumer.position(tp)));
                    }
                    try {
                        consumer.commitSync(positions);
                    } catch (WakeupException e) {
                        throw e;
                    } catch (KafkaException e) {
                        fail(failures, id, "коммит до обработки", e);
                    }
                }

                for (ConsumerRecord<String, String> r : records) {
                    if (processMs > 0) {
                        sleep(processMs);
                    }
                    processed++;
                    windowProcessed++;
                    lastRecordAt = System.currentTimeMillis();
                    windowMaxE2e = Math.max(windowMaxE2e, lastRecordAt - r.timestamp());
                    done.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                    if (out != null && r.value() != null) {
                        out.write(seqOf(r));
                        out.newLine();
                    }
                    if (crashAfter > 0 && processed == crashAfter) {
                        if (out != null) {
                            out.flush();
                        }
                        event(id, "СБОЙ после " + processed + " записей: выход без коммита и без close()");
                        Runtime.getRuntime().halt(1);
                    }
                }

                long now = System.currentTimeMillis();
                if (!records.isEmpty() && !done.isEmpty() && now - lastCommitAt >= commitEveryMs) {
                    if (commit.equals("sync")) {
                        try {
                            consumer.commitSync(done);
                        } catch (WakeupException e) {
                            throw e;
                        } catch (KafkaException e) {
                            fail(failures, id, "коммит", e);
                        }
                        lastCommitAt = now;
                    } else if (commit.equals("async")) {
                        consumer.commitAsync(new HashMap<>(done), (offs, ex) -> {
                            if (ex != null) {
                                fail(failures, id, "асинхронный коммит", ex);
                            }
                        });
                        lastCommitAt = now;
                    }
                }

                if (pauseAt > 0 && !pausedOnce && processed >= pauseAt) {
                    pausedOnce = true;
                    consumer.pause(consumer.assignment());
                    resumeAt = now + pauseMs;
                    event(id, "ПАУЗА " + nums(consumer.paused()) + " на " + pauseMs + " мс, poll() продолжается");
                }
                if (resumeAt > 0 && now >= resumeAt) {
                    event(id, "ВОЗОБНОВЛЕНИЕ " + nums(consumer.paused()));
                    consumer.resume(consumer.paused());
                    resumeAt = 0;
                }

                if (now - lastReport >= reportMs) {
                    System.out.printf("%s %s  обработано=%d/с  всего=%d  e2e-макс=%d мс  между-poll-макс=%d мс"
                                    + "  lag-макс=%.0f  партиции=%s%n",
                            LocalTime.now().format(TIME), id,
                            windowProcessed * 1000 / Math.max(1, now - lastReport), processed,
                            windowMaxE2e, windowMaxPollGap,
                            metric(consumer, "records-lag-max"), nums(consumer.assignment()));
                    lastReport = now;
                    windowProcessed = 0;
                    windowMaxE2e = 0;
                    windowMaxPollGap = 0;
                }

                long idleSince = lastRecordAt > 0 ? lastRecordAt : start;
                if (idleStopMs > 0 && now - idleSince >= idleStopMs) {
                    event(id, "новых записей нет " + idleStopMs + " мс — остановка");
                    break;
                }
            }
        } catch (WakeupException expectedOnShutdown) {
            event(id, "остановка по сигналу");
        } finally {
            try {
                if (manual && !done.isEmpty()) {
                    consumer.commitSync(done);
                    event(id, "финальный коммит " + offsets(done));
                }
            } catch (KafkaException e) {
                fail(failures, id, "финальный коммит", e);
            } finally {
                consumer.close();
                if (out != null) {
                    out.close();
                }
            }
            System.out.println();
            System.out.printf("-------------------- итог %s --------------------%n", id);
            System.out.printf("обработано: %d; событий: отзыв %d, назначение %d, потеря %d%n",
                    processed, events[0], events[1], events[2]);
            if (!failures.isEmpty()) {
                System.out.println("ошибки коммита:");
                failures.forEach((type, n) -> System.out.printf("  %s x%d%n", type, n));
            }
        }
    }

    private static void event(String id, String text) {
        System.out.printf("%s %s  %s%n", LocalTime.now().format(TIME), id, text);
    }

    private static void fail(Map<String, Integer> failures, String id, String what, Exception e) {
        failures.merge(e.getClass().getSimpleName(), 1, Integer::sum);
        event(id, what + " не прошёл: " + e.getClass().getSimpleName() + " — " + e.getMessage());
    }

    private static String nums(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::partition).sorted().toList().toString();
    }

    private static String offsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        return offsets.entrySet().stream()
                .sorted(Map.Entry.comparingByKey((a, b) -> Integer.compare(a.partition(), b.partition())))
                .map(e -> e.getKey().partition() + ":" + e.getValue().offset())
                .collect(Collectors.joining(" ", "{", "}"));
    }

    private static String seqOf(ConsumerRecord<String, String> r) {
        int bar = r.value().indexOf('|');
        return bar > 0 ? r.value().substring(0, bar) : r.partition() + "-" + r.offset();
    }

    private static double metric(KafkaConsumer<?, ?> consumer, String name) {
        for (Map.Entry<MetricName, ? extends Metric> e : consumer.metrics().entrySet()) {
            if (e.getKey().group().equals("consumer-fetch-manager-metrics")
                    && e.getKey().name().equals(name)
                    && !e.getKey().tags().containsKey("partition")
                    && !e.getKey().tags().containsKey("topic")) {
                return e.getValue().metricValue() instanceof Number n ? n.doubleValue() : Double.NaN;
            }
        }
        return Double.NaN;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
