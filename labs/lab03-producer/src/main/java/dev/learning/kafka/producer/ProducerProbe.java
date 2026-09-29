package dev.learning.kafka.producer;

import dev.learning.kafka.common.Env;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Модуль 3: продюсер-зонд для экспериментов с настройками и поломками.
 *
 * Все аргументы — пары key=value. Собственные опции зонда перечислены ниже,
 * всё остальное передаётся в конфигурацию продюсера как есть:
 *
 *   mvn -q -pl lab03-producer exec:java \
 *     -Dexec.mainClass=dev.learning.kafka.producer.ProducerProbe \
 *     -Dexec.args="topic=m3.probe.v1 count=10000 rate=1000 linger.ms=0 acks=1"
 *
 * Опции зонда:
 *   topic              топик, по умолчанию m3.probe.v1
 *   count              сколько записей отправить; 0 — до Ctrl+C
 *   rate               записей в секунду; 0 — без ограничения
 *   size               размер значения в байтах, по умолчанию 200
 *   keys               0 — без ключа; N — ключи key-0 .. key-(N-1) по кругу
 *   premium.every      каждая N-я запись получает ключ premium-* (для TenantPartitioner)
 *   callback           true/false — передавать ли callback в send()
 *   callback.sleep.ms  искусственная работа в callback
 *   callback.threads   0 — работа в Sender-потоке; N — вынести в пул из N потоков
 *   acked.file         файл, куда пишутся номера подтверждённых записей
 *   report             период строки прогресса в секундах, по умолчанию 1
 *
 * Значение записи начинается с её номера: "000000042|xxxx...". Это позволяет
 * проверить дубли, порядок и потери, читая топик console-консьюмером.
 */
public final class ProducerProbe {

    private static final Set<String> OWN_OPTIONS = Set.of(
            "topic", "count", "rate", "size", "keys", "premium.every",
            "callback", "callback.sleep.ms", "callback.threads", "acked.file", "report");

    private static final List<String> SUMMARY_METRICS = List.of(
            "batch-size-avg", "records-per-request-avg", "record-queue-time-avg",
            "request-latency-avg", "compression-rate-avg", "record-retry-total",
            "record-error-total", "bufferpool-wait-time");

    private static volatile boolean stop = false;

    private ProducerProbe() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opt = new HashMap<>();
        Properties props = Env.base();
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

        String topic = opt.getOrDefault("topic", "m3.probe.v1");
        long count = Long.parseLong(opt.getOrDefault("count", "100000"));
        int rate = Integer.parseInt(opt.getOrDefault("rate", "0"));
        int size = Integer.parseInt(opt.getOrDefault("size", "200"));
        int keys = Integer.parseInt(opt.getOrDefault("keys", "0"));
        int premiumEvery = Integer.parseInt(opt.getOrDefault("premium.every", "0"));
        boolean useCallback = Boolean.parseBoolean(opt.getOrDefault("callback", "true"));
        long callbackSleepMs = Long.parseLong(opt.getOrDefault("callback.sleep.ms", "0"));
        int callbackThreads = Integer.parseInt(opt.getOrDefault("callback.threads", "0"));
        String ackedFile = opt.get("acked.file");
        int reportSec = Integer.parseInt(opt.getOrDefault("report", "1"));

        String padding = "x".repeat(Math.max(0, size - 10));
        long[] latencies = new long[(int) Math.min(count == 0 ? 2_000_000 : count, 2_000_000)];

        LongAdder sent = new LongAdder();
        LongAdder acked = new LongAdder();
        LongAdder failed = new LongAdder();
        Map<String, LongAdder> asyncErrors = new ConcurrentHashMap<>();
        Map<String, LongAdder> syncErrors = new ConcurrentHashMap<>();
        Map<String, String> firstMessage = new ConcurrentHashMap<>();
        AtomicLong maxBlockNs = new AtomicLong();
        AtomicLong intervalMaxBlockNs = new AtomicLong();

        BufferedWriter ackedOut = ackedFile == null ? null : Files.newBufferedWriter(Path.of(ackedFile));
        ExecutorService offload = callbackThreads > 0 ? Executors.newFixedThreadPool(callbackThreads) : null;

        // Ctrl+C: прекращаем отправку и даём main-потоку дождаться доставки и напечатать итог.
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop = true;
            try {
                mainThread.join(180_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

        long start = System.nanoTime();
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "probe-reporter");
            t.setDaemon(true);
            return t;
        });
        reporter.scheduleAtFixedRate(() -> System.out.printf(
                "t=%3dс  send()=%d  подтверждено=%d  ошибок=%d  send() макс=%d мс  "
                        + "буфер свободен=%.0f КБ  queue-time=%.1f мс  request-latency=%.1f мс%n",
                (System.nanoTime() - start) / 1_000_000_000L,
                sent.sum(), acked.sum(), failed.sum(),
                intervalMaxBlockNs.getAndSet(0) / 1_000_000,
                metric(producer, "buffer-available-bytes") / 1024,
                metric(producer, "record-queue-time-avg"),
                metric(producer, "request-latency-avg")),
                reportSec, reportSec, TimeUnit.SECONDS);

        long seq = 0;
        while (!stop && (count == 0 || seq < count)) {
            if (rate > 0) {
                long wait = start + seq * 1_000_000_000L / rate - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                }
            }

            final long s = seq;
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, keyFor(s, keys, premiumEvery), String.format("%09d|", s) + padding);

            long t0 = System.nanoTime();
            try {
                if (useCallback) {
                    producer.send(record, (meta, ex) -> {
                        if (ex == null) {
                            if (s < latencies.length) {
                                latencies[(int) s] = System.nanoTime() - t0;
                            }
                            acked.increment();
                            if (ackedOut != null) {
                                writeLine(ackedOut, s);
                            }
                        } else {
                            failed.increment();
                            tally(asyncErrors, firstMessage, ex);
                        }
                        if (callbackSleepMs > 0) {
                            if (offload != null) {
                                offload.submit(() -> sleep(callbackSleepMs));
                            } else {
                                sleep(callbackSleepMs);
                            }
                        }
                    });
                } else {
                    producer.send(record);
                }
            } catch (RuntimeException e) {
                tally(syncErrors, firstMessage, e);
            }
            long blocked = System.nanoTime() - t0;
            maxBlockNs.accumulateAndGet(blocked, Math::max);
            intervalMaxBlockNs.accumulateAndGet(blocked, Math::max);
            sent.increment();
            seq++;
        }

        System.out.println("Отправка остановлена. Жду завершения доставки (не дольше delivery.timeout.ms)...");
        producer.flush();
        long elapsedMs = Math.max(1, (System.nanoTime() - start) / 1_000_000);
        reporter.shutdownNow();

        System.out.println();
        System.out.println("-------------------- итог --------------------");
        System.out.printf("вызовов send(): %d, подтверждено: %d, ошибок доставки: %d, время: %d мс%n",
                sent.sum(), acked.sum(), failed.sum(), elapsedMs);
        if (!useCallback) {
            System.out.println("callback выключен: приложение не знает, что стало с записями");
        }
        System.out.printf("пропускная способность: %d подтверждённых записей/с%n", acked.sum() * 1000 / elapsedMs);
        printErrors("ошибки из callback", asyncErrors, firstMessage);
        printErrors("исключения из send()", syncErrors, firstMessage);
        printLatencies(latencies);
        System.out.printf("send() блокировался максимум: %d мс%n", maxBlockNs.get() / 1_000_000);

        System.out.println("метрики продюсера:");
        Map<String, Object> summary = new TreeMap<>();
        for (Map.Entry<MetricName, ? extends Metric> e : producer.metrics().entrySet()) {
            MetricName name = e.getKey();
            if (name.group().equals("producer-metrics")
                    && SUMMARY_METRICS.stream().anyMatch(name.name()::startsWith)) {
                summary.put(name.name(), e.getValue().metricValue());
            }
        }
        summary.forEach((k, v) -> System.out.printf("  %-32s %s%n", k, format(v)));

        producer.close(Duration.ofSeconds(5));
        if (offload != null) {
            offload.shutdown();
        }
        if (ackedOut != null) {
            synchronized (ackedOut) {
                ackedOut.close();
            }
        }
    }

    private static String keyFor(long seq, int keys, int premiumEvery) {
        if (premiumEvery > 0 && seq % premiumEvery == 0) {
            return "premium-" + (keys > 0 ? seq % keys : seq);
        }
        return keys > 0 ? "key-" + (seq % keys) : null;
    }

    private static void tally(Map<String, LongAdder> errors, Map<String, String> firstMessage, Throwable e) {
        String type = e.getClass().getSimpleName();
        errors.computeIfAbsent(type, k -> new LongAdder()).increment();
        firstMessage.putIfAbsent(type, String.valueOf(e.getMessage()));
    }

    private static void printErrors(String title, Map<String, LongAdder> errors, Map<String, String> firstMessage) {
        if (errors.isEmpty()) {
            return;
        }
        System.out.println(title + ":");
        errors.forEach((type, n) -> System.out.printf("  %s x%d — %s%n", type, n.sum(), firstMessage.get(type)));
    }

    private static void printLatencies(long[] raw) {
        long[] ok = Arrays.stream(raw).filter(v -> v > 0).sorted().toArray();
        if (ok.length == 0) {
            return;
        }
        System.out.printf("задержка send() → подтверждение, мс: p50=%.1f p95=%.1f p99=%.1f max=%.1f%n",
                pct(ok, 0.50), pct(ok, 0.95), pct(ok, 0.99), ok[ok.length - 1] / 1e6);
    }

    private static double pct(long[] sorted, double q) {
        int idx = (int) Math.ceil(q * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, idx))] / 1e6;
    }

    private static double metric(Producer<?, ?> producer, String name) {
        for (Map.Entry<MetricName, ? extends Metric> e : producer.metrics().entrySet()) {
            if (e.getKey().group().equals("producer-metrics") && e.getKey().name().equals(name)) {
                return e.getValue().metricValue() instanceof Number n ? n.doubleValue() : Double.NaN;
            }
        }
        return Double.NaN;
    }

    private static String format(Object value) {
        return value instanceof Double d ? String.format("%.2f", d) : String.valueOf(value);
    }

    private static void writeLine(BufferedWriter out, long seq) {
        synchronized (out) {
            try {
                out.write(String.format("%09d", seq));
                out.newLine();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
