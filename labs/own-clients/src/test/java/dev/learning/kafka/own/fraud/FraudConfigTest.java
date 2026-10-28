package dev.learning.kafka.own.fraud;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FraudConfigTest {

    private FraudConfig create(String... args) {
        FraudConfig config = new FraudConfig();
        config.initProps(args);
        return config;
    }

    // ---------- значения по умолчанию ----------

    @Test
    @DisplayName("настройки потребителя по умолчанию: всё, без чего потребитель не создастся или будет вести себя не так")
    void consumerDefaults() {
        Map<String, Object> props = create().getConsumerProperties();

        assertThat(props).containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertThat(props)
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "fraud-check")
                .containsEntry(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer")
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        assertThat(props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG).toString()).isEqualTo("100");
        assertThat(props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG).toString())
                .contains(StringDeserializer.class.getName());
        assertThat(props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).toString())
                .contains(StringDeserializer.class.getName());
    }

    @Test
    @DisplayName("размеры по умолчанию — из расчёта: пул 96, очередь 300, пороги 200 и 80")
    void sizingDefaults() {
        FraudConfig config = create();

        assertThat(config.getPoolSize()).isEqualTo(96);
        assertThat(config.getQueueCapacity()).isEqualTo(300);
        assertThat(config.getPauseHigh()).isEqualTo(200);
        assertThat(config.getPauseLow()).isEqualTo(80);
    }

    @Test
    @DisplayName("задержка проверки по умолчанию — 50–200 мс, как в общем контексте")
    void checkDelayDefaults() {
        FraudConfig config = create();

        assertThat(config.getCheckMinMs()).isEqualTo(50);
        assertThat(config.getCheckMaxMs()).isEqualTo(200);
    }

    @Test
    @DisplayName("периоды по умолчанию — в миллисекундах и осмысленного порядка")
    void periodDefaults() {
        FraudConfig config = create();

        // коммит не чаще нескольких раз в секунду: иначе сотни запросов к координатору
        assertThat(config.getCommitIntervalMs()).as("commit.interval.ms").isBetween(100L, 10_000L);
        // ожидание пула — секунды: расчёт даёт около 3 с при деградации, с запасом в 2–3 раза
        assertThat(config.getDrainTimeoutMs()).as("drain.timeout.ms").isBetween(3_000L, 30_000L);
        // вывод счётчиков — раз в секунды, а не на каждом витке
        assertThat(config.getPrintPeriodMs()).as("print.period").isBetween(1_000L, 60_000L);
    }

    @Test
    @DisplayName("по умолчанию деградации нет")
    void noDegradationByDefault() {
        FraudConfig config = create();

        assertThat(config.getSlowDurationMs()).isZero();
    }

    @Test
    @DisplayName("drain.timeout.ms по умолчанию меньше max.poll.interval.ms")
    void drainBelowPollInterval() {
        FraudConfig config = create();
        long maxPollInterval = Long.parseLong(
                config.getConsumerProperties().get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG).toString());

        assertThat(config.getDrainTimeoutMs()).isLessThan(maxPollInterval);
    }

    // ---------- разбор параметров ----------

    @Test
    @DisplayName("параметры сервиса разбираются и не попадают в настройки потребителя")
    void serviceParamsParsed() {
        FraudConfig config = create("pool.size=50", "queue.capacity=400", "pause.high=250", "pause.low=100",
                                    "check.min.ms=10", "check.max.ms=20", "commit.interval.ms=2000", "drain.timeout.ms=5000",
                                    "print.period=3000");

        assertThat(config.getPoolSize()).isEqualTo(50);
        assertThat(config.getQueueCapacity()).isEqualTo(400);
        assertThat(config.getPauseHigh()).isEqualTo(250);
        assertThat(config.getPauseLow()).isEqualTo(100);
        assertThat(config.getCheckMinMs()).isEqualTo(10);
        assertThat(config.getCheckMaxMs()).isEqualTo(20);
        assertThat(config.getCommitIntervalMs()).isEqualTo(2000);
        assertThat(config.getDrainTimeoutMs()).isEqualTo(5000);
        assertThat(config.getPrintPeriodMs()).isEqualTo(3000);
        assertThat(config.getConsumerProperties()).doesNotContainKeys("pool.size", "queue.capacity", "pause.high",
                                                                      "pause.low", "check.min.ms", "check.max.ms", "commit.interval.ms", "drain.timeout.ms", "print.period");
    }

    @Test
    @DisplayName("окно деградации разбирается")
    void degradationParsed() {
        FraudConfig config = create("slow.after.ms=60000", "slow.duration.ms=120000", "slow.delay.ms=1000");

        assertThat(config.getSlowAfterMs()).isEqualTo(60_000);
        assertThat(config.getSlowDurationMs()).isEqualTo(120_000);
        assertThat(config.getSlowDelayMs()).isEqualTo(1_000);
    }

    @Test
    @DisplayName("начало деградации без длительности — ошибка: окно без конца бессмысленно")
    void slowAfterWithoutDuration() {
        assertThatThrownBy(() -> create("slow.after.ms=60000")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("окно деградации без задержки — ошибка: сценарий 4-В молча прошёл бы без деградации")
    void slowWindowWithoutDelay() {
        assertThatThrownBy(() -> create("slow.after.ms=60000", "slow.duration.ms=120000"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("окно деградации с самого старта — допустимо")
    void slowFromStart() {
        FraudConfig config = create("slow.after.ms=0", "slow.duration.ms=10000", "slow.delay.ms=1000");

        assertThat(config.getSlowAfterMs()).isZero();
        assertThat(config.getSlowDurationMs()).isEqualTo(10_000);
    }

    @Test
    @DisplayName("ошибка переполнения называет параметр")
    void overflowMessageNamesKey() {
        assertThatThrownBy(() -> create("queue.capacity=5000000000"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queue.capacity");
    }

    @Test
    @DisplayName("настройки потребителя из аргументов перекрывают умолчания: group.id, client.id")
    void consumerOverrides() {
        Map<String, Object> props = create("group.id=fraud-b", "client.id=f1").getConsumerProperties();

        assertThat(props)
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "fraud-b")
                .containsEntry(ConsumerConfig.CLIENT_ID_CONFIG, "f1");
    }

    @Test
    @DisplayName("каталог журнала: существующий принимается, несуществующий — ошибка")
    void journalDir(@TempDir Path dir) {
        assertThat(create("journal.dir=" + dir).getJournalLogDir()).isEqualTo(dir.toString());
        assertThatThrownBy(() -> create("journal.dir=" + dir.resolve("nope")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("нечисловое, нулевое или отрицательное значение размера — ошибка")
    void invalidSizes() {
        for (String arg : new String[] {"pool.size=abc", "pool.size=0", "queue.capacity=-1", "pause.high=0"}) {
            assertThatThrownBy(() -> create(arg)).as(arg).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("размер, не помещающийся в int, — ошибка, а не тихое переполнение")
    void intOverflow() {
        for (String arg : new String[] {"pool.size=5000000000", "queue.capacity=5000000000",
                                        "pause.high=5000000000", "pause.low=5000000000"}) {
            assertThatThrownBy(() -> create(arg)).as(arg).isInstanceOf(IllegalArgumentException.class);
        }
    }

    // ---------- согласованность ----------

    @Test
    @DisplayName("pause.high не больше pause.low — ошибка")
    void thresholdsOrder() {
        assertThatThrownBy(() -> create("pause.high=80", "pause.low=80")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> create("pause.high=50", "pause.low=80")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("порог плюс пачка не помещаются в очередь и пул — ошибка")
    void capacityTooSmall() {
        // 400 + 100 > 300 + 96
        assertThatThrownBy(() -> create("pause.high=400")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("max.poll.records из аргументов участвует в проверке ёмкости")
    void maxPollRecordsFromArgs() {
        // 200 + 300 > 300 + 96
        assertThatThrownBy(() -> create("max.poll.records=300")).isInstanceOf(IllegalArgumentException.class);
        // 200 + 100 <= 300 + 96
        assertThat(create("max.poll.records=100").getPauseHigh()).isEqualTo(200);
    }

    @Test
    @DisplayName("проверка ёмкости проходит на границе: порог + пачка = очередь + пул")
    void capacityExactlyEnough() {
        // 296 + 100 = 300 + 96
        assertThat(create("pause.high=296").getPauseHigh()).isEqualTo(296);
    }

    @Test
    @DisplayName("check.max.ms меньше check.min.ms — ошибка")
    void checkRange() {
        assertThatThrownBy(() -> create("check.min.ms=200", "check.max.ms=50"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("drain.timeout.ms не меньше max.poll.interval.ms — ошибка; с увеличенным интервалом — проходит")
    void drainVsPollInterval() {
        assertThatThrownBy(() -> create("drain.timeout.ms=45000")).isInstanceOf(IllegalArgumentException.class);
        assertThat(create("drain.timeout.ms=45000", "max.poll.interval.ms=300000").getDrainTimeoutMs())
                .isEqualTo(45_000);
    }

    @Test
    @DisplayName("порядок аргументов не влияет на проверки согласованности")
    void orderIndependent() {
        assertThat(create("pause.low=100", "pause.high=250").getPauseHigh()).isEqualTo(250);
        assertThat(create("pause.high=250", "pause.low=100").getPauseHigh()).isEqualTo(250);
    }

    @Test
    @DisplayName("карта настроек потребителя снаружи неизменяема")
    void consumerPropsImmutable() {
        Map<String, Object> props = create().getConsumerProperties();

        assertThatThrownBy(() -> props.put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }
}