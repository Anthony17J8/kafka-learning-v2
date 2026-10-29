package dev.learning.kafka.own.fraud;

import dev.learning.kafka.common.OrderEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Тесты на реальном времени, с короткими интервалами: каждый — доли секунды.
 * Конструктор: FraudCheckService(checkMinMs, checkMaxMs, new Delay(slowAfterMs, slowDurationMs, slowDelayMs)).
 * Моменты проверок в тесте окна выбраны в серединах интервалов, а не у границ:
 * первая проверка в JVM бывает медленнее из-за загрузки классов и JIT, и запас от границы окна это поглощает.
 */
class FraudCheckServiceTest {

    private static OrderEvent order(String id, long amount) {
        return OrderEvent.sample(id, "seller-1", amount, "standard");
    }

    private static long timeMs(Runnable r) {
        long start = System.nanoTime();
        r.run();
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static void sleepUntil(long startNanos, long offsetMs) throws InterruptedException {
        long left = offsetMs - (System.nanoTime() - startNanos) / 1_000_000;
        if (left > 0) {
            Thread.sleep(left);
        }
    }

    @Test
    @DisplayName("обычная проверка занимает время в пределах [min, max]")
    void normalDurationWithinRange() {
        FraudCheckService service = new FraudCheckService(30, 60, new FraudCheckService.Delay(0, 0, 0));

        for (int i = 0; i < 5; i++) {
            OrderEvent o = order("o-" + i, 100);
            long ms = timeMs(() -> service.check(o));
            assertThat(ms).as("проверка %d", i).isBetween(30L, 60L + 40L);
        }
    }

    @Test
    @DisplayName("вердикт детерминирован: один и тот же заказ — один и тот же вердикт, в том числе в другом экземпляре сервиса")
    void deterministicVerdict() {
        FraudCheckService first = new FraudCheckService(1, 2, new FraudCheckService.Delay(0, 0, 0));
        FraudCheckService second = new FraudCheckService(1, 2, new FraudCheckService.Delay(0, 0, 0));
        OrderEvent small = order("o-small", 100);
        OrderEvent large = order("o-large", 10_000_000);

        FraudCheckService.Verdict smallVerdict = first.check(small);
        FraudCheckService.Verdict largeVerdict = first.check(large);

        for (int i = 0; i < 5; i++) {
            assertThat(first.check(small)).isEqualTo(smallVerdict);
            assertThat(second.check(small)).isEqualTo(smallVerdict);
            assertThat(second.check(large)).isEqualTo(largeVerdict);
        }
    }

    @Test
    @DisplayName("без окна деградации все проверки обычные")
    void noWindowNoDelay() {
        FraudCheckService service = new FraudCheckService(10, 10, new FraudCheckService.Delay(0, 0, 0));

        long ms = timeMs(() -> service.check(order("o-1", 100)));

        assertThat(ms).isLessThan(200);
    }

    @Test
    @DisplayName("окно деградации: до окна — обычно, в окне — не меньше slow.delay.ms, после окна — снова обычно")
    void degradationWindow() throws InterruptedException {
        long start = System.nanoTime();
        // окно: с 200 до 600 мс от создания, задержка в окне 300 мс;
        // проверка внутри окна стартует в 400 — в середине, по 200 мс до каждой границы
        FraudCheckService service = new FraudCheckService(10, 10, new FraudCheckService.Delay(200, 400, 300));

        long before = timeMs(() -> service.check(order("o-1", 100)));
        sleepUntil(start, 400);
        long inside = timeMs(() -> service.check(order("o-2", 100)));
        sleepUntil(start, 700);
        long after = timeMs(() -> service.check(order("o-3", 100)));

        assertThat(before).as("до окна").isLessThan(150);
        assertThat(inside).as("в окне").isGreaterThanOrEqualTo(300);
        assertThat(after).as("после окна").isLessThan(150);
    }

    @Test
    @DisplayName("окно с нулевым началом — деградация с первой проверки")
    void windowFromStart() {
        FraudCheckService service = new FraudCheckService(10, 10, new FraudCheckService.Delay(0, 1000, 200));

        long ms = timeMs(() -> service.check(order("o-1", 100)));

        assertThat(ms).isGreaterThanOrEqualTo(200);
    }

    @Test
    @DisplayName("решение о задержке — в момент начала проверки: начатая до окна проверка остаётся обычной")
    void delayDecidedAtStart() throws InterruptedException {
        long start = System.nanoTime();
        // окно с 50 мс; проверка стартует сразу и длится 100 мс — то есть заканчивается внутри окна
        FraudCheckService service = new FraudCheckService(100, 100, new FraudCheckService.Delay(50, 1000, 500));

        long ms = timeMs(() -> service.check(order("o-1", 100)));

        assertThat(ms).as("начата до окна").isLessThan(300);
    }

    @Test
    @DisplayName("сервис не сериализует вызовы: 50 одновременных проверок по 50 мс идут параллельно")
    void parallelCalls() throws Exception {
        FraudCheckService service = new FraudCheckService(50, 50, new FraudCheckService.Delay(0, 0, 0));
        ExecutorService pool = Executors.newFixedThreadPool(50);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<FraudCheckService.Verdict>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 50; i++) {
                OrderEvent o = order("p-" + i, 100);
                futures.add(pool.submit(() -> {
                    go.await();
                    return service.check(o);
                }));
            }
            long started = System.nanoTime();
            go.countDown();
            for (Future<FraudCheckService.Verdict> f : futures) {
                assertThat(f.get(5, TimeUnit.SECONDS)).isNotNull();
            }
            long ms = (System.nanoTime() - started) / 1_000_000;

            assertThat(ms).as("последовательно было бы 2500 мс").isLessThan(1_000);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("прерывание во время проверки: вердикт не возвращается, флаг прерывания сохранён")
    void interruptionDoesNotProduceVerdict() throws InterruptedException {
        FraudCheckService service = new FraudCheckService(2_000, 2_000, new FraudCheckService.Delay(0, 0, 0));
        AtomicReference<FraudCheckService.Verdict> verdict = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean interruptedAfter = new AtomicBoolean();

        Thread t = new Thread(() -> {
            try {
                verdict.set(service.check(order("o-int", 100)));
            } catch (Throwable e) {
                error.set(e);
            } finally {
                interruptedAfter.set(Thread.currentThread().isInterrupted());
            }
        });
        t.start();
        Thread.sleep(100);
        t.interrupt();
        t.join(3_000);

        assertThat(verdict.get())
                .as("прерванная проверка не должна выдавать вердикт: он будет опубликован и закоммичен как настоящий")
                .isNull();
        assertThat(error.get()).as("прерывание сообщается исключением").isNotNull();
        assertThat(interruptedAfter.get()).as("флаг прерывания восстановлен").isTrue();
    }
}