package dev.learning.kafka.own.intake;

import dev.learning.kafka.common.OrderEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class OrderGeneratorTest {

    @Test
    @DisplayName("за длительность T при скорости R создаётся ровно R × T заказов")
    void exactCount() throws InterruptedException {
        List<OrderEvent> orders = new CopyOnWriteArrayList<>();

        runToEnd(new OrderGenerator(orders::add, 500, 2_000));

        assertThat(orders).hasSize(1_000);
    }

    @Test
    @DisplayName("заказы распределены по всей длительности, а не уходят пачкой")
    void ordersAreSpreadOverDuration() throws InterruptedException {
        List<Long> times = new CopyOnWriteArrayList<>();

        long start = System.nanoTime();
        runToEnd(new OrderGenerator(o -> times.add(System.nanoTime()), 500, 2_000));

        long firstMs = (times.get(0) - start) / 1_000_000;
        long lastMs = (times.get(times.size() - 1) - start) / 1_000_000;
        assertThat(firstMs).isLessThan(100);
        assertThat(lastMs).isBetween(1_900L, 2_100L);

        long inFirstHalf = times.stream().filter(t -> (t - start) / 1_000_000 < 1_000).count();
        assertThat(inFirstHalf).isBetween(450L, 550L);
    }

    @Test
    @DisplayName("идентификаторы заказов уникальны")
    void orderIdsAreUnique() throws InterruptedException {
        List<OrderEvent> orders = new CopyOnWriteArrayList<>();

        runToEnd(new OrderGenerator(orders::add, 1_000, 1_000));

        assertThat(orders.stream().map(OrderEvent::orderId).distinct().count()).isEqualTo(orders.size());
    }

    @Test
    @DisplayName("идентификаторы не повторяются между двумя запусками")
    void orderIdsDoNotRepeatAcrossRuns() throws InterruptedException {
        List<OrderEvent> first = new CopyOnWriteArrayList<>();
        List<OrderEvent> second = new CopyOnWriteArrayList<>();

        runToEnd(new OrderGenerator(first::add, 500, 500));
        runToEnd(new OrderGenerator(second::add, 500, 500));

        assertThat(first).extracting(OrderEvent::orderId)
                         .doesNotContainAnyElementsOf(second.stream().map(OrderEvent::orderId).toList());
    }

    @Test
    @DisplayName("самый крупный продавец получает около 5% заказов, продавцов порядка тысячи")
    void sellerDistribution() throws InterruptedException {
        List<OrderEvent> orders = new CopyOnWriteArrayList<>();

        runToEnd(new OrderGenerator(orders::add, 2_000, 5_000));

        Map<String, Long> perSeller = orders.stream()
                                            .collect(Collectors.groupingBy(OrderEvent::sellerId, Collectors.counting()));
        long top = perSeller.values().stream().mapToLong(Long::longValue).max().orElse(0);
        double topShare = (double) top / orders.size();

        assertThat(topShare).isBetween(0.03, 0.07);
        assertThat(perSeller.size()).isBetween(900, 1_100);
    }

    @Test
    @DisplayName("stop() завершает генератор задолго до срока")
    void stopTerminatesEarly() throws InterruptedException {
        List<OrderEvent> orders = new CopyOnWriteArrayList<>();
        OrderGenerator generator = new OrderGenerator(orders::add, 10, 60_000);
        Thread thread = new Thread(generator);

        thread.start();
        Thread.sleep(300);
        generator.stop();
        thread.join(1_000);

        assertThat(thread.isAlive()).isFalse();
        assertThat(orders.size()).isBetween(2, 5);
    }

    @Test
    @DisplayName("interrupt() завершает генератор")
    void interruptTerminates() throws InterruptedException {
        OrderGenerator generator = new OrderGenerator(o -> { }, 1, 60_000);
        Thread thread = new Thread(generator);

        thread.start();
        Thread.sleep(300);
        thread.interrupt();
        thread.join(1_000);

        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    @DisplayName("stop() срабатывает, даже если генератор отстал от расписания")
    void stopWorksWhileBehindSchedule() throws InterruptedException {
        OrderGenerator[] ref = new OrderGenerator[1];
        List<OrderEvent> orders = new CopyOnWriteArrayList<>();
        // Получатель тормозит: каждый заказ занимает 50 мс при периоде 2 мс —
        // генератор всё время отстаёт и не заходит в ожидание.
        ref[0] = new OrderGenerator(o -> {
            orders.add(o);
            sleep(50);
        }, 500, 60_000);
        Thread thread = new Thread(ref[0]);

        thread.start();
        Thread.sleep(300);
        ref[0].stop();
        thread.join(1_000);

        assertThat(thread.isAlive()).isFalse();
    }

    private static void runToEnd(OrderGenerator generator) throws InterruptedException {
        Thread thread = new Thread(generator);
        thread.start();
        thread.join(30_000);
        assertThat(thread.isAlive()).as("генератор должен завершиться сам по сроку").isFalse();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}