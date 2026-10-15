package dev.learning.kafka.own.processing;

import dev.learning.kafka.common.OrderEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Тесты опираются на схему из BalanceStore: таблицы balances(seller_id, amount)
 * и orders(order_id, seller_id, amount). Если переименуете — поправьте SQL в хелперах внизу.
 */
class BalanceStoreTest {

    @TempDir
    Path dir;

    private String url;
    private final List<BalanceStore> opened = new ArrayList<>();

    @BeforeEach
    void setUp() {
        url = "jdbc:h2:file:" + dir.resolve("db") + ";AUTO_SERVER=TRUE;LOCK_TIMEOUT=10000";
    }

    @AfterEach
    void tearDown() {
        opened.forEach(BalanceStoreTest::closeQuietly);
    }

    @Test
    @DisplayName("open() создаёт обе таблицы")
    void openCreatesTables() throws SQLException {
        open();

        assertThat(count("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN ('BALANCES', 'ORDERS')"))
                .isEqualTo(2);
    }

    @Test
    @DisplayName("повторный open() на существующей базе не падает и не теряет данные")
    void openIsIdempotent() throws SQLException {
        BalanceStore first = open();
        first.apply(order("o-1", "s-1", 100));
        closeQuietly(first);
        opened.remove(first);

        open();

        assertThat(balance("s-1")).isEqualTo(100);
    }

    @Test
    @DisplayName("новый заказ — APPLIED, баланс равен сумме")
    void newOrderApplied() throws SQLException {
        BalanceStore store = open();

        assertThat(store.apply(order("o-1", "s-1", 100))).isEqualTo(BalanceStore.BalanceOperationResult.APPLIED);
        assertThat(balance("s-1")).isEqualTo(100);
        assertThat(count("SELECT COUNT(*) FROM orders")).isEqualTo(1);
    }

    @Test
    @DisplayName("второй заказ существующего продавца сохраняется — проверка другим соединением")
    void secondOrderOfExistingSellerIsCommitted() throws SQLException {
        BalanceStore store = open();

        store.apply(order("o-1", "s-1", 100));
        store.apply(order("o-2", "s-1", 50));

        assertThat(balance("s-1")).isEqualTo(150);
        assertThat(count("SELECT COUNT(*) FROM orders")).isEqualTo(2);
    }

    @Test
    @DisplayName("повтор заказа — DUPLICATE, баланс не меняется")
    void duplicateDoesNotChangeBalance() throws SQLException {
        BalanceStore store = open();
        OrderEvent order = order("o-1", "s-1", 100);

        store.apply(order);
        BalanceStore.BalanceOperationResult second = store.apply(order);

        assertThat(second).isEqualTo(BalanceStore.BalanceOperationResult.DUPLICATE);
        assertThat(balance("s-1")).isEqualTo(100);
        assertThat(count("SELECT COUNT(*) FROM orders")).isEqualTo(1);
    }

    @Test
    @DisplayName("балансы разных продавцов независимы")
    void sellersAreIndependent() throws SQLException {
        BalanceStore store = open();

        store.apply(order("o-1", "s-1", 100));
        store.apply(order("o-2", "s-2", 30));
        store.apply(order("o-3", "s-1", 5));

        assertThat(balance("s-1")).isEqualTo(105);
        assertThat(balance("s-2")).isEqualTo(30);
    }

    @Test
    @DisplayName("данные переживают закрытие и повторное открытие")
    void dataSurvivesReopen() throws SQLException {
        BalanceStore store = open();
        store.apply(order("o-1", "s-1", 100));
        store.apply(order("o-2", "s-1", 20));
        closeQuietly(store);
        opened.remove(store);

        BalanceStore reopened = open();

        assertThat(balance("s-1")).isEqualTo(120);
        assertThat(reopened.apply(order("o-1", "s-1", 100))).isEqualTo(BalanceStore.BalanceOperationResult.DUPLICATE);
    }

    @Test
    @DisplayName("тысяча заказов: сумма балансов равна сумме заказов")
    void manyOrders() throws SQLException {
        BalanceStore store = open();
        long expected = 0;
        for (int i = 0; i < 1_000; i++) {
            store.apply(order("o-" + i, "s-" + (i % 37), i + 1));
            expected += i + 1;
        }

        assertThat(count("SELECT SUM(amount) FROM balances")).isEqualTo(expected);
        assertThat(count("SELECT COUNT(*) FROM orders")).isEqualTo(1_000);
    }

    @Test
    @DisplayName("гонка: два хранилища одновременно применяют один и тот же заказ — учтён один раз")
    void sameOrderConcurrently() throws Exception {
        BalanceStore a = open();
        BalanceStore b = open();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 100; i++) {
                OrderEvent order = order("race-" + i, "seller-race-" + i, 10);
                List<Future<BalanceStore.BalanceOperationResult>> results = runTogether(pool,
                                                                                        () -> a.apply(order),
                                                                                        () -> b.apply(order));

                List<BalanceStore.BalanceOperationResult> outcomes = new ArrayList<>();
                for (Future<BalanceStore.BalanceOperationResult> f : results) {
                    outcomes.add(f.get());
                }
                assertThat(outcomes).as("итерация " + i)
                                    .containsExactlyInAnyOrder(BalanceStore.BalanceOperationResult.APPLIED,
                                                               BalanceStore.BalanceOperationResult.DUPLICATE);
                assertThat(balance("seller-race-" + i)).as("итерация " + i).isEqualTo(10);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("гонка: два хранилища одновременно впервые видят продавца — обе суммы учтены")
    void newSellerConcurrently() throws Exception {
        BalanceStore a = open();
        BalanceStore b = open();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 100; i++) {
                String seller = "new-seller-" + i;
                OrderEvent first = order(UUID.randomUUID().toString(), seller, 7);
                OrderEvent second = order(UUID.randomUUID().toString(), seller, 5);
                List<Future<BalanceStore.BalanceOperationResult>> results = runTogether(pool,
                                                                                        () -> a.apply(first),
                                                                                        () -> b.apply(second));

                for (Future<BalanceStore.BalanceOperationResult> f : results) {
                    assertThat(f.get()).as("итерация " + i).isEqualTo(BalanceStore.BalanceOperationResult.APPLIED);
                }
                assertThat(balance(seller)).as("итерация " + i).isEqualTo(12);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // ---------- хелперы ----------

    private BalanceStore open() {
        BalanceStore store = new BalanceStore(url);
        try {
            store.open();
        } catch (Exception e) {
            throw new IllegalStateException("BalanceStore.open() упал", e);
        }
        opened.add(store);
        return store;
    }

    private static void closeQuietly(BalanceStore store) {
        try {
            store.close();
        } catch (Exception ignored) {
            // закрытие в тестах не проверяется
        }
    }

    private static OrderEvent order(String orderId, String sellerId, long amount) {
        return OrderEvent.sample(orderId, sellerId, amount, "standard");
    }

    private long balance(String sellerId) throws SQLException {
        return count("SELECT COALESCE(SUM(amount), 0) FROM balances WHERE seller_id = '" + sellerId + "'");
    }

    private long count(String sql) throws SQLException {
        try (Connection c = DriverManager.getConnection(url, "sa", "");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @SafeVarargs
    private static <T> List<Future<T>> runTogether(ExecutorService pool, Callable<T>... tasks) {
        CountDownLatch start = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>();
        for (Callable<T> task : tasks) {
            futures.add(pool.submit(() -> {
                start.await();
                return task.call();
            }));
        }
        start.countDown();
        return futures;
    }
}