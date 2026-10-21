package dev.learning.kafka.own.processing;

import dev.learning.kafka.common.OrderEvent;
import dev.learning.kafka.own.processing.exceptions.BalanceStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class BalanceStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BalanceStore.class);

    private static final String INSERT_ORDERS_SQL = "INSERT INTO ORDERS(order_id, seller_id, amount) VALUES (?, ?, ?);";
    private static final String UPDATE_BALANCE_SQL = "UPDATE BALANCES SET amount = amount + ? WHERE seller_id = ?;";
    private static final String INSERT_BALANCE_SQL = "INSERT INTO BALANCES(seller_id, amount) VALUES (?, ?);";
    private static final String ERR_UQ_CODE = "23505";

    private static final String DB_USER = "sa";
    private static final String DB_PASS = "";

    private final String dbUrl;

    private Connection connection;

    public BalanceStore(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public void open() throws Exception {
        if (connection != null) {
            log.info("Соединение с БД уже создано");
            return;
        }
        connection = DriverManager.getConnection(dbUrl, DB_USER, DB_PASS);
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("""
                                CREATE TABLE IF NOT EXISTS balances (
                                  seller_id VARCHAR(64) PRIMARY KEY, 
                                  amount BIGINT NOT NULL                               
                               );
                              """);
            statement.execute("""
                                CREATE TABLE IF NOT EXISTS orders (
                                  order_id VARCHAR PRIMARY KEY,
                                  seller_id VARCHAR(64) NOT NULL,
                                  amount BIGINT NOT NULL,
                                  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP   
                                );
                              """);
            connection.commit();

        }
    }

    public BalanceOperationResult apply(OrderEvent orderEvent) throws BalanceStoreException {
        if (connection == null) {
            throw new BalanceStoreException("Отсутствует открытое соединение с БД");
        }
        int ordersInserted = 0;
        int balanceUpdated = 0;
        try {
            connection.setAutoCommit(false);
            BalanceOperationResult result = BalanceOperationResult.APPLIED;
            try (PreparedStatement ps = connection.prepareStatement(INSERT_ORDERS_SQL)) {
                ps.setString(1, orderEvent.orderId());
                ps.setString(2, orderEvent.sellerId());
                ps.setLong(3, orderEvent.amount());
                ordersInserted = ps.executeUpdate();
            } catch (SQLException sqle1) {
                if (ERR_UQ_CODE.equals(sqle1.getSQLState())) {
                    log.debug("Обработка и вставка заказа(order_id={}) уже выполнена", orderEvent.orderId());
                    result = BalanceOperationResult.DUPLICATE;
                } else {
                    throw new BalanceStoreException(sqle1);
                }
            }

            if (ordersInserted == 1) {
                try (PreparedStatement ps = connection.prepareStatement(UPDATE_BALANCE_SQL)) {
                    ps.setLong(1, orderEvent.amount());
                    ps.setString(2, orderEvent.sellerId());
                    balanceUpdated = ps.executeUpdate();
                }

                if (balanceUpdated == 0) {
                    try (PreparedStatement ps = connection.prepareStatement(INSERT_BALANCE_SQL)) {
                        ps.setString(1, orderEvent.sellerId());
                        ps.setLong(2, orderEvent.amount());
                        ps.executeUpdate();
                    } catch (SQLException sqle2) {
                        if (ERR_UQ_CODE.equals(sqle2.getSQLState())) {
                            log.info(
                                    "Счет с балансом для продавца(seller_id={}) был открыт. Будет выполнена повторная попытка.",
                                    orderEvent.sellerId());
                            try (PreparedStatement ps = connection.prepareStatement(UPDATE_BALANCE_SQL)) {
                                ps.setLong(1, orderEvent.amount());
                                ps.setString(2, orderEvent.sellerId());
                                ps.executeUpdate();
                            }
                        } else {
                            throw new BalanceStoreException(sqle2);
                        }
                    }
                }
            }

            connection.commit();
            return result;
        } catch (Exception exc) {
            log.error("Ошибка BalanceStore. Выполняем rollback()", exc);
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException rollExc) {
                log.error("Откат завершился с ошибкой", rollExc);
                throw new BalanceStoreException(rollExc);
            }
            throw new BalanceStoreException(exc);
        }
    }

    private void closeQuietly(AutoCloseable... closeable) {
        Arrays.stream(closeable)
              .forEach(ac -> {
                  if (ac != null) {
                      try {
                          ac.close();
                      } catch (Exception exc) {
                          log.error("Ошибка во время закрытия ресурса", exc);
                      }
                  }
              });

    }

    @Override
    public void close() throws Exception {
        closeQuietly(connection);
    }

    public enum BalanceOperationResult {
        APPLIED, DUPLICATE
    }
}
