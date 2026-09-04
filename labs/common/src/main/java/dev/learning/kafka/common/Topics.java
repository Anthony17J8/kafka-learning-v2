package dev.learning.kafka.common;

/**
 * Имена топиков для лаб.
 *
 * Соглашение об именовании (модуль 15): {@code <домен>.<сущность>.<тип-события>.v<версия>}.
 * Учебные топики намеренно живут в отдельном префиксе {@code labs.}, чтобы их
 * было легко отличить от проектных.
 */
public final class Topics {

    private Topics() {
    }

    /** Модуль 3: базовая запись, 6 партиций, RF по окружению. */
    public static final String ORDERS = "labs.orders.created.v1";

    /** Модуль 4: вход для конвейера потребления. */
    public static final String ORDERS_INPUT = "labs.orders.input.v1";

    /** Модуль 6: выход транзакционного read-process-write. */
    public static final String ORDERS_OUTPUT = "labs.orders.output.v1";

    /** Модуль 15: retry-цепочка и финальный DLQ. */
    public static final String RETRY_5S = "labs.orders.retry-5s.v1";
    public static final String RETRY_1M = "labs.orders.retry-1m.v1";
    public static final String DLQ = "labs.orders.dlq.v1";

    /** Модуль 12: агрегаты Kafka Streams. */
    public static final String REVENUE_BY_SELLER = "labs.analytics.revenue-by-seller.v1";
}
