package dev.learning.kafka.common;

import java.util.Properties;

/**
 * Единая точка получения параметров подключения для всех лаб.
 *
 * Переопределяется переменными окружения или -D системными свойствами:
 *   BOOTSTRAP_SERVERS  (по умолчанию localhost:9092 — одиночный брокер)
 *   SCHEMA_REGISTRY_URL
 *
 * Для кластера из compose.cluster.yml используйте:
 *   BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092
 */
public final class Env {

    private Env() {
    }

    public static String bootstrapServers() {
        return get("BOOTSTRAP_SERVERS", "bootstrap.servers", "localhost:9092");
    }

    public static String schemaRegistryUrl() {
        return get("SCHEMA_REGISTRY_URL", "schema.registry.url", "http://localhost:8081");
    }

    public static String get(String envName, String propName, String defaultValue) {
        String fromProp = System.getProperty(propName);
        if (fromProp != null && !fromProp.isBlank()) {
            return fromProp;
        }
        String fromEnv = System.getenv(envName);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        return defaultValue;
    }

    /** Базовые свойства, поверх которых лаба доливает свои. */
    public static Properties base() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers());
        props.put("client.id", "kafka-labs-" + ProcessHandle.current().pid());
        return props;
    }
}
