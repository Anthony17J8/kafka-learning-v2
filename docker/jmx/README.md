# JMX Exporter

Положите сюда `jmx_prometheus_javaagent.jar` перед запуском кластера:

```bash
./kl jmx-agent
```

Файл не коммитится в репозиторий (см. `.gitignore`).
Если агент отсутствует, брокер не стартует — уберите `KAFKA_OPTS` из `compose.cluster.yml`
или скачайте jar.
