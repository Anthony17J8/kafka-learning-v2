# Бенчмарки

Результаты нагрузочных тестов из модулей 3 и 8. Каждый прогон описывается по одному шаблону,
иначе цифры невозможно сравнивать между собой.

## Шаблон прогона

**Окружение**
- версия Kafka, число брокеров, RF, min.insync.replicas
- железо: CPU, RAM, тип диска
- где запущен клиент относительно брокеров

**Параметры**
- размер сообщения, число сообщений, число потоков
- acks, linger.ms, batch.size, compression.type
- число партиций топика

**Результаты**

| Прогон | acks | linger.ms | batch.size | compression | throughput msg/s | throughput MB/s | p50 ms | p99 ms | размер на диске |
|---|---|---|---|---|---|---|---|---|---|
| | | | | | | | | | |

**Выводы**
Что оказалось узким местом и по каким метрикам это видно.

## Прогоны CLI-инструментами

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic labs.orders.created.v1 \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:29092 acks=all compression.type=lz4 linger.ms=10

docker exec -it kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:29092 \
  --topic labs.orders.created.v1 \
  --messages 1000000
```

## Правила

- Прогревайте: первые 10-20 секунд отбрасывайте.
- Меняйте один параметр за раз.
- Записывайте отрицательные результаты тоже. «Zstd не дал выигрыша на наших сообщениях» — это результат.
