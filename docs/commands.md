# Шпаргалка: прямые команды

Всё выполняется вручную, без обёрток. Копируйте, меняйте, ломайте.

Два соглашения на весь документ:

- команды `docker compose` запускаются **из каталога `docker/`** — там лежит `.env`, и относительные пути в compose считаются оттуда;
- `mvn` запускается **из каталога `labs/`**.

---

## Обязательный префикс для docker exec

```bash
docker exec -it -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-topics.sh ...
```

`-e KAFKA_OPTS=` нужен всегда, когда в контейнере брокера прописан javaagent (модуль 10). `kafka-run-class.sh` передаёт `KAFKA_OPTS` любому java-процессу, включая CLI-утилиты, и агент падает на уже занятом порту, унося утилиту с собой. Симптом — `BindException: Address in use` в ответ на безобидную команду.

Пока вы не подключили `compose.jmx.yml`, префикс избыточен, но привычка полезная.

---

## Окружение

### Одиночный брокер (модули 0–5)

```bash
cd docker
cp .env.example .env          # один раз

docker compose -f compose.single.yml up -d
docker compose -f compose.single.yml ps
docker compose -f compose.single.yml logs -f kafka

# остановить с удалением данных
docker compose -f compose.single.yml down -v
```

Bootstrap для клиентов с хоста: `localhost:9092`. UI: http://localhost:8080

### Кластер из трёх нод (модули 6+)

```bash
cd docker
docker compose -f compose.cluster.yml up -d
docker compose -f compose.cluster.yml ps
docker compose -f compose.cluster.yml logs -f kafka-1 kafka-2 kafka-3
docker compose -f compose.cluster.yml down -v
```

Bootstrap с хоста: `localhost:19092,localhost:29092,localhost:39092`
Внутри compose-сети: `kafka-1:9094,kafka-2:9094,kafka-3:9094`
Schema Registry: http://localhost:8081

### Кластер с JMX-метриками (модуль 10)

```bash
cd docker
./fetch-jmx-agent.sh
docker compose -f compose.cluster.yml -f compose.jmx.yml up -d
curl -s localhost:19404/metrics | head
```

### Мониторинг (модуль 10)

```bash
cd docker
docker compose -f compose.monitoring.yml up -d
```

Prometheus: http://localhost:9090, Grafana: http://localhost:3000

### Полная остановка всего

```bash
cd docker
for f in compose.monitoring.yml compose.cluster.yml compose.single.yml; do
  docker compose -f "$f" down -v 2>/dev/null || true
done
docker volume ls | grep kafka-learning
```

---

## Работа с брокером

### Интерактивная сессия

```bash
docker exec -it -e KAFKA_OPTS= kafka-1 bash
# внутри: все утилиты в /opt/kafka/bin, bootstrap localhost:9094
```

Для одиночного брокера контейнер называется `kafka`.

### Переменные для сокращения

Удобно объявить один раз на сессию:

```bash
# кластер
K="docker exec -i -e KAFKA_OPTS= kafka-1"
BS="localhost:9094"

# одиночный
K="docker exec -i -e KAFKA_OPTS= kafka"
BS="localhost:9094"

$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list
```

---

## Топики

```bash
# создать
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS \
  --create --topic labs.orders.created.v1 --partitions 6 --replication-factor 3

# список
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list

# подробности: лидеры, реплики, ISR
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS \
  --describe --topic labs.orders.created.v1

# проблемные партиции
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --at-min-isr-partitions

# изменить число партиций (уменьшить нельзя)
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS \
  --alter --topic labs.orders.created.v1 --partitions 12

# удалить
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --delete --topic labs.orders.created.v1
```

### Создание учебного набора топиков

Блок для копирования целиком. Для одиночного брокера поменяйте `RF=3` на `RF=1`.

```bash
K="docker exec -i -e KAFKA_OPTS= kafka-1"; BS="localhost:9094"; RF=3

create() {
  local name=$1 parts=$2; shift 2
  $K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS \
    --create --if-not-exists --topic "$name" --partitions "$parts" --replication-factor $RF "$@"
}

create labs.orders.created.v1 6
create labs.orders.input.v1 6
create labs.orders.output.v1 6
create labs.orders.retry-5s.v1 3
create labs.orders.retry-1m.v1 3
create labs.orders.dlq.v1 3 --config retention.ms=1209600000

# compacted-топик для модуля 2: параметры занижены, чтобы cleaner
# отработал за секунды, а не за часы
create labs.customers.state.v1 3 \
  --config cleanup.policy=compact --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 --config delete.retention.ms=10000

# короткий retention для модуля 2
create labs.shortlived.v1 3 --config retention.ms=60000 --config segment.ms=20000
```

---

## Конфигурация

```bash
# посмотреть конфиг топика
$K /opt/kafka/bin/kafka-configs.sh --bootstrap-server $BS \
  --describe --entity-type topics --entity-name labs.orders.created.v1

# изменить
$K /opt/kafka/bin/kafka-configs.sh --bootstrap-server $BS \
  --alter --entity-type topics --entity-name labs.orders.created.v1 \
  --add-config retention.ms=3600000

# конфиг брокера
$K /opt/kafka/bin/kafka-configs.sh --bootstrap-server $BS \
  --describe --entity-type brokers --entity-name 1
```

---

## Запись и чтение из консоли

```bash
# продюсер
docker exec -it -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9094 --topic labs.orders.created.v1 \
  --property parse.key=true --property key.separator=:

# консьюмер с начала
docker exec -it -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9094 --topic labs.orders.created.v1 --from-beginning \
  --property print.key=true --property print.partition=true --property print.timestamp=true

# только одна партиция
... --partition 2 --offset earliest
```

---

## Consumer-группы

```bash
$K /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS --list

# lag и назначение партиций
$K /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS \
  --describe --group labs.orders.processor

# сбросить оффсеты (группа должна быть неактивна)
$K /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS \
  --group labs.orders.processor --topic labs.orders.created.v1 \
  --reset-offsets --to-earliest --execute
```

---

## Кластер и метаданные

```bash
# доступность брокера и версии протокола
$K /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server $BS | head -3

# состояние Raft-кворума
$K /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
$K /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication

# идентификатор кластера
$K /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server $BS

# feature levels
$K /opt/kafka/bin/kafka-features.sh --bootstrap-server $BS describe

# распределение по каталогам логов
$K /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server $BS --describe
```

---

## Разбор сегментов лога (модуль 2)

```bash
docker exec -it -e KAFKA_OPTS= kafka-1 bash -c \
  'ls /var/lib/kafka/data/labs.orders.created.v1-0/'

docker exec -it -e KAFKA_OPTS= kafka-1 bash -c \
  '/opt/kafka/bin/kafka-dump-log.sh \
     --files /var/lib/kafka/data/labs.orders.created.v1-0/00000000000000000000.log \
     --print-data-log | head -40'
```

---

## Нагрузочные тесты (модули 3, 8)

```bash
docker exec -it -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic labs.orders.created.v1 \
  --num-records 1000000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9094 \
    acks=all compression.type=lz4 linger.ms=10 batch.size=32768

docker exec -it -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9094 --topic labs.orders.created.v1 --messages 1000000
```

---

## Лабораторный код

```bash
cd labs
mvn clean install -DskipTests     # собрать всё
mvn test                          # тесты
```

Запуск конкретного класса. Ключевые моменты: остаёмся в корне реактора (иначе не разрешится родительский pom), ограничиваем `exec:java` одним модулем через `-pl` (иначе плагин отработает на каждом проекте, включая родительский pom, и упадёт на нём).

```bash
cd labs

export BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092
# для одиночного брокера: export BOOTSTRAP_SERVERS=localhost:9092

mvn -pl lab03-producer -am install -DskipTests -q

mvn -pl lab03-producer exec:java \
  -Dexec.mainClass=dev.learning.kafka.producer.ProducerModes \
  -Dexec.args="async 100000"
```

Остальные классы:

```bash
mvn -pl lab04-consumer exec:java -Dexec.mainClass=dev.learning.kafka.consumer.ManualCommitConsumer
mvn -pl lab04-consumer exec:java -Dexec.mainClass=dev.learning.kafka.consumer.LagReporter -Dexec.args="labs.orders.processor"
mvn -pl lab06-transactions exec:java -Dexec.mainClass=dev.learning.kafka.tx.TransactionalPipeline -Dexec.args="tx-worker-1"
mvn -pl lab07-admin exec:java -Dexec.mainClass=dev.learning.kafka.admin.ClusterDoctor
mvn -pl lab12-streams exec:java -Dexec.mainClass=dev.learning.kafka.streams.RevenueApp
```

Параметры экспериментов передаются переменными окружения:

```bash
ACKS=1 COMPRESSION=zstd LINGER_MS=50 mvn -pl lab03-producer exec:java \
  -Dexec.mainClass=dev.learning.kafka.producer.ProducerModes -Dexec.args="async 500000"

GROUP_PROTOCOL=classic PROCESSING_MS=5000 mvn -pl lab04-consumer exec:java \
  -Dexec.mainClass=dev.learning.kafka.consumer.ManualCommitConsumer
```

Полный вывод Maven при отладке: добавьте `-e -X` и уберите `-q`.

---

## Сценарии отказов (модули 6, 7, 10)

```bash
# жёстко убить брокер, без controlled shutdown
docker kill kafka-2

# корректно остановить
docker stop kafka-2

# поднять обратно и дождаться healthy
docker start kafka-2
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka-2)" = healthy ]; do sleep 3; done

# сетевая изоляция
docker network disconnect kafka-learning kafka-3
docker network connect kafka-learning kafka-3

# последовательный перезапуск всех нод
for n in 1 2 3; do
  docker restart "kafka-$n"
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "kafka-$n")" = healthy ]; do sleep 3; done
done

# заполнить диск балластом
docker exec kafka-1 sh -c 'dd if=/dev/zero of=/var/lib/kafka/data/ballast bs=1M count=100000'
docker exec kafka-1 rm -f /var/lib/kafka/data/ballast
```

---

## Диагностика контейнеров

```bash
docker ps -a --filter name=kafka --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
docker inspect -f '{{.State.ExitCode}} OOMKilled={{.State.OOMKilled}}' kafka-1
docker inspect --format='{{json .State.Health}}' kafka-1 | head -c 800
docker logs kafka-1 2>&1 | grep -E 'ERROR|FATAL|Exception' | head -20
docker stats --no-stream

# слушающие сокеты внутри контейнера
docker exec kafka-1 sh -c 'ss -lntp 2>/dev/null || netstat -lntp'

# резолвится ли сосед
docker exec kafka-1 getent hosts kafka-2
```
