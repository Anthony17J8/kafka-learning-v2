# Модуль 0. Практика: запуск и диагностика Kafka

**Предварительно:** прочитать [теорию модуля 0](module-00-theory.md)
**Время:** 3–5 часов
**Результат:** заполненный `notes/00-setup.md` и понимание, почему брокер не подключается

---

Практика построена по принципу «сломать и починить». Половина заданий — это намеренные ошибки конфигурации: увидеть реальное сообщение об ошибке и связать его с причиной ценнее, чем прочитать про эту ошибку в документации.

Правило на весь модуль: **сначала предскажите результат, потом выполните команду.** Если предсказание не совпало — это самое интересное место, разберитесь в нём и запишите в конспект.

Все команды даются напрямую: `docker compose`, `docker exec`, `mvn`, утилиты Kafka.
Справочник с пояснениями — [docs/commands.md](../commands.md), держите открытым рядом.
Команды `docker compose` выполняются из каталога `docker/`, команды `mvn` — из `labs/`.

## Содержание

| # | Лаба | Время | Обязательна |
|---|---|---|---|
| 0.1 | [Проверка окружения](#01-проверка-окружения) | 10 мин | да |
| 0.2 | [Ручной запуск из архива](#02-ручной-запуск-из-архива) | 40 мин | да |
| 0.3 | [Тот же брокер в Docker](#03-тот-же-брокер-в-docker) | 20 мин | да |
| 0.4 | [Ломаем advertised.listeners](#04-ломаем-advertisedlisteners) | 40 мин | да |
| 0.5 | [Ломаем cluster.id](#05-ломаем-clusterid) | 20 мин | да |
| 0.6 | [Кластер из трёх нод](#06-кластер-из-трёх-нод) | 30 мин | да |
| 0.7 | [Отказ контроллера](#07-отказ-контроллера) | 30 мин | да |
| 0.8 | [Раздельные роли](#08-раздельные-роли) | 40 мин | нет |
| 0.9 | [JVM против native](#09-jvm-против-native) | 20 мин | нет |
| 0.10 | [Конспект](#010-конспект) | 30 мин | да |

---

## 0.1 Проверка окружения

**Цель:** убедиться, что дальше не будете отлаживать не то.

```bash
cd kafka-learning/docker
cp .env.example .env

docker --version
docker compose version
java -version      # нужен 21+
mvn -v
```

Отдельно проверьте память. Кластеру из трёх брокеров нужно около 6 ГБ, иначе Docker начнёт убивать контейнеры по OOM, а вы будете искать причину в конфигурации Kafka.

```bash
docker info | grep -i memory
```

На macOS и Windows лимит задаётся в настройках Docker Desktop, по умолчанию он часто равен 2 ГБ. Поднимите минимум до 8 ГБ.

**Занести в конспект:** версии Docker, JDK, объём выделенной Docker памяти. Через месяц, когда что-то перестанет работать, вы будете рады, что записали.

---

## 0.2 Ручной запуск из архива

**Цель:** увидеть все три шага запуска руками, без обёрток. Docker их прячет, и от этого возникает ощущение, что Kafka «магически стартует».

### Шаг 1. Скачать дистрибутив

Актуальную версию и ссылку возьмите на https://kafka.apache.org/downloads — выбирайте бинарную сборку для Scala 2.13.

```bash
cd /tmp
KAFKA_VER=4.3.1        # сверьтесь со страницей загрузок
curl -O "https://downloads.apache.org/kafka/${KAFKA_VER}/kafka_2.13-${KAFKA_VER}.tgz"
tar -xzf "kafka_2.13-${KAFKA_VER}.tgz"
cd "kafka_2.13-${KAFKA_VER}"
```

Осмотритесь:

```bash
ls bin/ | head -40
ls config/
```

В `bin/` около сорока скриптов — это весь инструментарий, с которым вы будете работать все следующие модули. В `config/` лежат заготовки конфигураций.

**Вопрос:** какие файлы в `config/` относятся к KRaft и чем они отличаются? Откройте `server.properties` и найдите `process.roles`, `node.id`, `controller.quorum.voters`, `listeners`, `log.dirs`.

### Шаг 2. Попробовать запустить без форматирования

Сделайте это намеренно — нужно увидеть ошибку:

```bash
bin/kafka-server-start.sh config/server.properties
```

Брокер откажется стартовать и сообщит, что каталог не отформатирован. Прочитайте сообщение целиком.

### Шаг 3. Отформатировать и запустить

```bash
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
echo "cluster id: $KAFKA_CLUSTER_ID"

bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Посмотрите, что появилось на диске. Путь берите из `log.dirs` в конфиге (по умолчанию что-то вроде `/tmp/kraft-combined-logs`):

```bash
LOG_DIR=$(grep '^log.dirs' config/server.properties | cut -d= -f2)
ls -la "$LOG_DIR"
cat "$LOG_DIR/meta.properties"
```

В `meta.properties` вы увидите `cluster.id`, `node.id` и версию формата. Это тот самый файл, который защищает от присоединения к чужому кластеру.

Теперь запуск:

```bash
bin/kafka-server-start.sh config/server.properties
```

Читайте лог старта внимательно. Найдите в нём:

- строку про `KafkaRaftServer` и переход в роль контроллера;
- сообщение о завершении восстановления логов;
- регистрацию брокера;
- переход из fenced в unfenced;
- финальное `started`.

Это ровно та последовательность из шести шагов, которая описана в теории. Сопоставьте.

### Шаг 4. Проверить, что живой

Во втором терминале, из того же каталога:

```bash
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -5
```

Первая строка вывода покажет, до какого адреса клиент реально достучался. Запомните этот вывод — в лабе 0.4 он изменится.

Посмотрите на слушающие сокеты:

```bash
ss -lntp | grep java     # Linux
lsof -iTCP -sTCP:LISTEN -n -P | grep java   # macOS
```

**Вопрос:** сколько сокетов открыто и почему именно столько? Сверьтесь со строкой `listeners` в конфиге.

### Шаг 5. Записать и прочитать

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic manual-test --partitions 1 --replication-factor 1

bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic manual-test
> привет
> из архива
^D

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic manual-test --from-beginning
```

Посмотрите, что легло на диск:

```bash
ls -la "$LOG_DIR/manual-test-0/"
```

Найдите `.log`, `.index`, `.timeindex`. Детально разбирать будем в модуле 2, сейчас достаточно увидеть, что данные — это обычные файлы.

Остановите брокер: `Ctrl-C` в первом терминале, либо `bin/kafka-server-stop.sh`.

---

## 0.3 Тот же брокер в Docker

**Цель:** увидеть, что образ делает ровно те же шаги, просто автоматически.

```bash
cd kafka-learning/docker
docker compose -f compose.single.yml up -d
docker compose -f compose.single.yml ps
docker compose -f compose.single.yml logs -f kafka
```

Сравните лог старта с тем, что видели в лабе 0.2. Он должен быть практически идентичен, плюс строки про форматирование в самом начале.

```bash
docker exec -it kafka bash
# внутри контейнера:
cat /var/lib/kafka/data/meta.properties
ls /opt/kafka/bin | head -20
ss -lntp 2>/dev/null || netstat -lntp
exit
```

**Вопрос:** сколько слушающих сокетов внутри контейнера и почему их больше, чем было в лабе 0.2?

Проверьте оба пути подключения:

```bash
# изнутри контейнера, по внутреннему адресу
docker exec -it kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka:9094 | head -3

# изнутри контейнера, по внешнему адресу
docker exec -it kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 | head -3

# с хоста, дистрибутивом из лабы 0.2
cd /tmp/kafka_2.13-4.3.1
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -3
```

Все три сработают, но обратите внимание, какой адрес брокер называет в каждом случае. Это и есть механизм выбора listener'а по точке входа.

---

## 0.4 Ломаем advertised.listeners

Центральная лаба модуля. Три поломки, каждая даёт свой характерный симптом.

### Сначала: чем проверять

Ключевой момент, без которого лаба не работает. **Место запуска диагностической
утилиты определяет, какой listener вы проверяете.**

Любая команда через `docker exec` выполняется **внутри контейнера** и подключается к
внутреннему адресу. Такой клиент приходит на listener `INTERNAL`, получает
`INTERNAL`-адреса и работает даже при полностью сломанном внешнем listener'е. По той же
причине продолжают работать healthcheck и kafka-ui: они тоже живут внутри compose-сети.

Поэтому для этой лабы нужен клиент **на хосте**. Выберите удобный вариант:

```bash
# вариант 1: дистрибутив из лабы 0.2, распакованный локально
cd /tmp/kafka_2.13-4.3.1
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# вариант 2: лабораторный продюсер через Maven
cd labs
BOOTSTRAP_SERVERS=localhost:9092 mvn -q -pl lab03-producer -am install -DskipTests
BOOTSTRAP_SERVERS=localhost:9092 mvn -pl lab03-producer exec:java \
  -Dexec.mainClass=dev.learning.kafka.producer.ProducerModes -Dexec.args="async 100"

# вариант 3: контейнер в сети хоста (только Linux)
docker run --rm --network host apache/kafka:4.3.1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Вариант 1 самый быстрый и не требует сборки. На macOS и Windows `--network host`
ведёт себя иначе, чем на Linux, поэтому третий вариант там не подойдёт.

Половина фраз «а у меня всё работает» при отладке listeners объясняется именно тем,
что человек проверял изнутри контейнера. Запишите это в конспект отдельной строкой.

### Поломка 1: внешний адрес не резолвится у клиента

Откройте `docker/compose.single.yml` и замените:

```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://kafka:9092
```

Было `EXTERNAL://localhost:9092`, стало `EXTERNAL://kafka:9092`.

```bash
cd docker
docker compose -f compose.single.yml down -v
docker compose -f compose.single.yml up -d

# сработает: клиент внутри контейнера
docker exec -i kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list

# упадёт: клиент на хосте
cd /tmp/kafka_2.13-4.3.1
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Предскажите до запуска:** что произойдёт с каждой из двух команд? Брокер стартует?
Какая будет ошибка?

Что вы увидите: брокер стартует нормально, healthcheck проходит, CLI внутри контейнера
работает, а клиент с хоста повисает и падает по таймауту. Сопоставьте это с разделом
выше — если объяснение расходится с вашим предсказанием, разберитесь до конца.

В выводе клиента найдите, как `node -1` (bootstrap-соединение) сменяется попыткой достучаться до узла с идентификатором `1`. Первый шаг проходит, второй — нет. Это подпись именно этой ошибки.

**Запишите в конспект дословную формулировку ошибки.** Вы встретите её ещё много раз.

### Поломка 2: порт в advertised не совпадает с проброшенным

Верните `localhost`, но смените порт:

```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:19092
```

Проброс в `ports` остаётся `9092:9092`.

```bash
cd docker && docker compose -f compose.single.yml down -v && docker compose -f compose.single.yml up -d
cd /tmp/kafka_2.13-4.3.1 && bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Симптом другой: вместо таймаута будет `Connection refused`. Разница в том, что имя резолвится, но на этом порту хоста никто не слушает.

**Вопрос для конспекта:** почему одна ошибка даёт таймаут, а другая — отказ в соединении? Что это говорит вам о том, на каком этапе искать проблему?

### Поломка 3: listener объявлен, но не забинден

```yaml
KAFKA_LISTENERS: INTERNAL://:19092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092
```

`EXTERNAL` остался в advertised, но исчез из listeners.

```bash
cd docker
docker compose -f compose.single.yml down -v
docker compose -f compose.single.yml up -d
docker compose -f compose.single.yml logs kafka | tail -30
```

Здесь брокер вообще не стартует. Найдите в логе точное сообщение — оно прямо говорит, чего не хватает.

### Починка

Верните исходную конфигурацию (`git checkout docker/compose.single.yml`, если файл под git) и убедитесь, что всё работает:

```bash
cd docker
docker compose -f compose.single.yml down -v
docker compose -f compose.single.yml up -d

cd /tmp/kafka_2.13-4.3.1
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Проверяйте именно клиентом с хоста: команда через `docker exec` была бы зелёной
и при сломанной конфигурации.

**Итог лабы:** таблица в конспекте из трёх строк — что сломали, какой симптом, как отличить от соседних случаев.

---

## 0.5 Ломаем cluster.id

**Цель:** увидеть `InconsistentClusterIdException` и понять, от чего защищает форматирование.

### Часть 1: старт без CLUSTER_ID

Уберите переменную из `compose.single.yml`:

```yaml
# CLUSTER_ID: ${CLUSTER_ID:-5L6g3nShT-eMCtK--X86sw}
```

```bash
cd docker
docker compose -f compose.single.yml down -v    # обязательно с -v: удаляем том
docker compose -f compose.single.yml up -d
docker compose -f compose.single.yml logs kafka | tail -30
```

Посмотрите, что произойдёт: образ сгенерирует идентификатор сам либо откажется стартовать, в зависимости от версии. Зафиксируйте фактическое поведение.

### Часть 2: несовпадение идентификаторов

Более интересный сценарий. Поднимите брокер штатно, затем подмените `cluster.id` в `meta.properties`:

```bash
cd docker
docker compose -f compose.single.yml down -v && docker compose -f compose.single.yml up -d

docker exec kafka sh -c \
  "sed -i 's/^cluster.id=.*/cluster.id=WRONGIdWRONGIdWRONGIdw/' /var/lib/kafka/data/meta.properties"

docker restart kafka
docker logs kafka 2>&1 | tail -30
```

Брокер упадёт с `InconsistentClusterIdException` или похожей ошибкой о несовпадении идентификатора.

**Вопрос:** что было бы, если бы такой проверки не существовало? Опишите сценарий в конспекте — это ровно та ситуация, которая случается при неаккуратном копировании конфигов между окружениями.

Восстановление:

```bash
cd docker && docker compose -f compose.single.yml down -v && docker compose -f compose.single.yml up -d
```

---

## 0.6 Кластер из трёх нод

```bash
cd docker
docker compose -f compose.single.yml down -v
docker compose -f compose.cluster.yml up -d

# дождаться готовности всех трёх нод
time (for n in 1 2 3; do
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "kafka-$n")" = healthy ]; do sleep 3; done
done)

docker compose -f compose.cluster.yml ps
```

Засеките, сколько занял сбор кластера, и запишите. Обратите внимание, что
`schema-registry` и `kafka-ui` стартуют только после того, как все брокеры
станут healthy — это задано через `depends_on: condition: service_healthy`.

### Разобраться, кто есть кто

```bash
docker exec -it -e KAFKA_OPTS= kafka-1 \
  /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9094 describe --status
```

Разберите вывод по полям:

- `LeaderId` — кто сейчас active controller;
- `LeaderEpoch` — сколько раз менялся лидер кворума;
- `HighWatermark` — до какого оффсета лог метаданных зафиксирован большинством;
- `CurrentVoters` — состав кворума.

```bash
docker exec -it -e KAFKA_OPTS= kafka-1 \
  /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9094 describe --replication
```

Здесь видно отставание каждого голосующего (`LagOffset`) и время последнего контакта. У здорового кластера отставание нулевое или близко к нему.

**Вопрос:** в выводе `--replication` есть строки со статусом observer. Кто это и почему они не голосуют?

### Создать топики и посмотреть распределение

Создайте учебные топики — блок для копирования целиком есть в
[docs/commands.md](../commands.md#создание-учебного-набора-топиков). Минимум для этой лабы:

```bash
docker exec -i -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9094 \
  --create --topic labs.orders.created.v1 --partitions 6 --replication-factor 3

docker exec -i -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9094 --describe --topic labs.orders.created.v1
```

Разберите вывод: для каждой партиции есть `Leader`, `Replicas` и `Isr`.

**Вопросы:**

1. Совпадает ли `Leader` с первым элементом `Replicas`? Что означает первый элемент?
2. Все ли партиции имеют лидером один и тот же брокер? Как распределены лидеры?
3. Мы задали `KAFKA_BROKER_RACK` для каждой ноды. Видно ли влияние на порядок реплик?

Сравните с диагностикой через Admin API:

```bash
cd labs
export BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092
mvn -q -pl lab07-admin -am install -DskipTests
mvn -pl lab07-admin exec:java -Dexec.mainClass=dev.learning.kafka.admin.ClusterDoctor
```

---

## 0.7 Отказ контроллера

**Цель:** увидеть перевыборы лидера кворума и разделение плоскостей управления и данных.

### Подготовка

В первом терминале запустите непрерывную запись:

```bash
cd labs
export BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092
mvn -q -pl lab03-producer -am install -DskipTests
mvn -pl lab03-producer exec:java \
  -Dexec.mainClass=dev.learning.kafka.producer.ProducerModes \
  -Dexec.args="async 5000000"
```

Первый раз запустите с малым объёмом (`async 1000`), чтобы убедиться, что путь
до кластера рабочий, и только потом давайте нагрузку.

Во втором — следите за кворумом:

```bash
watch -n 2 'docker exec -e KAFKA_OPTS= kafka-2 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9094 describe --status | grep -E "LeaderId|LeaderEpoch"'
```

Если `watch` нет (macOS), поставьте через brew либо крутите в цикле:

```bash
while true; do
  docker exec -e KAFKA_OPTS= kafka-2 /opt/kafka/bin/kafka-metadata-quorum.sh \
    --bootstrap-server localhost:9094 describe --status | grep -E "LeaderId|LeaderEpoch"
  sleep 2
done
```

### Убить active controller

Определите, какая нода сейчас лидер кворума, и убейте именно её:

```bash
docker kill kafka-1     # подставьте номер лидера кворума
```

**Наблюдайте:**

- за сколько секунд сменился `LeaderId`;
- на сколько вырос `LeaderEpoch`;
- что произошло с продюсером в первом терминале — были ошибки, была ли пауза;
- что показывают `--under-replicated-partitions` и `describe --status`.

Ожидаемое: перевыборы занимают единицы секунд, продюсер переживает это с короткой паузой и ретраями, потому что данные обслуживают брокеры, а не контроллер.

### Вернуть ноду

```bash
docker start kafka-1
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka-1)" = healthy ]; do sleep 3; done

docker exec -it -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9094 describe --replication
```

Убедитесь, что нода вернулась в кворум и догнала лог метаданных: её `LagOffset`
должен схлопнуться до нуля.

### Убить большинство

Теперь то, ради чего лаба:

```bash
docker stop kafka-2 kafka-3
```

Осталась одна нода из трёх — кворум потерян.

**Проверьте по очереди:**

```bash
K="docker exec -i -e KAFKA_OPTS= kafka-1"; BS="localhost:9094"

$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list          # чтение метаданных
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic labs.orders.created.v1

# запись в существующие партиции
cd labs && mvn -pl lab03-producer exec:java \
  -Dexec.mainClass=dev.learning.kafka.producer.ProducerModes -Dexec.args="async 1000"

# операция плоскости управления — должна провалиться
$K /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS \
  --create --topic new-topic --partitions 1 --replication-factor 1
```

Последняя команда — создание топика — это операция плоскости управления, она должна провалиться. Запись в существующие партиции ведёт себя иначе.

**Главный вопрос модуля:** какие операции продолжают работать без кворума контроллеров, а какие нет? Ответ запишите в конспект своими словами.

Восстановление:

```bash
docker start kafka-2 kafka-3
for n in 2 3; do
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "kafka-$n")" = healthy ]; do sleep 3; done
done
```

---

## 0.8 Раздельные роли `[опционально]`

**Цель:** собрать конфигурацию, приближенную к продовой, где контроллеры вынесены отдельно.

Создайте `docker/compose.roles.yml`: три ноды с `process.roles=controller` и две с `process.roles=broker`. Ключевые отличия от `compose.cluster.yml`:

- у контроллеров нет клиентских listener'ов вообще, только `CONTROLLER`;
- у контроллеров нет `advertised.listeners`;
- у брокеров `process.roles=broker` и они указывают на кворум через `controller.quorum.voters`;
- идентификаторы нод не пересекаются: контроллеры 1–3, брокеры 4–5.

Это самостоятельное задание — возьмите `compose.cluster.yml` за основу и разберитесь по документации. Проверка успеха:

```bash
docker compose -f docker/compose.roles.yml up -d
docker exec broker-4 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:19092 describe --status
```

В `CurrentVoters` должны быть только идентификаторы 1–3, брокеры 4–5 должны фигурировать как observers.

**Вопрос:** что произойдёт, если убить оба брокера, но оставить все контроллеры? А наоборот?

---

## 0.9 JVM против native `[опционально]`

**Цель:** понять, зачем существует `apache/kafka-native`.

Замерьте время от запуска контейнера до готовности принимать запросы для обоих образов:

```bash
# JVM
time (docker run -d --name t-jvm -e KAFKA_NODE_ID=1 ... apache/kafka:4.3.1 && \
      until docker exec t-jvm /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 0.2; done)
```

Проще всего: сделайте копию `compose.single.yml`, замените образ на `apache/kafka-native:4.3.1` и сравните время до healthy, а также потребление памяти:

```bash
docker stats --no-stream
```

**Вопрос:** где такая разница имеет практическое значение, а где нет?

---

## 0.10 Конспект

Заполните `notes/00-setup.md` по шаблону. Обязательные разделы:

**Таблица экспериментов** — минимум восемь строк по выполненным лабам: что делал, какие настройки, что получилось, какой вывод.

**Что сломалось и почему** — самая ценная часть. Все три поломки listener'ов с дословными сообщениями об ошибках, плюс `cluster.id`.

**Ответы на контрольные вопросы** из теории. Письменно, своими словами, без подглядывания:

1. Что такое `cluster.id`, где хранится, от чего защищает?
2. Чем `listeners` отличается от `advertised.listeners`? Приведите конфигурацию, при которой брокер стартует, но клиент с хоста работать не сможет.
3. Опишите по шагам путь клиента от создания продюсера до первой записи. Когда `bootstrap.servers` перестаёт использоваться?
4. Почему совмещённый режим `broker,controller` не рекомендуется в проде? Три причины.
5. Что произойдёт при потере большинства контроллеров? Что продолжит работать, что нет?
6. Почему Kafka рекомендуют небольшой heap и много свободной памяти?
7. Что означает состояние fenced и почему `docker ps: Up` не означает готовность?

---

## Чек-лист завершения модуля

- [ ] Брокер запущен вручную из архива, все три шага понятны
- [ ] Прочитан и разобран лог старта, найдены все шесть стадий
- [ ] Воспроизведены три поломки listener'ов, симптомы записаны
- [ ] Воспроизведён `InconsistentClusterIdException`
- [ ] Кластер из трёх нод поднят, active controller определён
- [ ] Проведены перевыборы контроллера под нагрузкой
- [ ] Проверено поведение при потере кворума
- [ ] `notes/00-setup.md` заполнен, на все семь вопросов есть письменные ответы

Если хотя бы один пункт не отмечен — не переходите к модулю 2. Он полностью опирается на понимание того, что кластер работает и почему.

---

## Типичные проблемы

| Симптом | Причина | Что делать |
|---|---|---|
| Контейнер перезапускается по кругу | не хватает памяти Docker | поднять лимит до 8 ГБ |
| Кластер не доходит до healthy | брокер падает на javaagent из `compose.jmx.yml` | до модуля 10 поднимать без этого override |
| `не удалось скачать JMX exporter` | версии нет в Maven Central либо нет сети | задать `JMX_AGENT_VERSION`, скачать вручную либо не подключать `compose.jmx.yml` |
| Клиент виснет и падает по таймауту | неверный `advertised.listeners` | лаба 0.4, поломка 1 |
| Сломали advertised, а `docker exec ... --list` работает | CLI выполняется внутри контейнера, по `INTERNAL` | проверять клиентом с хоста, см. лабу 0.4 |
| `Connection refused` с хоста | порт в advertised не совпадает с проброшенным | лаба 0.4, поломка 2 |
| `InconsistentClusterIdException` | старый том с чужим `cluster.id` | `docker compose ... down -v` |
| Брокер стартует, но топики не создаются | брокер ещё fenced, не догнал метаданные | подождать, проверить `describe --status` |
| Порт 9092 занят | локально запущена Kafka из лабы 0.2 | остановить её либо поменять проброс |
| `BindException: Address in use` от CLI-утилиты | утилита унаследовала `KAFKA_OPTS` с javaagent | добавить `-e KAFKA_OPTS=` в `docker exec` |

---

## Дальше

Модуль 1 — короткий обзорный блок про event streaming и лог как абстракцию. Если понимаете, чем лог отличается от очереди, можно сразу к модулю 2: топики, партиции, сегменты, retention и compaction.
