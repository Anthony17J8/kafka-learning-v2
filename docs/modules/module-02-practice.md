# Модуль 2. Практика: партиции, сегменты, retention, compaction

**Предварительно:** [теория модуля 2](module-02-theory.md)
**Время:** 8–10 часов
**Окружение:** одиночный брокер (`compose.single.yml`), кроме лабы 2.12
**Результат:** `notes/02-log.md` с таблицей экспериментов, разделом «почему так устроено» и четырьмя сценариями поломки

---

Все команды — напрямую: `docker compose` из `docker/`, `mvn` из `labs/`, утилиты Kafka через `docker exec`. Справочник — [docs/commands.md](../commands.md).

Правило модуля 0 остаётся в силе: **сначала предсказание, потом команда.** В этом модуле особенно много мест, где интуиция ошибается — время жизни данных, момент закрытия сегмента, результат компакции. Каждое несовпадение — строка в таблице экспериментов.

## Содержание

| # | Лаба | Время | Сценарий поломки |
|---|---|---|---|
| 2.0 | [Подготовка окружения](#20-подготовка-окружения) | 20 мин | |
| 2.1 | [CLI топиков и конфигурации](#21-cli-топиков-и-конфигурации) | 40 мин | |
| 2.2 | [Ключ определяет партицию](#22-ключ-определяет-партицию) | 50 мин | |
| 2.3 | [Перекос партиций](#23-перекос-партиций) | 30 мин | №1 |
| 2.4 | [Увеличение партиций ломает порядок](#24-увеличение-партиций-ломает-порядок) | 30 мин | |
| 2.5 | [Сегменты вживую](#25-сегменты-вживую) | 50 мин | |
| 2.6 | [Разбор файлов лога](#26-разбор-файлов-лога) | 60 мин | |
| 2.7 | [Индекс, который не строится](#27-индекс-который-не-строится) | 30 мин | |
| 2.8 | [Retention](#28-retention) | 50 мин | |
| 2.9 | [Потребитель за началом лога](#29-потребитель-за-началом-лога) | 40 мин | №2 |
| 2.10 | [Компакция и надгробия](#210-компакция-и-надгробия) | 60 мин | |
| 2.11 | [Компакция, которая не запускается](#211-компакция-которая-не-запускается) | 40 мин | №3 |
| 2.12 | [Надгробие ломает потребителя](#212-надгробие-ломает-потребителя) | 40 мин | №4 |
| 2.13 | [Расчёт числа партиций](#213-расчёт-числа-партиций) | 30 мин | |

---

## 2.0 Подготовка окружения

### Ускорить фоновые процессы брокера

По умолчанию брокер проверяет retention раз в 5 минут, а компактор после пустого прохода спит 15 секунд. Для наблюдений это слишком медленно. Добавьте в `docker/compose.single.yml` в `environment` сервиса `kafka`:

```yaml
      # Только для учебного окружения: ускоряем фоновые проверки,
      # чтобы retention и компакцию можно было наблюдать в реальном времени.
      KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 10000
      KAFKA_LOG_CLEANER_BACKOFF_MS: 5000
      KAFKA_LOG_SEGMENT_DELETE_DELAY_MS: 30000
```

**Предскажите:** какие из трёх параметров можно было бы поменять без перезапуска брокера, через `kafka-configs.sh --alter --entity-type brokers`? Проверите в лабе 2.1.

```bash
cd docker
docker compose -f compose.single.yml down -v
docker compose -f compose.single.yml up -d
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka)" = healthy ]; do sleep 3; done
```

### Переменные на сессию

Каждый новый терминал начинайте с них:

```bash
K="docker exec -i -e KAFKA_OPTS= kafka"
BS="localhost:9094"
BIN="/opt/kafka/bin"
DATA="/var/lib/kafka/data"
```

Проверка:

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --list
```

### jshell с клиентом Kafka

В нескольких лабах понадобится вызвать код клиента напрямую, без сборки приложения. Получите classpath модуля `common` через Maven:

```bash
cd labs
mvn -q -pl common -am install -DskipTests
mvn -q -pl common dependency:build-classpath -Dmdep.outputFile=/tmp/kafka-cp.txt
cat /tmp/kafka-cp.txt | tr ':' '\n' | grep kafka-clients
```

Запуск:

```bash
jshell --class-path "$(cat /tmp/kafka-cp.txt)"
```

Внутри проверьте, что клиент доступен:

```java
import org.apache.kafka.common.utils.Utils;
Utils.murmur2("hello".getBytes())
```

---

## 2.1 CLI топиков и конфигурации

**Цель:** уверенно владеть `kafka-topics.sh` и `kafka-configs.sh`, понимать, откуда берётся действующее значение настройки.

### Создание и описание

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.cli.v1 --partitions 3 --replication-factor 1

$K $BIN/kafka-topics.sh --bootstrap-server $BS --describe --topic m2.cli.v1
```

Разберите вывод: `PartitionCount`, `ReplicationFactor`, `Configs`, строки по партициям с `Leader`, `Replicas`, `Isr`.

### Откуда берётся значение

```bash
$K $BIN/kafka-configs.sh --bootstrap-server $BS \
  --describe --entity-type topics --entity-name m2.cli.v1 --all
```

Флаг `--all` покажет **все** настройки с указанием источника. Найдите `retention.ms`, `segment.bytes`, `cleanup.policy` и посмотрите на `source=`:

- `DEFAULT_CONFIG` — зашитое в Kafka значение по умолчанию;
- `STATIC_BROKER_CONFIG` — из конфигурации брокера (у нас это переменные окружения compose);
- `DYNAMIC_TOPIC_CONFIG` — задано явно для топика.

Измените настройку и посмотрите снова:

```bash
$K $BIN/kafka-configs.sh --bootstrap-server $BS \
  --alter --entity-type topics --entity-name m2.cli.v1 \
  --add-config retention.ms=3600000

$K $BIN/kafka-configs.sh --bootstrap-server $BS \
  --describe --entity-type topics --entity-name m2.cli.v1
```

Без `--all` выводятся только явно заданные настройки. Удалите переопределение и убедитесь, что значение вернулось к источнику по умолчанию:

```bash
$K $BIN/kafka-configs.sh --bootstrap-server $BS \
  --alter --entity-type topics --entity-name m2.cli.v1 \
  --delete-config retention.ms
```

### Ограничения версии

Попробуйте задать значение ниже допустимого:

```bash
$K $BIN/kafka-configs.sh --bootstrap-server $BS \
  --alter --entity-type topics --entity-name m2.cli.v1 \
  --add-config segment.bytes=100000
```

Прочитайте ошибку целиком. Это ограничение из KIP-1030: в Kafka 4.x минимальный размер сегмента 1 МБ.

### Динамические настройки брокера

Вернитесь к вопросу из 2.0:

```bash
$K $BIN/kafka-configs.sh --bootstrap-server $BS \
  --alter --entity-type brokers --entity-name 1 \
  --add-config log.retention.check.interval.ms=5000
```

Принял ли брокер это изменение? Затем проверьте `log.cleaner.backoff.ms`. Какие настройки брокера меняются на лету, а какие требуют перезапуска, указано в документации в колонке «Update Mode» — сверьтесь.

**Занести в конспект:** три уровня источника настройки и какой из них побеждает.

---

## 2.2 Ключ определяет партицию

**Цель:** убедиться, что партиция вычисляется детерминированно, и вычислить её самостоятельно.

### Запись с ключами

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.keys.v1 --partitions 6 --replication-factor 1

printf '%s\n' \
  'user-1:a' 'user-2:b' 'user-3:c' 'user-1:d' 'user-2:e' 'user-1:f' \
| $K $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic m2.keys.v1 \
    --property parse.key=true --property key.separator=:
```

Прочитайте с указанием партиции и оффсета:

```bash
$K $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic m2.keys.v1 \
  --from-beginning --timeout-ms 5000 \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Обратите внимание на порядок вывода: он **не совпадает** с порядком отправки. Консьюмер читает партиции независимо. Но внутри одного ключа порядок `a → d → f` для `user-1` сохранён.

### Вычислить партицию самостоятельно

**Предскажите сначала**, а потом проверьте. В jshell:

```java
import org.apache.kafka.common.utils.Utils;

int partitionFor(String key, int n) {
    return Utils.toPositive(Utils.murmur2(key.getBytes())) % n;
}

partitionFor("user-1", 6)
partitionFor("user-2", 6)
partitionFor("user-3", 6)
```

Совпало с тем, что показал консьюмер? Если да — вы только что воспроизвели алгоритм партиционирования, который одинаков для клиентов на любом языке.

Проверьте, что будет при другом числе партиций:

```java
for (int n : new int[]{3, 6, 12}) System.out.println(n + " -> " + partitionFor("user-1", n));
```

Сохраните результат: он понадобится в лабе 2.4.

### Запись без ключа

```bash
$K $BIN/kafka-producer-perf-test.sh --topic m2.keys.v1 \
  --num-records 60000 --record-size 100 --throughput -1 \
  --producer-props bootstrap.servers=$BS

$K $BIN/kafka-get-offsets.sh --bootstrap-server $BS --topic m2.keys.v1 --time -1
```

**Предскажите:** будет ли распределение по шести партициям равномерным? Посмотрите на конечные оффсеты. Затем повторите с `--num-records 30` и сравните.

На малом объёме вы увидите, что записи без ключа ложатся в одну-две партиции — это sticky-поведение из теории: партиция меняется, когда заполнен батч. На большом объёме распределение выравнивается.

---

## 2.3 Перекос партиций

**Сценарий поломки №1.** Ключ с низкой кардинальностью и неравномерным распределением.

### Воспроизведение

Сгенерируйте 10 000 заказов, где ключ — страна, и 90% трафика из одной страны:

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.skew.v1 --partitions 6 --replication-factor 1

for i in $(seq 1 10000); do
  r=$((RANDOM % 100))
  if   [ $r -lt 90 ]; then c=RU
  elif [ $r -lt 95 ]; then c=KZ
  elif [ $r -lt 98 ]; then c=BY
  else                     c=AM
  fi
  echo "$c:order-$i"
done | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic m2.skew.v1 \
         --property parse.key=true --property key.separator=:
```

**Предскажите** распределение по партициям. Сколько партиций останется пустыми?

```bash
$K $BIN/kafka-get-offsets.sh --bootstrap-server $BS --topic m2.skew.v1 --time -1
```

Проверьте размер на диске:

```bash
$K sh -c "du -sh $DATA/m2.skew.v1-*"
```

### Диагностика

Запишите в конспект, как вы бы заметили эту проблему в продакшене, не имея доступа к коду продюсера. Подсказка: какие две команды из этой лабы дают ответ, и какую метрику вы бы вывели на дашборд?

### Лечение

Подумайте письменно, до того как смотреть варианты:

- что изменится, если увеличить число партиций до 60;
- какой составной ключ сохранит порядок внутри страны, но распределит нагрузку;
- чем придётся пожертвовать при каждом варианте.

---

## 2.4 Увеличение партиций ломает порядок

**Цель:** увидеть своими глазами, почему менять число партиций у топика с порядком по ключу нельзя.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.resize.v1 --partitions 3 --replication-factor 1

printf '%s\n' 'user-1:v1' 'user-1:v2' 'user-1:v3' \
| $K $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic m2.resize.v1 \
    --property parse.key=true --property key.separator=:
```

Увеличьте число партиций:

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --alter --topic m2.resize.v1 --partitions 12
```

**Предскажите** по результату jshell из лабы 2.2, в какую партицию попадут следующие записи `user-1`.

```bash
printf '%s\n' 'user-1:v4' 'user-1:v5' \
| $K $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic m2.resize.v1 \
    --property parse.key=true --property key.separator=:

$K $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic m2.resize.v1 \
  --from-beginning --timeout-ms 5000 \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Записи одного ключа теперь в двух партициях. Потребитель, читающий две партиции параллельно, может обработать `v4` раньше `v3`.

Попробуйте уменьшить обратно:

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --alter --topic m2.resize.v1 --partitions 3
```

Запишите сообщение об ошибке и три причины из теории, почему это невозможно.

---

## 2.5 Сегменты вживую

**Цель:** увидеть, когда именно закрывается сегмент.

### Roll по размеру

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.segments.v1 --partitions 1 --replication-factor 1 \
  --config segment.bytes=1048576
```

Во втором терминале наблюдайте за каталогом партиции:

```bash
watch -n 1 "docker exec kafka ls -la /var/lib/kafka/data/m2.segments.v1-0/"
```

В первом — запишите около 5 МБ:

```bash
$K $BIN/kafka-producer-perf-test.sh --topic m2.segments.v1 \
  --num-records 5000 --record-size 1000 --throughput 500 \
  --producer-props bootstrap.servers=$BS
```

**Предскажите:** сколько сегментов появится и какие будут у них имена. Совпало?

Обратите внимание на размер `.index` у активного сегмента: он большой и не меняется. Это предвыделенное место (`segment.index.bytes`, 10 МБ). У закрытых сегментов индекс обрезан до реального размера.

### Roll по времени

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.roll-time.v1 --partitions 1 --replication-factor 1 \
  --config segment.ms=15000
```

Запишите одну запись и подождите минуту, глядя в `watch`:

```bash
echo 'k:first' | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS \
  --topic m2.roll-time.v1 --property parse.key=true --property key.separator=:

watch -n 1 "docker exec kafka ls -la /var/lib/kafka/data/m2.roll-time.v1-0/"
```

**Предскажите:** появится ли второй сегмент через 15 секунд?

Не появится. Теперь запишите ещё одну запись:

```bash
echo 'k:second' | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS \
  --topic m2.roll-time.v1 --property parse.key=true --property key.separator=:
```

Сегмент закрылся в момент записи. **Проверка `segment.ms` происходит при добавлении данных, а не по таймеру.** Простаивающая партиция не закрывает активный сегмент никогда.

Это главный вывод лабы. Запишите, как он связан с retention и с компакцией: что происходит с данными в топике, куда перестали писать?

---

## 2.6 Разбор файлов лога

**Цель:** прочитать сегмент как структуру данных, найти в нём поля батча из теории.

### Сегмент данных

```bash
$K $BIN/kafka-dump-log.sh \
  --files $DATA/m2.keys.v1-0/00000000000000000000.log | head -20
```

Выведутся строки по батчам. Найдите в них `baseOffset`, `lastOffset`, `count`, `CreateTime`, `size`, `compresscodec`, `producerId`, `isTransactional`.

С содержимым записей:

```bash
$K $BIN/kafka-dump-log.sh --print-data-log \
  --files $DATA/m2.keys.v1-0/00000000000000000000.log | head -30
```

Сопоставьте с тем, что вы писали в лабе 2.2.

**Вопросы:**

1. Какое значение `producerId` у батчей из консольного продюсера и из `kafka-producer-perf-test`? Если где-то оно не равно `-1` — значит продюсер работал в идемпотентном режиме, хотя вы его не включали. Выясните по документации producer configs, какое значение `enable.idempotence` действует по умолчанию и при каких условиях оно отключается.
2. Сколько записей в одном батче у данных из `kafka-console-producer`, а сколько у данных из `kafka-producer-perf-test`? Почему разница?

### Сжатие

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.compressed.v1 --partitions 1 --replication-factor 1

$K $BIN/kafka-producer-perf-test.sh --topic m2.compressed.v1 \
  --num-records 20000 --record-size 500 --throughput -1 \
  --producer-props bootstrap.servers=$BS compression.type=zstd linger.ms=50

$K $BIN/kafka-dump-log.sh \
  --files $DATA/m2.compressed.v1-0/00000000000000000000.log | head -5

$K sh -c "du -sh $DATA/m2.compressed.v1-0 $DATA/m2.segments.v1-0"
```

Найдите `compresscodec: zstd`. Сравните размер на диске с несжатым топиком из 2.5 при сопоставимом объёме. `kafka-producer-perf-test` генерирует случайные данные, которые сжимаются плохо, — учтите это в выводе.

### Индексы

```bash
$K $BIN/kafka-dump-log.sh --files $DATA/m2.segments.v1-0/00000000000000000000.index
$K $BIN/kafka-dump-log.sh --files $DATA/m2.segments.v1-0/00000000000000000000.timeindex
```

Посчитайте: какой шаг по позиции между соседними записями индекса? Как он соотносится с `index.interval.bytes` (4096) и размером записи (1000 байт плюс накладные расходы)?

Найдите в индексе запись, ближайшую снизу к оффсету 500, и объясните письменно три шага поиска записи с этим оффсетом.

---

## 2.7 Индекс, который не строится

**Цель:** проверить ответ на вопрос «что будет, если интервал индекса больше сегмента».

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.no-index.v1 --partitions 1 --replication-factor 1 \
  --config segment.bytes=1048576 \
  --config index.interval.bytes=2097152

$K $BIN/kafka-producer-perf-test.sh --topic m2.no-index.v1 \
  --num-records 5000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=$BS
```

**Предскажите:** есть ли файлы `.index` у закрытых сегментов, и что в них?

```bash
$K ls -la $DATA/m2.no-index.v1-0/
$K $BIN/kafka-dump-log.sh --files $DATA/m2.no-index.v1-0/00000000000000000000.index
```

Проверьте, что чтение с произвольного оффсета работает:

```bash
$K $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic m2.no-index.v1 \
  --partition 0 --offset 1234 --max-messages 1 | head -c 80; echo
```

Запишите в конспект: почему пустой индекс не нарушает корректность и почему не ухудшает гарантию времени поиска.

---

## 2.8 Retention

**Цель:** наблюдать удаление сегментов и движение log start offset.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.retention.v1 --partitions 1 --replication-factor 1 \
  --config retention.ms=60000 \
  --config segment.ms=15000
```

### Наблюдение

Три терминала. В первом — непрерывная запись раз в секунду:

```bash
i=0; while true; do
  i=$((i+1)); echo "k:msg-$i"; sleep 1
done | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS \
         --topic m2.retention.v1 --property parse.key=true --property key.separator=:
```

Во втором — файлы:

```bash
watch -n 2 "docker exec kafka ls -la /var/lib/kafka/data/m2.retention.v1-0/ | grep -E 'log|deleted'"
```

В третьем — границы лога:

```bash
watch -n 2 "docker exec -e KAFKA_OPTS= kafka sh -c '\
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9094 --topic m2.retention.v1 --time -2; \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9094 --topic m2.retention.v1 --time -1'"
```

`--time -2` показывает log start offset, `--time -1` — log end offset.

**Предскажите** до запуска:

- через сколько секунд после записи первого сообщения оно исчезнет;
- каким будет минимальное и максимальное время жизни записи при этих настройках.

Наблюдайте:

- сегменты закрываются примерно каждые 15 секунд;
- сегмент получает суффикс `.deleted`;
- через 30 секунд (`log.segment.delete.delay.ms`) файл исчезает;
- log start offset прыгает скачками, а не растёт плавно — ровно на размер удалённого сегмента.

Замерьте реальное время жизни первой записи и объясните, из каких слагаемых оно сложилось: `retention.ms`, возраст сегмента, интервал проверки, задержка удаления.

### retention.bytes на партицию

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.retention-bytes.v1 --partitions 3 --replication-factor 1 \
  --config retention.bytes=2097152 \
  --config segment.bytes=1048576

$K $BIN/kafka-producer-perf-test.sh --topic m2.retention-bytes.v1 \
  --num-records 30000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=$BS

sleep 20
$K sh -c "du -sh $DATA/m2.retention-bytes.v1-*"
```

**Предскажите** суммарный объём топика до проверки. Сколько сегментов останется в каждой партиции и почему не ровно два?

---

## 2.9 Потребитель за началом лога

**Сценарий поломки №2.** Потребитель отстал больше, чем на retention.

### Воспроизведение

Используйте топик из 2.8, пока в него идёт запись. Прочитайте немного группой и остановитесь:

```bash
$K $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic m2.retention.v1 \
  --group m2.slow-reader --from-beginning --max-messages 5

$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m2.slow-reader
```

Запомните `CURRENT-OFFSET` группы. Подождите две минуты, пока retention удалит сегменты, и проверьте, что log start offset ушёл дальше закоммиченного оффсета:

```bash
$K $BIN/kafka-get-offsets.sh --bootstrap-server $BS --topic m2.retention.v1 --time -2
```

Теперь запустите группу снова, запретив автоматический сброс позиции:

```bash
$K $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic m2.retention.v1 \
  --group m2.slow-reader --max-messages 5 \
  --consumer-property auto.offset.reset=none
```

**Предскажите** результат. Запишите полный текст исключения.

### Что делает клиент по умолчанию

Повторите с `auto.offset.reset=earliest`, а затем с новой группой и `latest`. Для каждого варианта запишите:

- с какого оффсета начал читать потребитель;
- сколько данных было потеряно для этой группы;
- заметил бы это сервис в продакшене без специального мониторинга.

Главный вывод: при значении по умолчанию потеря данных из-за отставания **происходит молча**. Запишите, какую метрику вы бы поставили на алерт, чтобы узнавать об этом до того, как retention догонит потребителя.

---

## 2.10 Компакция и надгробия

**Цель:** увидеть свёртку по ключу, дыры в оффсетах и жизненный цикл надгробия.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS \
  --create --topic m2.compact.v1 --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config delete.retention.ms=30000
```

### Несколько версий ключа

```bash
printf '%s\n' \
  'user-1:balance=100' 'user-2:balance=50' 'user-1:balance=150' \
  'user-3:balance=10'  'user-1:balance=120' 'user-2:balance=70' \
| $K $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic m2.compact.v1 \
    --property parse.key=true --property key.separator=:
```

Сразу прочитайте с начала:

```bash
$K $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic m2.compact.v1 \
  --from-beginning --timeout-ms 5000 \
  --property print.key=true --property print.offset=true
```

Все шесть записей на месте. **Предскажите:** что нужно, чтобы компакция сработала? Вспомните лабу 2.5.

Подождите 15 секунд и запишите ещё одну запись, чтобы закрыть сегмент:

```bash
sleep 15
echo 'user-4:balance=1' | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS \
  --topic m2.compact.v1 --property parse.key=true --property key.separator=:
```

Следите за работой компактора в логах брокера:

```bash
cd docker
docker compose -f compose.single.yml logs -f kafka | grep -i cleaner
```

Через 10–20 секунд прочитайте снова. Проверьте:

- для `user-1` осталась одна запись со значением `balance=120`;
- оффсеты идут с дырами: оффсеты удалённых версий пропали;
- `user-4` ещё не тронут, он в активном сегменте.

Посмотрите, как изменились файлы:

```bash
$K ls -la $DATA/m2.compact.v1-0/
$K cat $DATA/cleaner-offset-checkpoint
```

В `cleaner-offset-checkpoint` найдите строку для `m2.compact.v1` — это `firstDirtyOffset`, граница между скомпактированным хвостом и грязной головой.

### Надгробие

Консольный продюсер передаёт `null` как значение через `null.marker`:

```bash
echo 'user-2:NULL' | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS \
  --topic m2.compact.v1 --property parse.key=true --property key.separator=: \
  --property null.marker=NULL
```

Закройте сегмент ещё одной записью через 15 секунд и дождитесь компакции. Прочитайте с начала с `print.value=true`. Надгробие для `user-2` должно быть видно как `null`.

**Предскажите:** когда надгробие исчезнет само? Проверяйте раз в 15 секунд, дописывая запись для закрытия сегмента. Замерьте время от компакции до исчезновения и сравните с `delete.retention.ms`.

**Вопрос для конспекта:** почему потребитель, который начал читать топик с нуля **после** исчезновения надгробия, никогда не узнает, что `user-2` существовал? Это проблема или норма?

---

## 2.11 Компакция, которая не запускается

**Сценарий поломки №3.** Грязных данных недостаточно для порога.

Поднимите порог до значения по умолчанию на топике из 2.10:

```bash
$K $BIN/kafka-configs.sh --bootstrap-server $BS \
  --alter --entity-type topics --entity-name m2.compact.v1 \
  --add-config min.cleanable.dirty.ratio=0.5
```

Запишите небольшую порцию обновлений для существующих ключей и закройте сегмент:

```bash
printf '%s\n' 'user-1:balance=200' 'user-3:balance=20' \
| $K $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic m2.compact.v1 \
    --property parse.key=true --property key.separator=:
sleep 15
echo 'user-5:balance=5' | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS \
  --topic m2.compact.v1 --property parse.key=true --property key.separator=:
```

Подождите минуту. Прочитайте с начала: старые версии `user-1` и `user-3` на месте.

### Посчитайте отношение

```bash
$K ls -la $DATA/m2.compact.v1-0/
$K cat $DATA/cleaner-offset-checkpoint
```

По размерам `.log` файлов и значению `firstDirtyOffset` оцените clean и dirty в байтах и вычислите отношение. Меньше 0.5? Тогда поведение корректно.

**Предскажите**, сколько ещё данных нужно записать, чтобы компакция стартовала, и проверьте предсказание.

### Форсировать через max.compaction.lag.ms

```bash
$K $BIN/kafka-configs.sh --bootstrap-server $BS \
  --alter --entity-type topics --entity-name m2.compact.v1 \
  --add-config max.compaction.lag.ms=20000
```

Закройте сегмент и подождите. Компакция должна пройти, несмотря на низкое отношение. Запишите, какую бизнес-задачу решает этот параметр.

---

## 2.12 Надгробие ломает потребителя

**Сценарий поломки №4.** Код потребителя не готов к `null` в значении.

Запустите jshell с classpath из 2.0 и напишите минимального потребителя. Каркас:

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

var props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "m2.naive-reader");
props.put("auto.offset.reset", "earliest");

var consumer = new KafkaConsumer<String, String>(
    props, new StringDeserializer(), new StringDeserializer());
consumer.subscribe(List.of("m2.compact.v1"));
```

Обратите внимание на адрес: jshell работает на хосте, поэтому `localhost:9092`, а не внутренний `9094`.

Допишите сами цикл `poll`, который для каждой записи разбирает значение вида `balance=120` и печатает число. Не добавляйте проверку на `null` — это часть задания.

**Предскажите**, на какой записи и с каким исключением упадёт код.

После падения:

1. Запишите стектрейс.
2. Ответьте: что произойдёт с этим потребителем в продакшене, если он работает в цикле с перезапуском? Почему он будет падать на одной и той же записи бесконечно?
3. Исправьте обработку и объясните, что правильно делать с надгробием: пропустить, удалить запись из своего хранилища, что-то ещё.

Это первое знакомство с «poison pill» — записью, которая систематически ломает потребителя. Подробно вернёмся к ним в модулях 4 и 10.

---

## 2.13 Расчёт числа партиций

**Письменное задание.** Для гипотетической системы посчитайте число партиций и объём хранения. Входные данные:

- 50 000 событий в секунду в пике, средний размер 800 байт;
- требуется хранить 3 дня;
- RF=3;
- потребитель обрабатывает одну запись за 2 мс в одном потоке;
- порядок нужен в пределах `userId`, пользователей около миллиона;
- ожидаемый рост трафика — в три раза за два года.

Посчитайте:

1. Минимальное число партиций по параллелизму потребителей.
2. Объём хранения на кластер с учётом RF.
3. Итоговое число партиций с учётом роста — и обоснование, почему именно столько, а не больше.
4. Подходящие `segment.bytes` и `segment.ms` для такого retention.

Пропускную способность одной партиции на запись вы пока не измеряли — это модуль 8. Укажите, какое допущение сделали, и отметьте, что его нужно проверить.

---

## Гейт закрытия модуля

**Самопроверка без материалов.** Письменные ответы на десять контрольных вопросов из теории, не открывая ни документацию, ни эти файлы.

**Артефакт с обоснованием.** `notes/02-log.md` закоммичен и содержит:

- таблицу экспериментов, где в каждой строке есть колонка «почему»;
- раздел «почему так устроено» своими словами: сегменты вместо записей, разреженный индекс, асинхронная компакция;
- расчёт из лабы 2.13.

**Сценарии поломки.** Все четыре задокументированы: воспроизведение, симптом, способ диагностики, вывод.

- [ ] №1 Перекос партиций — лаба 2.3
- [ ] №2 Потребитель за log start offset — лаба 2.9
- [ ] №3 Компакция не запускается — лаба 2.11
- [ ] №4 Надгробие ломает потребителя — лаба 2.12

---

## Типичные проблемы

| Симптом | Причина | Что делать |
|---|---|---|
| `InvalidConfigurationException` на `segment.bytes` | в Kafka 4.x минимум 1 МБ | использовать `segment.ms` или ≥ 1048576 |
| Сегмент не закрывается по `segment.ms` | проверка идёт при записи, а не по таймеру | дописать запись после истечения срока |
| Retention не удаляет данные | активный сегмент не закрыт либо проверка ещё не прошла | закрыть сегмент, проверить интервал из 2.0 |
| Компакция не срабатывает | нет закрытых сегментов либо отношение ниже порога | закрыть сегмент, проверить `min.cleanable.dirty.ratio` |
| `kafka-dump-log.sh`: файл не найден | неверное имя сегмента | сначала `ls` каталога партиции |
| Консьюмер ничего не выводит и висит | нет `--timeout-ms` или `--max-messages` | добавить один из параметров |
| jshell не видит классы Kafka | classpath не построен или пустой | повторить `dependency:build-classpath` из 2.0 |
| `BindException: Address in use` от CLI | в контейнере задан `KAFKA_OPTS` с javaagent | `-e KAFKA_OPTS=` в `docker exec` |

---

## Уборка

```bash
cd docker
docker compose -f compose.single.yml down -v
```

Изменения в `compose.single.yml` из лабы 2.0 можно оставить: они пригодятся в модулях 3–5.
