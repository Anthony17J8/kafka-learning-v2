# Модуль 4. Практика: Consumer API и группы

**Предварительно:** [теория модуля 4](module-04-theory.md)
**Время:** 12–14 часов
**Окружение:** одиночный брокер (`compose.single.yml`)
**Результат:** `notes/04-consumer.md` и `benchmarks/04-consumer.md` с замерами, разделом «почему так устроено» и четырьмя сценариями поломки

---

Все команды — напрямую: `docker compose` из `docker/`, `mvn` из `labs/`, утилиты Kafka через `docker exec`.

Правило прежнее: **сначала предсказание, потом команда.** Каждое несовпадение — строка в таблице экспериментов.

Главный инструмент модуля — `ConsumerProbe`, потребитель, которому любую настройку можно передать аргументом. Он печатает два вида строк:

- **события** — сразу, с временем до миллисекунды: `ОТЗЫВ`, `НАЗНАЧЕНИЕ`, `ПОТЕРЯ` партиций (методы `ConsumerRebalanceListener`, раздел 9 теории), ошибки коммита, пауза, сбой;
- **строку состояния** — раз в секунду: сколько записей обработано за секунду, `e2e-макс` — наибольшее время от записи продюсером до обработки, `между-poll-макс` — наибольший интервал между вызовами `poll()`, `lag-макс` — метрика клиента `records-lag-max` (раздел 13), текущие партиции.

`e2e-макс` — главный индикатор недоступности: если партиция не читается во время ребалансировки, записи копятся, и после возобновления их время от записи до обработки подскакивает.

Записи для лаб пишет `ProducerProbe` из модуля 3. Значение каждой записи начинается с её номера, и `ConsumerProbe` с опцией `processed.file` записывает номера обработанных записей в файл. Дубли и пропуски проверяются так же, как в модуле 3: `sort`, `uniq`, `comm`.

## Содержание

| # | Лаба | Время | Сценарий поломки |
|---|---|---|---|
| 4.0 | [Подготовка](#40-подготовка) | 30 мин | |
| 4.1 | [Позиция и закоммиченный оффсет](#41-позиция-и-закоммиченный-оффсет) | 30 мин | |
| 4.2 | [Три семантики доставки](#42-три-семантики-доставки) | 60 мин | |
| 4.3 | [Масштабирование группы](#43-масштабирование-группы) | 40 мин | №4 |
| 4.4 | [auto.offset.reset](#44-autooffsetreset) | 50 мин | №3 |
| 4.5 | [Цикл ребалансировок](#45-цикл-ребалансировок) | 60 мин | №1 |
| 4.6 | [Сравнение протоколов групп](#46-сравнение-протоколов-групп) | 90 мин | |
| 4.7 | [Коммит при отзыве партиций](#47-коммит-при-отзыве-партиций) | 50 мин | №2 |
| 4.8 | [Статическое членство](#48-статическое-членство) | 40 мин | |
| 4.9 | [Переигровка с заданного момента](#49-переигровка-с-заданного-момента) | 30 мин | |
| 4.10 | [Лаг](#410-лаг) | 50 мин | |
| 4.11 | [pause и resume](#411-pause-и-resume) | 20 мин | |
| | [Гейт закрытия модуля](#гейт-закрытия-модуля) | | |

---

## 4.0 Подготовка

### Новые файлы и правки

- `labs/lab04-consumer/src/main/java/dev/learning/kafka/consumer/ConsumerProbe.java` — потребитель-зонд;
- в `labs/common/src/main/resources/logback.xml` после строки для `ProducerConfig`:

  ```xml
  <!-- То же для потребителя: действующие настройки при старте, в том числе group.protocol. -->
  <logger name="org.apache.kafka.clients.consumer.ConsumerConfig" level="INFO"/>
  ```

Код зонда целиком разбирать не нужно. Стоит прочитать три места: `ConsumerRebalanceListener` в `subscribe()` — что зонд делает при отзыве, назначении и потере партиций; блок коммита после цикла обработки — чем отличаются режимы `commit`; `crash.after` — процесс завершается через `Runtime.halt()`, без коммита и без `close()`, как при `kill -9`.

### Брокер, сборка, переменные

```bash
cd docker
docker compose -f compose.single.yml up -d
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka)" = healthy ]; do sleep 3; done

cd ../labs
mvn -q -pl lab03-producer,lab04-consumer -am install -DskipTests
```

Переменные — **в каждом терминале**, который используете в лабе:

```bash
K="docker exec -i -e KAFKA_OPTS= kafka"
BS="localhost:9094"
BIN="/opt/kafka/bin"
PPROBE="mvn -q -pl lab03-producer exec:java -Dexec.mainClass=dev.learning.kafka.producer.ProducerProbe"
CPROBE="mvn -q -pl lab04-consumer exec:java -Dexec.mainClass=dev.learning.kafka.consumer.ConsumerProbe"
```

Оба зонда запускаются из `labs/`.

### Фоновые процессы

В нескольких лабах одновременно работают продюсер и несколько потребителей. Их удобно запускать в фоне, с выводом в файл:

```bash
$CPROBE -Dexec.args="id=c1 ..." > /tmp/m4-c1.log 2>&1 & echo $! > /tmp/m4-c1.pid
tail -f /tmp/m4-c1.log
```

Остановить:

```bash
kill $(cat /tmp/m4-c1.pid)       # штатно: финальный коммит и close()
kill -9 $(cat /tmp/m4-c1.pid)    # мгновенная смерть процесса
```

Если `kill` по PID не останавливает процесс, найдите его через `pgrep -f 'id=c1'`.

### Имена членов группы

`Env` из заготовки задаёт `client.id` вида `kafka-labs-<pid>`, и в выводе `kafka-consumer-groups.sh` члены группы неотличимы от логов зонда. Во всех лабах, где запущено несколько потребителей, передавайте зонду `client.id` с тем же именем, что `id`: `id=c1 client.id=c1`. Тогда в колонке `CLIENT-ID` будут те же имена, что в логах.

### Проверка

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.probe.v1 --partitions 6 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.probe.v1 count=1000 keys=100 size=100"
$CPROBE -Dexec.args="id=c1 topic=m4.probe.v1 group.id=m4.check group.protocol=consumer auto.offset.reset=earliest idle.stop.ms=5000"
```

В начале вывода — блок `ConsumerConfig values:`. Найдите в нём `group.protocol`, `enable.auto.commit`, `auto.offset.reset`, `max.poll.records`, `max.poll.interval.ms` и сверьте со справочником настроек теории (раздел 16). Затем события `НАЗНАЧЕНИЕ`, строки состояния и итог: обработано должно быть 1000.

### Таблица экспериментов

Форма прежняя: `Что делал | Предсказание | Результат | Совпало? | Почему`. Ведите её в `notes/04-consumer.md` с первой лабы.

---

## 4.1 Позиция и закоммиченный оффсет

Цель — увидеть два числа из раздела 3 теории раздельно.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.pos.v1 --partitions 1 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.pos.v1 count=1000 size=100"
```

### Без коммита

```bash
$CPROBE -Dexec.args="id=c1 topic=m4.pos.v1 group.id=m4.pos group.protocol=consumer auto.offset.reset=earliest commit=none idle.stop.ms=5000"
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.pos
```

**Предскажите** до запуска: что покажет колонка `CURRENT-OFFSET` — закоммиченный оффсет группы? Сколько записей прочитает повторный запуск той же команды?

### С коммитом

```bash
$CPROBE -Dexec.args="id=c1 topic=m4.pos.v1 group.id=m4.pos group.protocol=consumer auto.offset.reset=earliest commit=sync idle.stop.ms=5000"
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.pos
```

**Предскажите**: какое значение будет в `CURRENT-OFFSET` — 999 или 1000? Сколько записей прочитает повторный запуск?

**Что зафиксировать:**

- `CURRENT-OFFSET`, `LOG-END-OFFSET` и `LAG` после каждого прогона;
- почему последняя обработанная запись имеет оффсет 999, а закоммичено 1000;
- что было бы при каждом перезапуске, если бы зонд коммитил оффсет обработанной записи, а не следующей;
- что показала утилита после прогона без коммита — таблицу с прочерком или только строку `has no active members`, — и чем это состояние отличается от закоммиченного 0: что в каждом случае определяет позицию при следующем запуске (раздел 4 теории).

---

## 4.2 Три семантики доставки

Цель — получить потери и повторы из раздела 5 теории в числах. Каждый вариант — отдельная группа; первый запуск обрывается сбоем, второй дочитывает остальное.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.sem.v1 --partitions 6 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.sem.v1 count=20000 keys=100 size=100"
```

### Прогон

Для каждого варианта из таблицы — два запуска с одним `group.id` и одним файлом:

```bash
# первый запуск: сбой после 5250 обработанных записей
$CPROBE -Dexec.args="id=c1 topic=m4.sem.v1 group.id=m4.sem-<ВАРИАНТ> group.protocol=consumer auto.offset.reset=earliest commit=<РЕЖИМ> process.ms=1 crash.after=5250 processed.file=/tmp/m4-sem-<ВАРИАНТ>.txt"

# второй запуск: дочитать остальное
$CPROBE -Dexec.args="id=c1 topic=m4.sem.v1 group.id=m4.sem-<ВАРИАНТ> group.protocol=consumer auto.offset.reset=earliest commit=<РЕЖИМ> idle.stop.ms=10000 processed.file=/tmp/m4-sem-<ВАРИАНТ>.txt"
```

| Вариант | `<ВАРИАНТ>` | `<РЕЖИМ>` | Что делает |
|---|---|---|---|
| at-most-once | `amo` | `before` | коммит сразу после `poll()`, до обработки пачки |
| at-least-once | `alo` | `sync` | коммит после обработки каждой пачки |
| автокоммит | `auto` | `auto` | коммит внутри `poll()` раз в 5 секунд |

`process.ms=1` замедляет обработку, чтобы записи до сбоя обрабатывались около 5 секунд — сопоставимо с интервалом автокоммита. Число 5250 выбрано не случайно: в выводах объясните, что изменилось бы при `crash.after=5000`.

**Второй запуск начинайте сразу после сбоя** — иначе задержку, о которой вопрос ниже, не увидеть. Засеките время от старта до первой строки с ненулевым `обработано`.

В строке состояния `всего` растёт скачками, равными числу записей, обработанных за секунду: при `process.ms=1` — около тысячи, без него — сколько успеет за секунду, вплоть до всего остатка сразу. С размером пачки `max.poll.records` эти скачки не связаны.

**Предскажите** для каждого варианта до запуска: сколько записей будет потеряно и сколько обработано дважды. Ответ дайте диапазоном и объясните, от чего зависит число. Подсказка: `max.poll.records` по умолчанию 500.

Второй запуск начнёт получать записи не сразу. **Предскажите**, через сколько секунд, и какая настройка это определяет. Учтите, что лаба идёт с `group.protocol=consumer` (раздел 8 теории: где в новом протоколе задаётся таймаут сессии). Подсказка: процесс, завершённый через `halt()`, не сообщил группе о выходе.

### Подсчёт

Для каждого файла:

```bash
F=/tmp/m4-sem-amo.txt
wc -l < $F                                        # обработано всего, с повторами
sort -u $F | wc -l                                # уникальных
echo $(( 20000 - $(sort -u $F | wc -l) ))         # потеряно
sort $F | uniq -d | wc -l                         # обработано дважды
```

### Что зафиксировать

| Вариант | Обработано всего | Уникальных | Потеряно | Дважды | Задержка второго запуска, с |
|---|---|---|---|---|---|
| at-most-once | | | | | |
| at-least-once | | | | | |
| автокоммит | | | | | |

- почему потери at-most-once и повторы at-least-once ограничены одной пачкой;
- от чего зависит число повторов при автокоммите и почему оно может быть больше, чем при at-least-once;
- какую семантику дал автокоммит здесь, где обработка идёт в потоке `poll()`, и какую дал бы при обработке в отдельном потоке (раздел 3 теории).

---

## 4.3 Масштабирование группы

**Сценарий поломки №4.** Потребителей в группе больше, чем партиций.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.scale.v1 --partitions 6 --replication-factor 1

$PPROBE -Dexec.args="topic=m4.scale.v1 count=0 rate=100 keys=100 size=100" > /tmp/m4-producer.log 2>&1 & echo $! > /tmp/m4-producer.pid
```

Запускайте потребителей по одному, с интервалом около 15 секунд, начиная с `c1`:

```bash
N=1
$CPROBE -Dexec.args="id=c$N client.id=c$N topic=m4.scale.v1 group.id=m4.scale group.protocol=consumer auto.offset.reset=earliest" > /tmp/m4-c$N.log 2>&1 & echo $! > /tmp/m4-c$N.pid
```

После каждого запуска:

```bash
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.scale --members --verbose
```

Сохраните в конспект один вывод целиком, вместе с заголовком. У групп нового протокола в нём есть колонки, которых нет у классических: эпоха и целевое назначение рядом с текущим. Целевое назначение вычисляет координатор, текущее — то, что член уже принял (раздел 8 теории). Если поймать вывод в момент вступления нового члена, они могут различаться.

**Предскажите** до запуска таблицу распределения: сколько партиций получит каждый потребитель при 1, 2, 3, 4, 6 и 7 членах группы.

Когда запущены все семь, найдите в `/tmp/m4-c*.log` потребителя с пустым списком партиций:

```bash
grep -h 'партиции=\[\]' /tmp/m4-c*.log | tail -3
```

Затем штатно остановите `c1` и посмотрите, кто получил его партиции:

```bash
kill $(cat /tmp/m4-c1.pid)
sleep 10
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.scale --members --verbose
```

Остановка всех:

```bash
for n in 1 2 3 4 5 6 7; do kill $(cat /tmp/m4-c$n.pid) 2>/dev/null; done
kill $(cat /tmp/m4-producer.pid)
```

### Что зафиксировать

| Членов в группе | Распределение партиций по членам |
|---|---|
| 1 | |
| 2 | |
| 3 | |
| 4 | |
| 6 | |
| 7 | |
| 7, после остановки `c1` | |

Для сценария поломки: какой член простаивал, что при этом показывала его строка состояния, как обнаружить простаивающих членов снаружи — по выводу `kafka-consumer-groups.sh`, — и как выбрать число партиций, чтобы этого не было (раздел 2 теории, модуль 2, раздел 9).

---

## 4.4 auto.offset.reset

**Сценарий поломки №3.** Тихий пропуск данных с `latest` — значением по умолчанию.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.reset.v1 --partitions 1 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.reset.v1 count=1000 size=100"
```

### Новая группа

Три новые группы, каждая со своим значением. Для `latest` параметр не указывается — проверяем значение по умолчанию.

```bash
$CPROBE -Dexec.args="id=c1 topic=m4.reset.v1 group.id=m4.reset-default group.protocol=consumer idle.stop.ms=10000"
$CPROBE -Dexec.args="id=c1 topic=m4.reset.v1 group.id=m4.reset-earliest group.protocol=consumer auto.offset.reset=earliest idle.stop.ms=10000"
$CPROBE -Dexec.args="id=c1 topic=m4.reset.v1 group.id=m4.reset-none group.protocol=consumer auto.offset.reset=none idle.stop.ms=10000"
```

**Предскажите** для каждой: сколько записей будет обработано, будет ли ошибка и какая.

### by_duration

Подождите две минуты после записи первых 1000, затем допишите ещё 500 и запустите группу с интервалом в одну минуту:

```bash
$PPROBE -Dexec.args="topic=m4.reset.v1 count=500 size=100"
$CPROBE -Dexec.args="id=c1 topic=m4.reset.v1 group.id=m4.reset-duration group.protocol=consumer auto.offset.reset=by_duration:PT1M idle.stop.ms=10000"
```

**Предскажите**, сколько записей будет обработано.

### Пропавшие оффсеты

Оффсеты пустой группы удаляются через `offsets.retention.minutes` — неделю ждать не будем, удалим вручную. Сначала пусть группа `m4.reset-earliest` дочитает 500 записей второй волны и закоммитит конец партиции. Затем допишите 300 записей, удалите оффсеты группы и запустите её снова, без `auto.offset.reset`:

```bash
$CPROBE -Dexec.args="id=c1 topic=m4.reset.v1 group.id=m4.reset-earliest group.protocol=consumer idle.stop.ms=10000"

$PPROBE -Dexec.args="topic=m4.reset.v1 count=300 size=100"
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.reset-earliest

$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --delete-offsets --group m4.reset-earliest --topic m4.reset.v1
```

Теперь запустите группу снова, без `auto.offset.reset`, в фоне. Пока она работает, запишите ещё 10 записей — как будто сервис продолжает получать новые заказы:

```bash
$CPROBE -Dexec.args="id=c1 topic=m4.reset.v1 group.id=m4.reset-earliest group.protocol=consumer" > /tmp/m4-reset.log 2>&1 & echo $! > /tmp/m4-c1.pid
sleep 15
$PPROBE -Dexec.args="topic=m4.reset.v1 count=10 size=100"
sleep 10
kill $(cat /tmp/m4-c1.pid); sleep 3
tail -4 /tmp/m4-reset.log
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.reset-earliest
```

Новые записи нужны, чтобы потребитель что-то обработал и закоммитил: зонд коммитит только после обработки, и без новых записей у группы так и не появилось бы оффсета.

**Предскажите**: сколько из 310 непрочитанных записей будет обработано, и какой `LAG` покажет последняя команда.

### Что зафиксировать

| Группа | `auto.offset.reset` | Обработано | Ошибка |
|---|---|---|---|
| `m4.reset-default` | не задан | | |
| `m4.reset-earliest` | `earliest` | | |
| `m4.reset-none` | `none` | | |
| `m4.reset-duration` | `by_duration:PT1M` | | |
| `m4.reset-earliest` после удаления оффсетов, +10 новых | не задан | | |

Для сценария поломки: сколько записей пропущено в последнем прогоне, была ли хоть одна ошибка или предупреждение, что показал `LAG` после прогона, и почему нулевой лаг здесь не означает, что всё прочитано (раздел 13 теории). Какое значение `auto.offset.reset` вы бы выбрали для сервиса, обрабатывающего заказы, и почему.

---

## 4.5 Цикл ребалансировок

**Сценарий поломки №1.** Обработка пачки дольше `max.poll.interval.ms`.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.storm.v1 --partitions 6 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.storm.v1 count=0 rate=50 keys=100 size=100" > /tmp/m4-producer.log 2>&1 & echo $! > /tmp/m4-producer.pid
```

### Воспроизведение

Бюджет: `max.poll.records=100` записей по `process.ms=200` — 20 секунд на пачку при `max.poll.interval.ms=10000`.

```bash
for n in 1 2; do
  $CPROBE -Dexec.args="id=c$n topic=m4.storm.v1 group.id=m4.storm group.protocol=consumer auto.offset.reset=earliest max.poll.records=100 max.poll.interval.ms=10000 process.ms=200 processed.file=/tmp/m4-storm-c$n.txt" > /tmp/m4-c$n.log 2>&1 & echo $! > /tmp/m4-c$n.pid
done
tail -f /tmp/m4-c1.log
```

**Предскажите** до запуска:

- через сколько секунд после первого `poll()` потребитель выбудет из группы;
- какой метод `ConsumerRebalanceListener` будет вызван — `ОТЗЫВ` или `ПОТЕРЯ` — сверьтесь с таблицей раздела 9 теории;
- чем закончится коммит после обработки пачки, и какое исключение назовёт зонд.

Через 3 минуты остановите потребителей и посчитайте повторы:

```bash
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.storm
for n in 1 2; do kill $(cat /tmp/m4-c$n.pid); done
cat /tmp/m4-storm-c1.txt /tmp/m4-storm-c2.txt | wc -l                    # обработано всего
cat /tmp/m4-storm-c1.txt /tmp/m4-storm-c2.txt | sort -u | wc -l          # уникальных
cat /tmp/m4-storm-c1.txt /tmp/m4-storm-c2.txt | sort | uniq -d | wc -l   # повторённых номеров
grep -h 'не прошёл' /tmp/m4-c1.log /tmp/m4-c2.log | head -3
```

Первая команда — до остановки потребителей: что успела закоммитить группа за три минуты.

### Лечение

Повторите с новой группой `m4.storm-fix`, новыми файлами `/tmp/m4-stormfix-c$n.txt` и `max.poll.records=20` — 4 секунды на пачку.

Остановите продюсер после второго прогона: `kill $(cat /tmp/m4-producer.pid)`.

### Что зафиксировать

| Вариант | Бюджет пачки, с | Событий ПОТЕРЯ | Ошибок коммита | Повторов | `между-poll-макс`, мс |
|---|---|---|---|---|---|
| `max.poll.records=100` | 20 | | | | |
| `max.poll.records=20` | 4 | | | | |

- дословно одна строка с ошибкой коммита;
- последовательность событий одного цикла в логе `c1` — от выбывания до следующего назначения;
- если вызван не тот метод listener'а, что предсказан по разделу 9, — это находка, запишите её;
- почему увеличение `max.poll.interval.ms` до 30 минут тоже остановило бы цикл, и чем такое лечение хуже (раздел 6 теории);
- что закоммитила группа за три минуты цикла, с какого оффсета начинал каждый новый владелец партиции, и что это значит для лага группы.

---

## 4.6 Сравнение протоколов групп

Цель — измерить, что происходит с чтением, когда в группу вступает новый член и когда он выходит. Различать нужно два вида партиций:

- **оставшиеся на месте** — их владелец не меняется. По теории пауза у них возможна только в eager-режиме (раздел 7);
- **переезжающие** — от старого владельца к новому. Пауза у них есть в любом варианте: старый владелец должен отдать партицию, новый — её получить. Вопрос в том, сколько это длится.

Чтобы эффект барьера классического протокола был виден, один из членов группы — **медленный**: `c2` обрабатывает пачку около 3 секунд (`max.poll.records=30`, `process.ms=100`). Классическая ребалансировка ждёт всех членов, а медленный член узнает о ней только при следующем вызове `poll()` (раздел 7). `c1` и `c3` — быстрые.

| Вариант | `<НАСТРОЙКИ>` |
|---|---|
| classic, eager | `group.protocol=classic` |
| classic, cooperative | `group.protocol=classic partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor` |
| consumer | `group.protocol=consumer` |

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.proto.v1 --partitions 6 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.proto.v1 count=0 rate=300 keys=100 size=100" > /tmp/m4-producer.log 2>&1 & echo $! > /tmp/m4-producer.pid
```

### Прогон одного варианта

Подставьте `<ВАРИАНТ>` — `eager`, `coop` или `consumer` — и `<НАСТРОЙКИ>` из таблицы.

```bash
G=m4.proto-<ВАРИАНТ>
SET="<НАСТРОЙКИ>"

$CPROBE -Dexec.args="id=c1 client.id=c1 topic=m4.proto.v1 group.id=$G auto.offset.reset=latest $SET" > /tmp/m4-<ВАРИАНТ>-c1.log 2>&1 & echo $! > /tmp/m4-c1.pid
$CPROBE -Dexec.args="id=c2 client.id=c2 topic=m4.proto.v1 group.id=$G auto.offset.reset=latest max.poll.records=30 process.ms=100 $SET" > /tmp/m4-<ВАРИАНТ>-c2.log 2>&1 & echo $! > /tmp/m4-c2.pid
```

`auto.offset.reset=latest` здесь осознанно: нужен только поток новых записей. Медленный `c2` будет накапливать лаг — это ожидаемо, его собственный `e2e-макс` в этой лабе не используется.

**Проверьте, что вариант действительно тот.** Через 20 секунд:

```bash
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group $G --state
grep -m1 'partition.assignment.strategy' /tmp/m4-<ВАРИАНТ>-c1.log
```

В первом выводе найдите стратегию назначения группы, во втором — список стратегий, с которым стартовал `c1`. Для eager-варианта ожидается `range`, для cooperative — `cooperative-sticky`, для consumer — серверная стратегия. Если стратегия не та, что в таблице, — остановите прогон и разберитесь: результат будет относиться к другому варианту.

Запишите базовый `e2e-макс` у `c1` — по 5–10 последним строкам. Затем добавьте `c3` и засеките время:

```bash
date +%T.%3N
$CPROBE -Dexec.args="id=c3 client.id=c3 topic=m4.proto.v1 group.id=$G auto.offset.reset=latest $SET" > /tmp/m4-<ВАРИАНТ>-c3.log 2>&1 & echo $! > /tmp/m4-c3.pid
```

Через 30 секунд штатно остановите `c3`, тоже засекая время:

```bash
date +%T.%3N; kill $(cat /tmp/m4-c3.pid)
```

Ещё через 30 секунд остановите `c1` и `c2`.

### Где смотреть

| Что измеряем | Где |
|---|---|
| пауза у оставшихся на месте партиций при входе `c3` | наибольший `e2e-макс` у `c1` в первые секунды после входа `c3` |
| события у `c1` при входе и выходе `c3` | `grep -E 'ОТЗЫВ\|НАЗНАЧЕНИЕ\|ПОТЕРЯ' /tmp/m4-<ВАРИАНТ>-c1.log` |
| пауза у переезжающих партиций при входе `c3` | время от `date` перед запуском `c3` до его первого `НАЗНАЧЕНИЕ`, и `e2e-макс` в первых строках `c3` |
| пауза у переезжающих партиций при выходе `c3` | время от `date` перед `kill` до `НАЗНАЧЕНИЕ` с партициями `c3` у `c1` или `c2`, и их `e2e-макс` в эту секунду |

`e2e-макс` в строке — максимум по всем партициям члена. Когда член получает переехавшую партицию, записи, накопившиеся за время переезда, поднимают его `e2e-макс` — поэтому всплеск у получателя относится к переехавшей партиции, а не к его собственным.

### Предсказания

До прогонов, для каждого варианта:

- какие партиции будут отозваны у `c1` при входе `c3` — все или только переезжающая;
- будет ли пауза у оставшихся на месте партиций `c1`, и с чем сопоставима её длительность. Подсказка: от кого зависит, когда закончится классическая ребалансировка;
- сколько займёт переезд партиции к `c3` в новом протоколе. Подсказка: раздел 8 теории — как назначение доходит до члена группы.

Продюсер остановите после третьего варианта: `kill $(cat /tmp/m4-producer.pid)`.

### Проверка раздела 8

Запустите потребителя с клиентской настройкой, которая по разделу 8 в новом протоколе не действует:

```bash
$CPROBE -Dexec.args="id=cx topic=m4.proto.v1 group.id=m4.proto-check group.protocol=consumer session.timeout.ms=10000 idle.stop.ms=5000"
```

**Предскажите**, как поступит клиент: проигнорирует, предупредит или откажется создаваться. Запишите, что произошло.

### Что зафиксировать

Цифры — в `benchmarks/04-consumer.md`, раздел «Протоколы». В конспекте:

- у каких партиций — оставшихся на месте или переезжающих — была пауза в каждом варианте, и чем она объясняется по разделам 7 и 8;
- почему в eager-варианте пауза у быстрого `c1` зависит от медленного `c2`;
- откуда пауза у переезжающих партиций в новом протоколе и чем она ограничена сверху;
- какой вариант выбрать для группы из 50 потребителей с частыми деплоями.

---

## 4.7 Коммит при отзыве партиций

**Сценарий поломки №2.** Повторная обработка при ребалансировке, если оффсеты не закоммичены до отзыва.

Чтобы эффект был виден, зонд коммитит не после каждой пачки, а раз в 5 секунд: `commit.every.ms=5000`. Между коммитами накапливаются обработанные, но не закоммиченные записи — ровно та ситуация, от которой защищает коммит в `onPartitionsRevoked`.

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.revoke.v1 --partitions 6 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.revoke.v1 count=0 rate=200 keys=100 size=100" > /tmp/m4-producer.log 2>&1 & echo $! > /tmp/m4-producer.pid
```

### Без коммита при отзыве

```bash
for n in 1 2; do
  $CPROBE -Dexec.args="id=c$n topic=m4.revoke.v1 group.id=m4.revoke-off group.protocol=consumer auto.offset.reset=earliest commit=sync commit.every.ms=5000 revoke.commit=false processed.file=/tmp/m4-revoke-off-c$n.txt" > /tmp/m4-c$n.log 2>&1 & echo $! > /tmp/m4-c$n.pid
done
```

Через 20 секунд добавьте `c3` с теми же настройками и файлом `/tmp/m4-revoke-off-c3.txt`. Ещё через 20 секунд остановите продюсер, подождите 10 секунд, чтобы потребители дочитали, и остановите всех:

```bash
kill $(cat /tmp/m4-producer.pid); sleep 10
for n in 1 2 3; do kill $(cat /tmp/m4-c$n.pid); done
cat /tmp/m4-revoke-off-c*.txt | sort | uniq -d | wc -l
```

### С коммитом при отзыве

Повторите всё на новом топике — создайте `m4.revoke-on.v1` и запустите продюсер в него — с группой `m4.revoke-on`, `revoke.commit=true` и файлами `/tmp/m4-revoke-on-c$n.txt`.

**Предскажите** до обоих прогонов: сколько повторов будет в каждом варианте. Для первого дайте оценку сверху из скорости продюсера и `commit.every.ms`.

### Что зафиксировать

| Вариант | Повторов | Какие партиции переехали к `c3` | Строки `коммит при отзыве` у `c1` |
|---|---|---|---|
| `revoke.commit=false` | | | |
| `revoke.commit=true` | | | |

- у каких членов группы в каждом прогоне был вызван `ОТЗЫВ`, и какие партиции он содержал — все или только переезжающие (раздел 9 теории);
- строк `коммит при отзыве` у `c1` будет несколько: при входе `c3` и при остановке. Найдите их по времени (`grep -E 'ОТЗЫВ|коммит при отзыве|остановка' /tmp/m4-c1.log`) и объясните, почему в строке при остановке все партиции `c1`, а при входе `c3` — только одна;
- почему повторы касаются только переехавших партиций;
- для сценария поломки: как обнаружить такие повторы в продакшене без номеров в записях, и почему идемпотентный обработчик делает их безвредными (раздел 5).

---

## 4.8 Статическое членство

Цель — проверить раздел 10: перезапуск статического члена не вызывает ребалансировку, а его смерть обнаруживается только по таймауту сессии.

Каждая часть ниже — готовый блок: скопируйте его целиком и выполните в терминале с переменными из 4.0, из каталога `labs/`. Блоки сами ждут нужное время, а в конце печатают всё, что нужно для таблицы. Предсказание делайте **до** запуска блока.

### Часть 0. Топик и продюсер

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.static.v1 --partitions 6 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.static.v1 count=0 rate=100 keys=100 size=100" > /tmp/m4-producer.log 2>&1 & echo $! > /tmp/m4-producer.pid
sleep 5; echo "продюсер запущен"
```

### Часть 1. Динамические члены

**Предскажите**: какие события увидит `c1`, пока `c2` перезапускается; получит ли `c2` после перезапуска те же партиции.

```bash
G=m4.dynamic; L=/tmp/m4-dyn
for n in 1 2; do
  $CPROBE -Dexec.args="id=c$n client.id=c$n topic=m4.static.v1 group.id=$G group.protocol=consumer auto.offset.reset=latest" > $L-c$n.log 2>&1 & echo $! > /tmp/m4-c$n.pid
done
sleep 30

echo "=== перезапуск c2: $(date +%T)"
P=$(cat /tmp/m4-c2.pid); kill $P; while kill -0 $P 2>/dev/null; do sleep 0.5; done
echo "=== c2 остановлен: $(date +%T)"
$CPROBE -Dexec.args="id=c2 client.id=c2 topic=m4.static.v1 group.id=$G group.protocol=consumer auto.offset.reset=latest" > $L-c2-restart.log 2>&1 & echo $! > /tmp/m4-c2.pid
sleep 30

echo "=== события c1:";          grep -E 'ОТЗЫВ|НАЗНАЧЕНИЕ|ПОТЕРЯ' $L-c1.log
echo "=== партиции c2 до:";      grep 'НАЗНАЧЕНИЕ' $L-c2.log | tail -1
echo "=== партиции c2 после:";   grep 'НАЗНАЧЕНИЕ' $L-c2-restart.log | tail -1

for n in 1 2; do P=$(cat /tmp/m4-c$n.pid); kill $P; while kill -0 $P 2>/dev/null; do sleep 0.5; done; done
echo "=== часть 1 завершена"
```

### Часть 2. Статические члены

То же, но с `group.instance.id`. **Предскажите**: что изменится в событиях `c1` и в партициях `c2` после перезапуска.

```bash
G=m4.static; L=/tmp/m4-st
for n in 1 2; do
  $CPROBE -Dexec.args="id=c$n client.id=c$n group.instance.id=c$n topic=m4.static.v1 group.id=$G group.protocol=consumer auto.offset.reset=latest" > $L-c$n.log 2>&1 & echo $! > /tmp/m4-c$n.pid
done
sleep 30

echo "=== перезапуск c2: $(date +%T)"
P=$(cat /tmp/m4-c2.pid); kill $P; while kill -0 $P 2>/dev/null; do sleep 0.5; done
echo "=== c2 остановлен: $(date +%T)"
$CPROBE -Dexec.args="id=c2 client.id=c2 group.instance.id=c2 topic=m4.static.v1 group.id=$G group.protocol=consumer auto.offset.reset=latest" > $L-c2-restart.log 2>&1 & echo $! > /tmp/m4-c2.pid
sleep 30

echo "=== события c1:";          grep -E 'ОТЗЫВ|НАЗНАЧЕНИЕ|ПОТЕРЯ' $L-c1.log
echo "=== партиции c2 до:";      grep 'НАЗНАЧЕНИЕ' $L-c2.log | tail -1
echo "=== партиции c2 после:";   grep 'НАЗНАЧЕНИЕ' $L-c2-restart.log | tail -1
echo "=== часть 2 завершена, c1 и c2 продолжают работать"
```

Не останавливайте `c1` и `c2` — они нужны в части 3.

### Часть 3. Смерть статического члена

`c2` убивается без штатного выхода. **Предскажите**, через сколько секунд `c1` получит его партиции, и какой настройкой это определяется (раздел 8 теории: в новом протоколе таймаут сессии задаёт брокер).

```bash
L=/tmp/m4-st
echo "=== kill -9 c2: $(date +%T)"
kill -9 $(cat /tmp/m4-c2.pid)
sleep 70

echo "=== события c1 после убийства:"; grep -E 'ОТЗЫВ|НАЗНАЧЕНИЕ|ПОТЕРЯ' $L-c1.log | tail -3
echo "=== строки состояния c1 вокруг назначения:"
grep -A3 'НАЗНАЧЕНИЕ' $L-c1.log | tail -4

P=$(cat /tmp/m4-c1.pid); kill $P; while kill -0 $P 2>/dev/null; do sleep 0.5; done
kill $(cat /tmp/m4-producer.pid)
echo "=== часть 3 завершена, всё остановлено"
```

Время до переназначения — разница между `kill -9 c2` и временем последнего `НАЗНАЧЕНИЕ` у `c1`. `e2e-макс` в строках сразу после назначения показывает, сколько ждали записи партиций `c2`.

### Что зафиксировать

| Вариант | События у `c1` при перезапуске `c2` | Партиции `c2` до / после | Время до переназначения после смерти, с |
|---|---|---|---|
| динамические | | | — |
| статические | | | |

- чем платит статическое членство, по вашим цифрам;
- была ли пауза у партиций `c2`, пока он перезапускался, в каждом варианте. Сравните `e2e-макс` в первых строках `/tmp/m4-st-c2-restart.log` (статические) и в строках `/tmp/m4-dyn-c1.log` сразу после `НАЗНАЧЕНИЕ` партиций `c2` (динамические);
- как подобрать таймаут сессии для статических членов в Kubernetes, если под перезапускается за 20 секунд.

---

## 4.9 Переигровка с заданного момента

Цель — переставить закоммиченный оффсет группы на момент времени. Инструмент — `kafka-consumer-groups.sh --reset-offsets`, который на брокере ищет оффсет через `.timeindex` (модуль 2, раздел 5; теория, раздел 11).

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.replay.v1 --partitions 1 --replication-factor 1

$PPROBE -Dexec.args="topic=m4.replay.v1 count=500 size=100"
sleep 60
date -u +%Y-%m-%dT%H:%M:%S.000      # запомните: граница между волнами, в UTC
sleep 5
$PPROBE -Dexec.args="topic=m4.replay.v1 count=300 size=100"

$CPROBE -Dexec.args="id=c1 topic=m4.replay.v1 group.id=m4.replay group.protocol=consumer auto.offset.reset=earliest idle.stop.ms=5000"
```

Группа прочитала все 800 записей. Переставьте её на границу между волнами — сначала посмотреть, потом выполнить:

```bash
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --group m4.replay --topic m4.replay.v1 \
  --reset-offsets --to-datetime <ГРАНИЦА> --dry-run
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --group m4.replay --topic m4.replay.v1 \
  --reset-offsets --to-datetime <ГРАНИЦА> --execute

$CPROBE -Dexec.args="id=c1 topic=m4.replay.v1 group.id=m4.replay group.protocol=consumer idle.stop.ms=5000"
```

Время указывается в UTC: утилита работает в контейнере, часовой пояс которого — UTC.

**Предскажите**: какой оффсет покажет `--dry-run`, и сколько записей обработает повторный запуск.

Проверьте ограничение: запустите потребителя группы `m4.replay` в фоне и повторите `--reset-offsets ... --execute`, пока он работает. **Предскажите**, что ответит утилита, и почему (раздел 3 теории: где живёт позиция и где закоммиченный оффсет).

### Что зафиксировать

- оффсет из `--dry-run` и число записей после переигровки;
- ответ утилиты при работающем потребителе;
- ещё два способа той же переигровки без точного времени: `--by-duration` и `--shift-by`. Выполните один из них с `--dry-run` и запишите результат.

---

## 4.10 Лаг

Цель — наблюдать лаг обоими способами из раздела 13 теории: по коммиту и по позиции.

- **по коммиту** — суммарный `LAG` из `kafka-consumer-groups.sh`, сумма по шести партициям;
- **по позиции** — `lag-макс` в строке состояния зонда: метрика клиента `records-lag-max`;
- **во времени** — `e2e-макс` в той же строке: сколько ждала самая старая обработанная за секунду запись.

Каждая часть — готовый блок: скопируйте целиком и выполните в терминале с переменными из 4.0, из каталога `labs/`. Блоки сами снимают замеры каждые 30 секунд и печатают строку на каждый замер: время от начала части, суммарный `LAG` и последнюю строку состояния `c1`. Предсказание делайте до запуска блока.

### Часть 0. Топик и продюсер

```bash
$K $BIN/kafka-topics.sh --bootstrap-server $BS --create --topic m4.lag.v1 --partitions 6 --replication-factor 1
$PPROBE -Dexec.args="topic=m4.lag.v1 count=0 rate=500 keys=100 size=100" > /tmp/m4-producer.log 2>&1 & echo $! > /tmp/m4-producer.pid
sleep 5; echo "продюсер запущен: 500 записей/с"
```

### Часть 1. Потребитель не справляется — около 2,5 минут

`process.ms=5` — один потребитель обрабатывает около 200 записей в секунду при потоке в 500.

**Предскажите**, с какой скоростью будет расти суммарный `LAG` — в записях в секунду.

Первая команда блока сразу закоммичивает для группы конец всех шести партиций. Без неё у партиций, до которых потребитель ещё не добрался, не было бы закоммиченного оффсета, `kafka-consumer-groups.sh` показывал бы для них `-`, и суммарный `LAG` их бы не учитывал. Медленный потребитель подолгу читает одну партицию — выборка по партиции до `max.partition.fetch.bytes` (1 МБ) это несколько тысяч записей, — поэтому таких партиций было бы много. Чтобы это было видно, замер печатает, по скольким партициям посчитан `LAG`.

```bash
$K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --group m4.lag --topic m4.lag.v1 --reset-offsets --to-latest --execute > /dev/null

$CPROBE -Dexec.args="id=c1 client.id=c1 topic=m4.lag.v1 group.id=m4.lag group.protocol=consumer auto.offset.reset=latest process.ms=5" > /tmp/m4-lag-c1.log 2>&1 & echo $! > /tmp/m4-c1.pid
sleep 15

for t in 0 30 60 90 120; do
  [ $t -gt 0 ] && sleep 30
  printf "%4s с | LAG=%-7s | " $t "$($K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.lag 2>/dev/null | awk '$2=="m4.lag.v1" && $6 ~ /^[0-9]+$/ {n++; s+=$6} END {printf "%d (партиций: %d из 6)", s, n}')"
  grep 'обработано=' /tmp/m4-lag-c1.log | tail -1
done
echo "=== часть 1 завершена, c1 продолжает работать"
```

### Часть 2. Масштабирование — около 1,5 минут

Добавляются `c2` и `c3` с теми же настройками. **Предскажите**, начнёт ли лаг уменьшаться, и с какой скоростью.

```bash
for n in 2 3; do
  $CPROBE -Dexec.args="id=c$n client.id=c$n topic=m4.lag.v1 group.id=m4.lag group.protocol=consumer auto.offset.reset=latest process.ms=5" > /tmp/m4-lag-c$n.log 2>&1 & echo $! > /tmp/m4-c$n.pid
done
sleep 15

for t in 0 30 60; do
  [ $t -gt 0 ] && sleep 30
  printf "+%3s с | LAG=%-7s | " $t "$($K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.lag 2>/dev/null | awk '$2=="m4.lag.v1" && $6 ~ /^[0-9]+$/ {n++; s+=$6} END {printf "%d (партиций: %d из 6)", s, n}')"
  grep 'обработано=' /tmp/m4-lag-c1.log | tail -1
done
echo "=== часть 2 завершена"
```

### Часть 3. Потребителей нет — около 1,5 минут

Все потребители останавливаются штатно, продюсер продолжает писать. **Предскажите**, что покажет `kafka-consumer-groups.sh` и что выведет `LagReporter`.

```bash
for n in 1 2 3; do P=$(cat /tmp/m4-c$n.pid); kill $P; while kill -0 $P 2>/dev/null; do sleep 0.5; done; done
echo "=== потребители остановлены: $(date +%T)"

for t in 0 30 60; do
  [ $t -gt 0 ] && sleep 30
  printf "+%3s с | LAG=%s\n" $t "$($K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group m4.lag 2>/dev/null | awk '$2=="m4.lag.v1" && $6 ~ /^[0-9]+$/ {n++; s+=$6} END {printf "%d (партиций: %d из 6)", s, n}')"
done

echo "=== LagReporter:"
mvn -q -pl lab04-consumer exec:java -Dexec.mainClass=dev.learning.kafka.consumer.LagReporter -Dexec.args="m4.lag"

kill $(cat /tmp/m4-producer.pid)
echo "=== часть 3 завершена, всё остановлено"
```

### Что зафиксировать

| Момент | Членов | `LAG` суммарно (`kafka-consumer-groups.sh`) | `lag-макс` у `c1` (зонд) | `e2e-макс` у `c1`, мс |
|---|---|---|---|---|
| 0 с | 1 | | | |
| 30 с | 1 | | | |
| 60 с | 1 | | | |
| 90 с | 1 | | | |
| 120 с | 1 | | | |
| после добавления, +0 с | 3 | | | |
| после добавления, +30 с | 3 | | | |
| после добавления, +60 с | 3 | | | |
| без потребителей, +0 с | 0 | | — | — |
| без потребителей, +30 с | 0 | | — | — |
| без потребителей, +60 с | 0 | | — | — |

- почему `lag-макс` зонда меньше суммарного `LAG` — что именно каждый из них показывает (раздел 13 теории);
- какой из способов остался доступен, когда потребителей не стало, и что это значит для алертов;
- скорость роста `LAG` равна разнице между скоростью записи и суммарной скоростью обработки. Найдите обе по логам — продюсера (`grep 't=' /tmp/m4-producer.log | tail -3`, прирост `send()` за секунду) и потребителей (`обработано=…/с`) — и проверьте, сходится ли с ростом `LAG` в таблице;
- алерт с порогом `LAG > 1000` записей: через сколько секунд задержки он сработает для топика с потоком 5 записей в секунду и для топика с потоком 500? Какую величину поставить на алерт, чтобы один порог подходил обоим топикам (раздел 13 теории)?

---

## 4.11 pause и resume

Цель — проверить раздел 11 теории: пауза останавливает выборку, но не членство, пока продолжается `poll()`.

```bash
$PPROBE -Dexec.args="topic=m4.lag.v1 count=0 rate=100 keys=100 size=100" > /tmp/m4-producer.log 2>&1 & echo $! > /tmp/m4-producer.pid

$CPROBE -Dexec.args="id=c1 topic=m4.lag.v1 group.id=m4.pause group.protocol=consumer auto.offset.reset=latest max.poll.interval.ms=10000 pause.at=500 pause.ms=30000"
```

Пауза — 30 секунд при `max.poll.interval.ms=10000`.

**Предскажите**: будет ли событие `ПОТЕРЯ` или `ОТЗЫВ` во время паузы; что будут показывать `обработано/с`, `между-poll-макс` и `lag-макс` во время паузы и после неё.

Остановите зонд через Ctrl+C после возобновления, продюсер — `kill $(cat /tmp/m4-producer.pid)`.

### Что зафиксировать

- строки состояния во время паузы и после возобновления;
- почему группа не исключила потребителя, хотя пауза втрое дольше `max.poll.interval.ms`, и чем эта ситуация отличается от лабы 4.5;
- для какой задачи нужен `pause`, если бы обработка шла в пуле потоков (раздел 12 теории): какие ещё два варианта есть у потока `poll()`, когда пул переполнен, и чем каждый из них плох;
- что показывал `lag-макс` во время паузы, хотя записи копились, и почему (раздел 11 теории);
- откуда `обработано` больше 3000 в первую секунду после возобновления при `max.poll.records=500`.

---

## Гейт закрытия модуля

**Самопроверка без материалов.** Письменные ответы на двенадцать контрольных вопросов теории.

**Артефакты:**

- `benchmarks/04-consumer.md` — сравнение протоколов из 4.6 с колонкой «почему так»;
- `notes/04-consumer.md` — таблица экспериментов, наблюдения по лабам и раздел «почему так устроено»: позиция и коммит, два детектора отказа, отличие нового протокола;
- `ConsumerProbe.java` и правка `logback.xml` закоммичены.

**Сценарии поломки** — четыре, каждый с воспроизведением, симптомом, диагностикой и выводом:

1. цикл ребалансировок — 4.5;
2. повторы при ребалансировке без коммита в `onPartitionsRevoked` — 4.7;
3. тихий пропуск данных с `auto.offset.reset=latest` — 4.4;
4. простаивающие члены группы — 4.3.

Модуль закрыт, когда все три условия выполнены.
