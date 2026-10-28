# Свой код: клиенты — результаты приёмки

Требования и критерии — в [docs/modules/clients-own-code.md](../docs/modules/clients-own-code.md). Решения и их
обоснование — в README модуля и use case. Здесь — только доказательства.

Формат доказательства: команда, её вывод (сокращённый до значимых строк) и одна фраза вывода.

## Общие решения

- Ключ записи:

> sellerId, т.к. по ТЗ необходимо гарантировать порядок в пределах одного продавца

- Число партиций `orders.created.v1` и расчёт:

> 32 партиции — потолок в 32 потока, с запасом под рост до 6000 заказов в секунду (нужно ~19 потоков). Масштабирование:
> 8 → 16 → 32 потока, кратно числу партиций. Горячий продавец — до 5% потока — при текущей нагрузке укладывается в
> модель
> всплесков, но при росте втрое его партиция загружает один поток почти на 100%, и дополнительные партиции этого не
> исправят. Поэтому обработка заказов одного продавца в пачке poll() объединяется в одну транзакцию — см. решения UC-2.

- `acks` / `min.insync.replicas` — учебный стенд и продакшен:

> acks = all(1), min.insync.replicas = 1 - на учебном
>
> acks =all, min.insync.replicas = 2 (RF=3) - в проде
---

## UC-2. Обработка заказов

### 2-Г

```
17:26 $ S=g; mkdir -p /tmp/balances/$S; DB="jdbc:h2:tcp://localhost:9123/$S/db;LOCK_TIMEOUT=10000"; GROUP="balance-$S"
J=/tmp/journal-$S; mkdir -p $J

$INTAKE -Dexec.args="rate=500 duration.ms=300000 journal.folder=$J" > /tmp/intake-$S.log 2>&1 & echo $! > /tmp/intake.pid
start_proc() { $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1 & echo $! > /tmp/proc-$1.pid; }
start_proc p1 a
start_proc p2 a

sleep 60;  echo "kill -9 p1: $(date +%T)"; kill -9 $(cat /tmp/proc-p1.pid); start_proc p1 b
sleep 60;  echo "kill -9 p2: $(date +%T)"; kill -9 $(cat /tmp/proc-p2.pid); start_proc p2 b
sleep 60;  echo "kill -9 p1: $(date +%T)"; kill -9 $(cat /tmp/proc-p1.pid); start_proc p1 c

wait $(cat /tmp/intake.pid); echo "UC-1 завершён: $(date +%T)"
until $K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group $GROUP 2>/dev/null \
      | awk '$2=="orders.created.v1" && $6 ~ /^[0-9]+$/ {n++; s+=$6} END {exit !(n>0 && s==0)}'; do sleep 5; done
echo "лаг 0: $(date +%T)"
for p in p1 p2; do kill $(cat /tmp/proc-$p.pid); done; sleep 10
[2] 1548731
[3] 1548738
[4] 1548752
kill -9 p1: 17:27:08
[5] 1550636
[3]   Убито              $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1
kill -9 p2: 17:28:08
[6] 1551939
[4]   Убито              $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1
kill -9 p1: 17:29:08
[7] 1553610
[2]   Завершён        $INTAKE -Dexec.args="rate=500 duration.ms=300000 journal.folder=$J" > /tmp/intake-$S.log 2>&1
[5]   Убито              $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1
UC-1 завершён: 17:31:12
лаг 0: 17:31:13
[6]-  Выход 143          $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1
[7]+  Выход 143          $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1
✔ ~/IdeaProjects/kafka-learning/labs [main L|✚ 3] 
17:31 $ for f in /tmp/proc-p*-*.log; do echo "$f: $(grep 'Обработано:' $f | tail -1)"; done
/tmp/proc-p1-a.log: 17:27:07.840 INFO  d.l.k.o.p.ProcessingLoop - Обработано: 163598, дубликатов: 0, битых: 0
/tmp/proc-p1-b.log: 17:29:07.435 INFO  d.l.k.o.p.ProcessingLoop - Обработано: 245498, дубликатов: 85, битых: 0
/tmp/proc-p1-c.log: 17:31:13.419 INFO  d.l.k.o.p.ProcessingLoop - Обработано: 28850, дубликатов: 0, битых: 0
/tmp/proc-p2-a.log: 17:28:07.969 INFO  d.l.k.o.p.ProcessingLoop - Обработано: 422764, дубликатов: 206, битых: 0
/tmp/proc-p2-b.log: 17:31:13.421 INFO  d.l.k.o.p.ProcessingLoop - Обработано: 209121, дубликатов: 500, битых: 0

```

### 2-Д

```
15:57 $ S=d; mkdir -p /tmp/balances/$S; DB="jdbc:h2:tcp://localhost:9123/$S/db;LOCK_TIMEOUT=10000"; GROUP="balance-$S"
J=/tmp/journal-$S; mkdir -p $J

$INTAKE -Dexec.args="rate=500 duration.ms=180000 journal.folder=$J" > /tmp/intake-$S.log 2>&1 & echo $! > /tmp/intake.pid
start_proc() { $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1 & echo $! > /tmp/proc-$1.pid; }
start_proc p1 a
start_proc p2 a

sleep 60; echo "вход p3: $(date +%T)";  start_proc p3 a
sleep 60; echo "выход p1: $(date +%T)"; kill $(cat /tmp/proc-p1.pid)

wait $(cat /tmp/intake.pid)
until $K $BIN/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group $GROUP 2>/dev/null \
      | awk '$2=="orders.created.v1" && $6 ~ /^[0-9]+$/ {n++; s+=$6} END {exit !(n>0 && s==0)}'; do sleep 5; done
for p in p2 p3; do kill $(cat /tmp/proc-$p.pid); done; sleep 10
[2] 2243715
[3] 2243723
[4] 2243735
вход p3: 15:58:30
[5] 2245595
выход p1: 15:59:30
[2]   Завершён        $INTAKE -Dexec.args="rate=500 duration.ms=180000 journal.folder=$J" > /tmp/intake-$S.log 2>&1
[3]   Выход 143          $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1
[4]-  Выход 143          $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1
[5]+  Выход 143          $PROC -Dexec.args="group.id=$GROUP client.id=$1 db.url=$DB" > /tmp/proc-$1-$2.log 2>&1

16:00 $ grep -E 'ОТОЗВАНЫ|НАЗНАЧЕНЫ|ПОТЕРЯНЫ|Ошибка при коммите' /tmp/proc-p2-a.log
15:57:45.438 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=16
15:57:45.438 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=17
15:57:45.439 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=18
15:57:45.440 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=19
15:57:45.440 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=20
15:57:45.441 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=21
15:57:45.442 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=22
15:57:45.443 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=23
15:57:45.443 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=24
15:57:45.444 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=25
15:57:45.445 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=26
15:57:45.446 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=27
15:57:45.447 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=28
15:57:45.448 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=29
15:57:45.448 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=30
15:57:45.449 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=31
15:58:35.862 INFO  d.l.k.o.p.RebalanceHandler - ОТОЗВАНЫ(topic=orders.created.v1), [[27, 28, 29, 30, 31]]: 
15:59:36.007 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=6
15:59:36.007 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=7
15:59:36.008 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=8
15:59:36.009 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=9
15:59:36.009 INFO  d.l.k.o.p.RebalanceHandler - НАЗНАЧЕНЫ: Topic=orders.created.v1, partition=10
16:00:37.690 INFO  d.l.k.o.p.RebalanceHandler - ОТОЗВАНЫ(topic=orders.created.v1), [[6, 7, 8, 9, 10, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]]: 

```

### 2-E

```
16:09 $ echo 'это не json' | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic $TOPIC
echo 'k1:NULL' | $K $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic $TOPIC \
  --property parse.key=true --property key.separator=: --property null.marker=NULL
sleep 5
cat /tmp/poison/poison.log
Warning: --property is deprecated and will be removed in a future version. Use --reader-property instead.
22:44337:Unrecognized token 'это': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')  at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 4]:0Y3RgtC+INC90LUganNvbg==
9:38003:Заказ не может быть null или пустым:

 
16:11 $ $K $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC \
  --partition 22 --offset 44337 --max-messages 1 2>/dev/null
это не json

```

---

## UC-3. Переигровка заказов за период

### Средняя волна из трёх — AC3.1

Эталон:

```
09:06 $ grep -o '"orderId":"[^"]*"' /tmp/replay/out.jsonl | cut -d'"' -f4 | sort > /tmp/got.txt
wc -l < /tmp/got.txt; sort -u /tmp/got.txt | wc -l                  # всего и уникальных — должны совпасть
comm -23 /tmp/expected.txt <(sort -u /tmp/got.txt) | wc -l           # должны были выгрузиться, но нет — 0
comm -13 /tmp/expected.txt <(sort -u /tmp/got.txt) | wc -l           # выгружены лишние — 0
0
0
15000
0
```

Результат:

```
09:10 $ jq -r '.value' /tmp/replay/out.jsonl | grep -o '"orderId":"[^"]*"' | cut -d'"' -f4 | sort > /tmp/got.txt
wc -l < /tmp/got.txt; sort -u /tmp/got.txt | wc -l                  # всего и уникальных — должны совпасть
comm -23 /tmp/expected.txt <(sort -u /tmp/got.txt) | wc -l           # должны были выгрузиться, но нет — 0
comm -13 /tmp/expected.txt <(sort -u /tmp/got.txt) | wc -l           # выгружены лишние — 0
15000
15000
0
0
```

### Партиция без записей в интервале — AC3.2

```
10:38:20.432 INFO  d.l.kafka.own.replay.ReplayApp - Составленные планы для чтения:
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - ===============
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=0, foundStartOffset=41553, endOffset=41554, beginningOffset=1215, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:1, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:2, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:3, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=4, foundStartOffset=115728, endOffset=115729, beginningOffset=3280, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:5, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:6, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:7, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=8, foundStartOffset=38801, endOffset=38803, beginningOffset=1155, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:9, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:10, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:11, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=12, foundStartOffset=42849, endOffset=42850, beginningOffset=1277, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=13, foundStartOffset=42832, endOffset=42833, beginningOffset=1230, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=14, foundStartOffset=41551, endOffset=41552, beginningOffset=1214, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:15, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=16, foundStartOffset=37883, endOffset=37884, beginningOffset=1185, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=17, foundStartOffset=39142, endOffset=39143, beginningOffset=1195, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:18, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:19, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:20, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:21, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:22, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:23, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:24, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:25, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:26, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:27, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition=28, foundStartOffset=45547, endOffset=45548, beginningOffset=1359, state=READ
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:29, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:30, state=EMPTY
10:38:20.433 INFO  d.l.kafka.own.replay.ReplayApp - Partition:31, state=EMPTY
10:38:20.648 INFO  d.l.kafka.own.replay.ReplayApp - Записей прочитано:
10:38:20.648 INFO  d.l.kafka.own.replay.ReplayApp - ===============
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=8 - 2
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=4 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=16 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=0 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=17 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=28 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=12 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=13 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=14 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Записей выгружено:
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - ===============
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=8 - 2
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=4 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=16 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=0 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=17 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=28 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=12 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=13 - 1
10:38:20.649 INFO  d.l.kafka.own.replay.ReplayApp - Partition=14 - 1

```

### Запись во время выгрузки — AC3.3

```
10:51 $ FROM_MS=$(date -d "$FROM" +%s%3N); TO_MS=$(date -d "$TO" +%s%3N)
$K $BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC --from-beginning \
  --timeout-ms 20000 --property print.timestamp=true 2>/dev/null \
  | awk -F'\t' -v f=$FROM_MS -v t=$TO_MS '{split($1, a, ":"); if (a[2] >= f && a[2] < t) print $2}' \
  | grep -o '"orderId":"[^"]*"' | cut -d'"' -f4 | sort -u > /tmp/expected.txt
wc -l < /tmp/expected.txt
0

10:53 $ jq -r '.value' /tmp/replay/out.jsonl | grep -o '"orderId":"[^"]*"' | cut -d'"' -f4 | sort > /tmp/got.txt
wc -l < /tmp/got.txt; sort -u /tmp/got.txt | wc -l                  # всего и уникальных — должны совпасть
comm -23 /tmp/expected.txt <(sort -u /tmp/got.txt) | wc -l           # должны были выгрузиться, но нет — 0
comm -13 /tmp/expected.txt <(sort -u /tmp/got.txt) | wc -l           # выгружены лишние — 0
15000
15000
0
15000

10:53 $ $K $BIN/kafka-get-offsets.sh --bootstrap-server $BS --topic $TOPIC
..
orders.created.v1:30:39900
..

{"partition":30,"offset":37743,"timestamp":1792908121316,"value":"{\"orderId\":\"c2edf870-5a2d-49f7-8db9-edb4fd312995\",\"sellerId\":\"seller-224\",\"amount\":15090,\"tenant\":\"standard\",\"createdAt\":\"2026-10-25T06:02:01.316636239Z\"}"}

```

### Начало удалено retention — AC3.4

```

14:46:58.845 INFO  d.l.kafka.own.replay.ReplayApp - Составленные планы для чтения:
14:46:58.846 INFO  d.l.kafka.own.replay.ReplayApp - ===============
14:46:58.846 INFO  d.l.kafka.own.replay.ReplayApp - Partition=0, foundStartOffset=405, endOffset=1715, beginningOffset=405, state=READ_TRUNCATED
14:46:59.099 INFO  d.l.kafka.own.replay.ReplayApp - Записей прочитано:
14:46:59.099 INFO  d.l.kafka.own.replay.ReplayApp - ===============
14:46:59.100 INFO  d.l.kafka.own.replay.ReplayApp - Partition=0 - 1315
14:46:59.100 INFO  d.l.kafka.own.replay.ReplayApp - Записей выгружено:
14:46:59.100 INFO  d.l.kafka.own.replay.ReplayApp - ===============
14:46:59.100 INFO  d.l.kafka.own.replay.ReplayApp - Partition=0 - 1310
[2]+  Выход 143          $PPROBE -Dexec.args="topic=replay.retention.v1 count=12000 rate=20 size=50" > /tmp/probe-ret.log 2>&1

```

---

## UC-4. Тяжёлая обработка

Расчёт перед началом:

- последовательно в потоке `poll()`, заказов/с на экземпляр:

> берем за время обработки среднее значение - 125ms.
> Получается что в одном потоке будет обрабатываться 8 з/c

- экземпляров при модели «потребитель на поток»; хватит ли партиций:

> при требуемой пропускной способности 500 заказов/с и пропускной способности 1 потребителя 8 заказов/с, получается
> нам необходимо 63 потребителя. При 32 партициях мы сможем обеспечить только ~256 заказа/сек.

- параллелизм по закону Литтла, с запасом:

> одновременно в работе = поток в секунду × средняя длительность в секундах
> 62,5 = 500 * (125 / 1000), с учетом целевой нагрузки пула в 65%  - **96**

- крупный продавец: пропускная способность одной очереди против его потока; выбранный вариант порядка: порядок результата

> 8 - столько проверок в секунду пройдёт через одну последовательную очередь при средней длительности проверки;
> 25 - столько заказов в секунду у крупнейшего продавца
> хватит ли первого на второе? - нет
> Упорядочиваем вердикты не по продавцу, а по партиции: строго в порядке оффсетов

- запас между порогом паузы и ёмкостью очередей:

> max.poll.records = 100 (выбираем сами)
> возьмем емкость очереди в 300 записей на каждую очередь 
> верхний порог - 200. Нижний порог - 80.
> Выбираем drain.timeout.ms = 300 / 96 × 1 с ≈ 3 секунды. Берем в 2–3 раза больше = 10 сек. А max.poll.interval.ms можно брать default значение.

| ID    | Пройден | Ключевые цифры |
|-------|---------|----------------|
| AC4.1 |         |                |
| AC4.2 |         |                |
| AC4.3 |         |                |
| AC4.4 |         |                |

### AC4.1

```
```

### AC4.2

```
```

### AC4.3

```
```

### AC4.4

```
```

---

## UC-5. Наблюдение за отставанием

| ID    | Пройден | Ключевые цифры |
|-------|---------|----------------|
| AC5.1 |         |                |
| AC5.2 |         |                |
| AC5.3 |         |                |
| AC5.4 |         |                |

### AC5.1

```
```

### AC5.2

```
```

### AC5.3

```
```

### AC5.4

```
```

---

## Юнит-тесты

```
mvn -q -pl own-clients test
```

| Класс             | Тестов | Проходят |
|-------------------|--------|----------|
| `BalanceStore`    |        |          |
| `OrderedExecutor` |        |          |
| `CommitTracker`   |        |          |
| `AlertRules`      |        |          |

---

## Когда этот подход не нужен

- UC-1:
- UC-2:
- UC-3:
- UC-4:
- UC-5:

## Открытые вопросы
