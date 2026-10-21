# Свой код: клиенты — результаты приёмки

Требования и критерии — в [docs/modules/clients-own-code.md](../docs/modules/clients-own-code.md). Решения и их
обоснование — в README модуля и use case. Здесь — только доказательства.

Формат доказательства: команда, её вывод (сокращённый до значимых строк) и одна фраза вывода.

## Общие решения

- Ключ записи:

> sellerId, т.к. по ТЗ необходимо гарантировать порядок в пределах одного продавца

- Число партиций `orders.created.v1` и расчёт:

> 32 партиции — потолок в 32 потока, с запасом под рост до 6000 заказов в секунду (нужно ~19 потоков). Масштабирование:
> 8 → 16 → 32 потока, кратно числу партиций. Горячий продавец — до 5% потока — при текущей нагрузке укладывается в модель
> всплесков, но при росте втрое его партиция загружает один поток почти на 100%, и дополнительные партиции этого не
> исправят. Поэтому обработка заказов одного продавца в пачке poll() объединяется в одну транзакцию — см. решения UC-2.

- `acks` / `min.insync.replicas` — учебный стенд и продакшен:

> acks = all(1), min.insync.replicas = 1 - на учебном
>
> acks =all, min.insync.replicas = 2 (RF=3) - в проде
---

## UC-1. Приём заказов

| ID    | Пройден | Ключевые цифры |
|-------|---------|----------------|
| AC1.1 |         |                |
| AC1.2 |         |                |
| AC1.3 |         |                |
| AC1.4 |         |                |
| AC1.5 |         |                |
| AC1.6 |         |                |

### AC1.1

```
```

### AC1.2

```
```

### AC1.3

```
```

### AC1.4

```
```

### AC1.5

```
```

### AC1.6

```
```

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

### 2-Ж

```

```

### AC2.5

```
```

### AC2.6

```
```

---

## UC-3. Переигровка заказов за период

| ID    | Пройден | Ключевые цифры |
|-------|---------|----------------|
| AC3.1 |         |                |
| AC3.2 |         |                |
| AC3.3 |         |                |
| AC3.4 |         |                |

### AC3.1

```
```

### AC3.2

```
```

### AC3.3

```
```

### AC3.4

```
```

---

## UC-4. Тяжёлая обработка

Расчёт перед началом:

- последовательно в потоке `poll()`, заказов/с на экземпляр:
- экземпляров при модели «потребитель на поток»; хватит ли партиций:
- параллелизм в экземпляре для 500 заказов/с с запасом:

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
