# kafka-learning

Учебный репозиторий по Apache Kafka 4.x: окружения, лабораторные работы, теория и практикумы по роадмапу.

**Принцип работы: всё вручную.** Никаких `make`, никаких обёрток. Вы пишете `docker compose`, `docker exec`, `mvn` и утилиты Kafka напрямую. Это медленнее, но именно так формируется навык, который переносится на любой проект.

Готовые обёртки в репозитории есть, но лежат отдельно и как учебный материал — см. [раздел ниже](#обёртки-справочный-материал).

## Быстрый старт

```bash
# 1. Окружение
cd docker
cp .env.example .env
docker compose -f compose.single.yml up -d
docker compose -f compose.single.yml ps

# 2. Топики
docker exec -i kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9094 \
  --create --topic labs.orders.created.v1 --partitions 6 --replication-factor 1

# 3. Проверка
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9094 --topic labs.orders.created.v1
```

Все команды с пояснениями: **[docs/commands.md](docs/commands.md)**. Держите открытым в соседней вкладке.

## Требования

Docker 24+, docker compose v2, JDK 21, Maven 3.9+, минимум 8 ГБ памяти, выделенной Docker.

```bash
docker --version && docker compose version && java -version && mvn -v
docker info | grep -i "total memory"
```

## Что где лежит

| Каталог | Содержимое |
|---|---|
| `docker/` | окружения: одиночный брокер, кластер, JMX-override, мониторинг |
| `docs/commands.md` | справочник прямых команд |
| `docs/modules/` | теория и практикум по каждому модулю |
| `labs/` | Maven-мультимодуль с кодом лабораторных |
| `notes/` | ваши конспекты и результаты экспериментов |
| `benchmarks/` | результаты нагрузочных тестов и выводы |
| `projects/` | финальные проекты |
| `kl`, `scripts/` | обёртки, оставлены как справочный материал |

## Окружения

| Файл | Назначение | Bootstrap с хоста |
|---|---|---|
| `compose.single.yml` | один брокер, модули 0–5 | `localhost:9092` |
| `compose.cluster.yml` | 3 ноды + Schema Registry, модули 6+ | `localhost:19092,localhost:29092,localhost:39092` |
| `compose.jmx.yml` | override, добавляет JMX-агент, модуль 10 | — |
| `compose.monitoring.yml` | Prometheus + Grafana, модуль 10 | — |

Внутри compose-сети брокеры доступны как `kafka-1:9094`, `kafka-2:9094`, `kafka-3:9094` (одиночный — `kafka:9094`). Внутренний порт одинаков у всех нод, внешние разные.

## Модули лаб

| Модуль | Каталог | Что внутри |
|---|---|---|
| 3. Producer | `labs/lab03-producer` | три режима отправки с замером, кастомный partitioner |
| 4. Consumer | `labs/lab04-consumer` | ручной коммит, rebalance listener, lag-репортер |
| 6. Транзакции | `labs/lab06-transactions` | read-process-write с EOS и zombie fencing |
| 7. Администрирование | `labs/lab07-admin` | диагностика кластера через Admin API |
| 12. Streams | `labs/lab12-streams` | оконная агрегация + тесты на TopologyTestDriver |

Модули 2, 5, 8–11, 13–15 выполняются поверх той же инфраструктуры. Каталоги под них добавляйте по мере прохождения, соблюдая ту же структуру.

## Порядок работы над модулем

1. Прочитать раздел официальной документации из роадмапа и теорию в `docs/modules/module-NN-theory.md`.
2. Пройти практикум `docs/modules/module-NN-practice.md`, дописывая код в `labs/labNN-*`.
3. Записать выводы в `notes/NN-*.md` по шаблону `notes/TEMPLATE.md`.
4. Ответить на контрольные вопросы письменно. Если ответ не пишется — модуль не закрыт.

## Одна ловушка, о которой стоит знать сразу

Если в контейнере брокера задан `KAFKA_OPTS` с javaagent (это происходит при подключении `compose.jmx.yml`), то **любая** CLI-утилита, запущенная через `docker exec`, унаследует эту переменную, попытается занять уже занятый порт агента и умрёт с `BindException: Address in use`.

Лечится гашением переменной для конкретного вызова:

```bash
docker exec -it -e KAFKA_OPTS= kafka-1 /opt/kafka/bin/kafka-topics.sh ...
```

Ровно по этой причине в healthcheck обоих compose-файлов стоит `KAFKA_OPTS=` перед командой.

## Обёртки: справочный материал

В корне лежит `kl` — bash-CLI с подкомандами, и `scripts/` с общей библиотекой. **Они не являются рабочим интерфейсом репозитория** и могут отставать от compose-файлов.

Оставлены намеренно, как пример того, как такие вещи устроены:

- диспетчер подкоманд через `declare -f` и динамический вызов;
- генерация справки из комментариев в собственном исходнике;
- определение активного окружения и подстановка нужного bootstrap;
- работа с массивами, process substitution, обработка кодов возврата.

Разбор кода построчно — в истории проекта. Когда прямые команды войдут в привычку, полезное упражнение: переписать `kl` под текущие compose-файлы самостоятельно.
