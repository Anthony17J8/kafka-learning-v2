# kafka-learning

Учебный монорепозиторий по Apache Kafka 4.x. Окружения, лабораторные работы и заготовки под финальные проекты по роадмапу.

## Быстрый старт

```bash
cp .env.example .env
./kl check                 # проверить, что стоят docker, jdk 21, maven

# Модули 0-5: одиночный брокер
./kl up single
./kl topics create

# Проверка: отправить и прочитать
./kl produce async 10000
./kl tail labs.orders.created.v1
```

UI кластера: http://localhost:8080

Для модулей 6 и дальше нужен полноценный кластер:

```bash
./kl down
./kl up cluster            # 3 брокера, RF=3, Schema Registry
./kl topics create         # RF определится автоматически
./kl health
```

Все команды: `./kl help`. Скрипт сам определяет, какое окружение поднято,
и подставляет нужный bootstrap — руками экспортировать `BOOTSTRAP_SERVERS`
не нужно, но можно, если хотите переопределить.

## Что где лежит

| Каталог | Содержимое |
|---|---|
| `docker/` | три окружения: одиночный брокер, кластер 3×брокер + Schema Registry, мониторинг |
| `labs/` | Maven-мультимодуль с кодом лабораторных |
| `kl` | единая точка входа: окружения, топики, лабы, учения |
| `scripts/` | создание топиков, проверка здоровья, сценарии отказов, общая bash-библиотека |
| `notes/` | конспекты по модулям — заполняете вы |
| `projects/` | финальные проекты |
| `benchmarks/` | результаты нагрузочных тестов и выводы |

## Модули лаб

| Модуль | Каталог | Что внутри |
|---|---|---|
| 3. Producer | `labs/lab03-producer` | три режима отправки с замером, кастомный partitioner |
| 4. Consumer | `labs/lab04-consumer` | ручной коммит, rebalance listener, lag-репортер |
| 6. Транзакции | `labs/lab06-transactions` | read-process-write с EOS и zombie fencing |
| 7. Администрирование | `labs/lab07-admin` | диагностика кластера через Admin API |
| 12. Streams | `labs/lab12-streams` | оконная агрегация + тесты на TopologyTestDriver |

Модули 2, 5, 8-11, 13-15 из роадмапа выполняются поверх этой же инфраструктуры: CLI-упражнения, Schema Registry, Connect, безопасность. Каталоги под них добавляйте по мере прохождения, соблюдая ту же структуру.

## Порядок работы над модулем

1. Прочитать раздел документации из роадмапа.
2. Выполнить задания, дописывая код в соответствующий `labs/labNN-*`.
3. Записать выводы в `notes/NN-*.md` по шаблону `notes/TEMPLATE.md`.
4. Ответить на контрольные вопросы модуля письменно. Если ответ не пишется — модуль не закрыт.

## Учения по отказам

```bash
./kl chaos kill-broker 2      # убить брокер под нагрузкой
./kl chaos isolate 3          # сетевая изоляция
./kl chaos rolling-restart    # перезапуск всего кластера
./kl chaos fill-disk 1        # заполнить диск балластом
```

Список сценариев: `./kl chaos`.

Цель rolling restart: ноль ошибок на стороне клиента. Если ошибки есть, разберитесь почему, прежде чем идти дальше.

## Переменные окружения для лаб

| Переменная | Назначение |
|---|---|
| `BOOTSTRAP_SERVERS` | адреса брокеров |
| `ACKS`, `LINGER_MS`, `BATCH_SIZE`, `COMPRESSION` | параметры producer для экспериментов модуля 3 |
| `GROUP_PROTOCOL` | `consumer` (KIP-848) или `classic` — сравнение в модуле 4 |
| `MAX_POLL_RECORDS`, `MAX_POLL_INTERVAL_MS`, `PROCESSING_MS` | воспроизведение rebalance storm |
| `PROCESSING_GUARANTEE`, `NUM_STANDBY` | режимы Kafka Streams |

Пример:

```bash
ACKS=1 COMPRESSION=zstd LINGER_MS=50 ./kl produce async 500000
```

## Проверка окружения

Нужны: Docker 24+, docker compose v2, JDK 21, Maven 3.9+, около 6 ГБ свободной RAM для кластера.

```bash
./kl check
```

## Справочник команд

```
Окружение
  ./kl check                       проверить инструменты
  ./kl up single|cluster|monitoring
  ./kl down [--keep-data]
  ./kl restart cluster
  ./kl ps                          что запущено
  ./kl logs [сервис]

Топики
  ./kl topics create               создать учебные топики
  ./kl topics list
  ./kl topics describe labs.orders.created.v1
  ./kl topics purge                удалить все labs.*

Диагностика
  ./kl health                      состояние кластера
  ./kl doctor                      то же через Admin API
  ./kl groups [группа]             consumer-группы и lag
  ./kl lag [группа]                lag через Admin API
  ./kl dump <топик> [партиция]     разбор сегментов лога

Лабы
  ./kl build                       сборка labs/
  ./kl test                        тесты
  ./kl produce [режим] [кол-во]
  ./kl consume
  ./kl tx [transactional.id]
  ./kl streams
  ./kl bench [размер] [кол-во]

Прочее
  ./kl shell                       bash внутри брокера
  ./kl tail <топик>
  ./kl chaos <сценарий>
```

Скрипты `scripts/*.sh` работают и напрямую, без `kl`: общие функции вынесены в
`scripts/lib/common.sh`, который каждый из них подключает сам.
