# Модуль 0. Окружение

## Что читал
- https://kafka.apache.org/quickstart
- https://kafka.apache.org/documentation/#brokerconfigs (listeners, advertised.listeners)

## Ключевые идеи своими словами
<!-- Заполнить. Обязательно ответить: чем listeners отличается от advertised.listeners
     и почему клиент с хоста не может подключиться, если в advertised указано имя контейнера. -->

## Что делал
| Эксперимент | Настройки | Результат | Вывод |
|---|---|---|---|
| Запуск из tar-архива | kafka-storage.sh format + server-start | | |
| Запуск в Docker | compose.single.yml | | |
| Кластер из 3 брокеров | compose.cluster.yml | | |
| Подключение с хоста и из контейнера | два listener'а | | |

## Что сломалось и почему

## Ответы на контрольные вопросы
1. Что такое cluster.id и зачем форматировать хранилище до старта?
2. Что произойдёт, если в advertised.listeners указать имя контейнера, а подключаться с хоста?

## Открытые вопросы
