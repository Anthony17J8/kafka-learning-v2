#!/usr/bin/env bash
#
# Учения по отказам. Модули 6, 7, 10.
# Запускать при работающей нагрузке и смотреть, что происходит с клиентами.
# Вызов: ./kl chaos <сценарий> [аргументы]

set -euo pipefail

# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
load_env

usage() {
  cat <<TXT
Использование: ./kl chaos <сценарий> [аргументы]

  kill-broker <n>      жёстко убить брокер n (SIGKILL, без controlled shutdown)
  stop-broker <n>      корректно остановить брокер n
  start-broker <n>     поднять брокер n обратно
  isolate <n>          отрезать брокер n от сети (network partition)
  rejoin <n>           вернуть брокер n в сеть
  rolling-restart      последовательный перезапуск всех брокеров
  slow-network <n>     добавить задержку 200мс на брокере n (нужен tc в контейнере)
  fill-disk <n>        заполнить диск брокера n балластом
  clean-disk <n>       убрать балласт

После каждого сценария зафиксируйте в notes/10-monitoring.md:
что увидел клиент, что показали метрики, за сколько восстановилось.
TXT
}

broker() {
  local n=${1:?укажите номер брокера}
  echo "kafka-$n"
}

case "${1:-}" in
  kill-broker)
    warn "убиваю $(broker "${2:-}") без graceful shutdown"
    docker kill "$(broker "${2:-}")"
    ;;
  stop-broker)
    docker stop "$(broker "${2:-}")"
    ;;
  start-broker)
    docker start "$(broker "${2:-}")"
    wait_healthy "$(broker "${2:-}")"
    ;;
  isolate)
    docker network disconnect kafka-learning "$(broker "${2:-}")"
    warn "брокер отрезан от сети. Верните: ./kl chaos rejoin ${2:-}"
    ;;
  rejoin)
    docker network connect kafka-learning "$(broker "${2:-}")"
    ;;
  rolling-restart)
    for n in 1 2 3; do
      info "перезапуск kafka-$n"
      docker restart "kafka-$n" >/dev/null
      wait_healthy "kafka-$n" 180
    done
    ok "rolling restart завершён. Сколько ошибок увидел клиент?"
    ;;
  slow-network)
    docker exec "$(broker "${2:-}")" sh -c \
      'tc qdisc add dev eth0 root netem delay 200ms 2>/dev/null || echo "tc недоступен в образе"'
    ;;
  fill-disk)
    warn "заполняю диск брокера балластом"
    docker exec "$(broker "${2:-}")" sh -c \
      'dd if=/dev/zero of=/var/lib/kafka/data/ballast bs=1M count=100000 2>&1 | tail -1'
    ;;
  clean-disk)
    docker exec "$(broker "${2:-}")" rm -f /var/lib/kafka/data/ballast
    ok "балласт удалён"
    ;;
  *)
    usage
    exit 1
    ;;
esac
