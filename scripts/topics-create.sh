#!/usr/bin/env bash
#
# Создание учебных топиков.
# Обычно вызывается через ./kl topics create, но работает и напрямую:
#   REPLICATION_FACTOR=3 ./scripts/topics-create.sh
#
# Автосоздание топиков в compose намеренно выключено: в проде оно тоже
# должно быть выключено, привыкайте сразу.

set -euo pipefail

# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
load_env

RF="${REPLICATION_FACTOR:-$([[ "$(active_env)" == "cluster" ]] && echo 3 || echo 1)}"

create() {
  local name=$1 partitions=$2
  shift 2
  info "$name (partitions=$partitions rf=$RF) $*"
  kafka_cli kafka-topics.sh --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor "$RF" "$@"
}

create labs.orders.created.v1 6
create labs.orders.input.v1 6
create labs.orders.output.v1 6

# Модуль 15: retry-цепочка с нарастающей задержкой и финальный DLQ
create labs.orders.retry-5s.v1 3
create labs.orders.retry-1m.v1 3
create labs.orders.dlq.v1 3 --config retention.ms=1209600000   # 14 дней на разбор инцидентов

# Модуль 2: демонстрация log compaction.
# segment.ms и min.cleanable.dirty.ratio занижены специально, чтобы cleaner
# отработал за секунды, а не за часы. В проде так не делают.
create labs.customers.state.v1 3 \
  --config cleanup.policy=compact \
  --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config delete.retention.ms=10000

# Модуль 2: демонстрация retention по времени
create labs.shortlived.v1 3 \
  --config retention.ms=60000 \
  --config segment.ms=20000

echo
ok "готово. Текущие топики:"
kafka_cli kafka-topics.sh --list
