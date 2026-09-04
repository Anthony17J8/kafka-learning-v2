#!/usr/bin/env bash
#
# Быстрая проверка состояния кластера. Модули 7 и 10.
# Вызов: ./kl health   или   ./scripts/cluster-health.sh

set -euo pipefail

# shellcheck source=lib/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
load_env

section() { printf '\n%s=== %s ===%s\n' "$C_BOLD" "$1" "$C_RESET"; }

section "Кворум контроллеров"
docker exec -i "$(broker_container)" /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server "$(internal_bootstrap)" describe --status || \
  warn "не удалось получить статус кворума"

section "Under-replicated партиции (должно быть пусто)"
kafka_cli kafka-topics.sh --describe --under-replicated-partitions

section "Партиции без лидера (должно быть пусто)"
kafka_cli kafka-topics.sh --describe --unavailable-partitions

section "Партиции с min.insync.replicas под угрозой"
kafka_cli kafka-topics.sh --describe --at-min-isr-partitions || true

section "Consumer-группы"
kafka_cli kafka-consumer-groups.sh --list

echo
ok "проверка завершена"
