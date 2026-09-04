#!/usr/bin/env bash
# Общие функции для всех скриптов. Подключается через: source scripts/lib/common.sh
# Собственный set -euo pipefail каждый скрипт ставит сам.

# shellcheck disable=SC2034
KL_LIB_LOADED=1

# ---------- вывод ----------

if [[ -t 1 ]]; then
  C_RESET=$'\033[0m'; C_RED=$'\033[31m'; C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'; C_BLUE=$'\033[36m'; C_BOLD=$'\033[1m'
else
  C_RESET=""; C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_BOLD=""
fi

info()  { printf '%s==>%s %s\n' "$C_BLUE" "$C_RESET" "$*"; }
ok()    { printf '%s ok %s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn()  { printf '%s warn%s %s\n' "$C_YELLOW" "$C_RESET" "$*" >&2; }
die()   { printf '%serror%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; exit 1; }

# ---------- пути и окружение ----------

# Корень репозитория определяется относительно этого файла, поэтому скрипты
# можно запускать из любого каталога.
KL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export KL_ROOT

SINGLE_COMPOSE="$KL_ROOT/docker/compose.single.yml"
CLUSTER_COMPOSE="$KL_ROOT/docker/compose.cluster.yml"
MONITORING_COMPOSE="$KL_ROOT/docker/compose.monitoring.yml"

load_env() {
  local file="${1:-$KL_ROOT/.env}"
  [[ -f "$file" ]] || return 0
  set -a
  # shellcheck disable=SC1090
  source "$file"
  set +a
}

require() {
  local missing=()
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
  done
  if ((${#missing[@]} > 0)); then
    die "не найдены обязательные команды: ${missing[*]}"
  fi
}

# ---------- docker ----------

dc() {
  local file=$1; shift
  docker compose -f "$file" "$@"
}

# Какое окружение сейчас поднято: single | cluster | none
active_env() {
  command -v docker >/dev/null 2>&1 || { echo none; return 0; }
  if ! docker info >/dev/null 2>&1; then
    echo none; return 0
  fi
  if docker ps --format '{{.Names}}' | grep -qx 'kafka-1'; then
    echo cluster
  elif docker ps --format '{{.Names}}' | grep -qx 'kafka'; then
    echo single
  else
    echo none
  fi
}

# Имя контейнера и внутренний bootstrap для CLI-команд внутри брокера.
broker_container() {
  case "$(active_env)" in
    cluster) echo "${KAFKA_CONTAINER:-kafka-1}" ;;
    single)  echo "${KAFKA_CONTAINER:-kafka}" ;;
    *)       die "ни одно окружение не запущено. Сначала: ./kl up single" ;;
  esac
}

internal_bootstrap() {
  case "$(active_env)" in
    cluster) echo "${INTERNAL_BOOTSTRAP:-localhost:29092}" ;;
    single)  echo "${INTERNAL_BOOTSTRAP:-localhost:19092}" ;;
    *)       die "ни одно окружение не запущено" ;;
  esac
}

# Bootstrap для клиентов, запускаемых на хосте (labs/).
host_bootstrap() {
  if [[ -n "${BOOTSTRAP_SERVERS:-}" ]]; then
    echo "$BOOTSTRAP_SERVERS"
    return
  fi
  case "$(active_env)" in
    cluster) echo "localhost:19092,localhost:29092,localhost:39092" ;;
    *)       echo "localhost:9092" ;;
  esac
}

# Обёртка над kafka-*.sh внутри контейнера брокера.
kafka_cli() {
  local tool=$1; shift
  docker exec -i "$(broker_container)" "/opt/kafka/bin/$tool" \
    --bootstrap-server "$(internal_bootstrap)" "$@"
}

wait_healthy() {
  local container=$1 timeout=${2:-120} elapsed=0
  info "жду healthy: $container"
  while true; do
    local status
    status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
             "$container" 2>/dev/null || echo missing)
    case "$status" in
      healthy) ok "$container healthy"; return 0 ;;
      none)    ok "$container запущен (healthcheck не задан)"; return 0 ;;
      missing) die "контейнер $container не найден" ;;
    esac
    sleep 3
    elapsed=$((elapsed + 3))
    ((elapsed < timeout)) || die "$container не стал healthy за ${timeout}с"
  done
}

# ---------- maven ----------

mvn_run() {
  local module=$1 main=$2; shift 2
  require mvn

  local -a mvn_args=(
    -q -f "$KL_ROOT/labs/pom.xml"
    -pl "$module" -am
    compile exec:java
    "-Dexec.mainClass=$main"
  )
  # Аргументы передаём одной строкой и обязательно в кавычках, иначе
  # "async 100000" развалится на два параметра Maven.
  if (($# > 0)); then
    mvn_args+=("-Dexec.args=$*")
  fi

  info "запуск $main (bootstrap=$(host_bootstrap))"
  BOOTSTRAP_SERVERS="$(host_bootstrap)" mvn "${mvn_args[@]}"
}
