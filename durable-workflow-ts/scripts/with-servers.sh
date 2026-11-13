#!/usr/bin/env bash
# Run a command with throwaway Postgres and Redis servers for the SDK's
# server tests, e.g. `scripts/with-servers.sh npm test`.
#
# Exports DURABLE_TEST_DATABASE_URL and DURABLE_TEST_REDIS_URL, then removes
# the containers when the command exits. If DURABLE_TEST_DATABASE_URL is
# already set, the caller's servers are used and nothing is started.
set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "usage: $0 <command> [args...]" >&2
    exit 2
fi

if [[ -n "${DURABLE_TEST_DATABASE_URL:-}" ]]; then
    exec "$@"
fi

if ! docker info >/dev/null 2>&1; then
    echo "error: Docker is not running; start it, or set DURABLE_TEST_DATABASE_URL (and DURABLE_TEST_REDIS_URL)" >&2
    exit 1
fi

containers=()
cleanup() {
    if ((${#containers[@]})); then
        docker rm -f "${containers[@]}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# `docker run`, retrying pulls a few times: registries rate limit. Prints the
# container id; the caller records it (this runs in a `$(...)` subshell).
start() {
    local attempt
    for attempt in 1 2 3 4; do
        if docker run -d --rm -P "$@"; then
            return 0
        fi
        sleep $((attempt * 5))
    done
    echo "error: could not start a container: $*" >&2
    return 1
}

# The host port Docker published for a container port.
host_port() {
    docker port "$1" "$2" | head -n 1 | sed 's/.*://'
}

wait_until() {
    local what=$1
    shift
    for _ in $(seq 1 120); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    echo "error: $what did not become ready" >&2
    return 1
}

pg=$(start -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=durable postgres:17)
containers+=("$pg")
redis=$(start redis:7)
containers+=("$redis")

# `-h 127.0.0.1`: the image's first, socket-only server during init does not count.
wait_until "Postgres" docker exec "$pg" pg_isready -U postgres -h 127.0.0.1 -q
wait_until "Redis" docker exec "$redis" redis-cli ping

export DURABLE_TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$(host_port "$pg" 5432/tcp)/durable"
export DURABLE_TEST_REDIS_URL="redis://127.0.0.1:$(host_port "$redis" 6379/tcp)"

# Not `exec`: the trap removes the containers when the command is done.
"$@"
