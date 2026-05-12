#!/usr/bin/env bash
# Kill taskmanager-1 в момент когда KafkaWriter в фазе preCommit (sending records).
# Watch logs на "Snapshot" started → wait ~50ms → kill.
set -euo pipefail

TM=${TM:-taskmanager-1}
echo "[chaos] waiting for next checkpoint preCommit phase..."

docker compose logs -f --tail=0 jobmanager 2>&1 | \
    while IFS= read -r line; do
        if echo "$line" | grep -q "Triggering checkpoint"; then
            echo "[chaos] checkpoint triggered: $line"
            echo "[chaos] sleeping 100ms then killing $TM"
            sleep 0.1
            docker compose kill -s SIGKILL "$TM"
            echo "[chaos] $TM killed during preCommit"
            echo "[chaos] watch recovery: docker compose logs -f $TM (it auto-restarts)"
            exit 0
        fi
    done
