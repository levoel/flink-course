#!/usr/bin/env bash
# Kill taskmanager-1 во время самого commit (после notifyCheckpointComplete, до commitTransaction return).
# Watch: "notifyCheckpointComplete called" в TM logs → kill within milliseconds.
set -euo pipefail

TM=${TM:-taskmanager-1}
echo "[chaos] waiting for notifyCheckpointComplete in $TM..."

docker compose logs -f --tail=0 "$TM" 2>&1 | \
    while IFS= read -r line; do
        if echo "$line" | grep -qE "(notifyCheckpointComplete|commitTransaction)"; then
            echo "[chaos] commit phase detected: $line"
            echo "[chaos] killing $TM in milliseconds"
            docker compose kill -s SIGKILL "$TM"
            echo "[chaos] $TM killed during commit"
            echo "[chaos] Kafka transaction may be in pending state; recovery will retry."
            exit 0
        fi
    done
