#!/usr/bin/env bash
# Kill taskmanager-1 в окне между preCommit done и commit started.
# Watch: "Completed checkpoint" (preCommit done, ack received) → before "notifyCheckpointComplete" reaches sink.
set -euo pipefail

TM=${TM:-taskmanager-1}
echo "[chaos] waiting for checkpoint completion event..."

docker compose logs -f --tail=0 jobmanager 2>&1 | \
    while IFS= read -r line; do
        if echo "$line" | grep -qE "Completed checkpoint [0-9]+"; then
            echo "[chaos] checkpoint completed: $line"
            echo "[chaos] killing $TM IMMEDIATELY (preCommit done, commit pending)"
            docker compose kill -s SIGKILL "$TM"
            echo "[chaos] $TM killed. KafkaCommitter on restart should retry commit."
            echo "[chaos] watch logs: docker compose logs -f $TM jobmanager | grep -i commit"
            exit 0
        fi
    done
