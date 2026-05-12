#!/bin/sh
# Read producer-output topic с read_committed и проверяет: no duplicates, no gaps.
# Usage: docker compose exec verifier sh /scripts/verify.sh

set -eu

TOPIC=${TOPIC:-producer-output}
MAX_MESSAGES=${MAX_MESSAGES:-100000}

echo "[verify] reading $MAX_MESSAGES messages from $TOPIC with read_committed isolation..."

TMPFILE=$(mktemp)

kafka-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic "$TOPIC" \
    --from-beginning \
    --isolation-level read_committed \
    --max-messages "$MAX_MESSAGES" \
    --timeout-ms 30000 > "$TMPFILE" 2>/dev/null || true

TOTAL=$(wc -l < "$TMPFILE" | tr -d ' ')
echo "[verify] received $TOTAL messages"

if [ "$TOTAL" -eq 0 ]; then
    echo "[verify] ERROR: no messages received"
    rm -f "$TMPFILE"
    exit 1
fi

# Check for duplicates
DUPES=$(sort -n "$TMPFILE" | uniq -d | wc -l | tr -d ' ')
if [ "$DUPES" -gt 0 ]; then
    echo "[verify] FAIL: $DUPES duplicate values found"
    sort -n "$TMPFILE" | uniq -d | head -10
    rm -f "$TMPFILE"
    exit 1
fi

# Check for gaps in sorted unique sequence
UNIQ=$(sort -n -u "$TMPFILE")
MIN=$(echo "$UNIQ" | head -1)
MAX=$(echo "$UNIQ" | tail -1)
COUNT=$(echo "$UNIQ" | wc -l | tr -d ' ')
EXPECTED=$((MAX - MIN + 1))

if [ "$COUNT" -ne "$EXPECTED" ]; then
    GAPS=$((EXPECTED - COUNT))
    echo "[verify] FAIL: $GAPS gaps in range [$MIN..$MAX]"
    # show first few gaps
    seq "$MIN" "$MAX" | sort -n > /tmp/expected.txt
    diff /tmp/expected.txt <(echo "$UNIQ") | head -20
    rm -f "$TMPFILE" /tmp/expected.txt
    exit 1
fi

echo "[verify] PASS: no duplicates, no gaps, $COUNT unique messages in range [$MIN..$MAX]"
rm -f "$TMPFILE"
