#!/usr/bin/env bash
set -euo pipefail

BUCKET="sundew-honeypot-diana"
LOG_DIR="/opt/honeypot/var/log/cowrie"
STATE_FILE="/opt/honeypot/.uploaded_logs"
WORK_DIR="/tmp/honeypot-upload"

mkdir -p "$WORK_DIR"
touch "$STATE_FILE"

uploaded=0

for f in "$LOG_DIR"/cowrie.json.*; do
    [ -e "$f" ] || continue

    name=$(basename "$f")
    day=${name##*.}

    if grep -qxF "$name" "$STATE_FILE"; then
        continue
    fi

    gzip -c "$f" > "$WORK_DIR/upload.gz"

    if aws s3 cp "$WORK_DIR/upload.gz" "s3://$BUCKET/raw/dt=$day/cowrie.json.gz"; then
        echo "$name" >> "$STATE_FILE"
        uploaded=$((uploaded + 1))
        echo "$(date -Is) uploaded $name"
    else
        echo "$(date -Is) FAILED $name" >&2
    fi

    rm -f "$WORK_DIR/upload.gz"
done

echo "$(date -Is) done, uploaded $uploaded file(s)" 