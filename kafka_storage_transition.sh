#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <fs-name> <log-dir>"
    echo "Example:"
    echo "  $0 ext4 /mnt/ext4/kafka-logs"
    echo "  $0 f2fs /mnt/f2fs/kafka-logs"
    exit 1
fi

FS_NAME="$1"
LOG_DIR="$2"

KAFKA_HOME=${KAFKA_HOME:-"$HOME/kafka_2.13-4.2.0"}
KAFKA_BIN=${KAFKA_BIN:-"$KAFKA_HOME/bin"}
KAFKA_CONFIG=${KAFKA_CONFIG:-"$KAFKA_HOME/config/server.properties"}
KAFKA_PID_FILE=${KAFKA_PID_FILE:-"/tmp/kafka-server.pid"}

echo "=== Kafka storage transition ==="
echo "fs-name      : $FS_NAME"
echo "log.dirs     : $LOG_DIR"
echo "kafka home   : $KAFKA_HOME"
echo "config       : $KAFKA_CONFIG"

# 1. Stop Kafka if running
if [ -f "$KAFKA_PID_FILE" ]; then
    KAFKA_PID="$(cat "$KAFKA_PID_FILE")"

    if kill -0 "$KAFKA_PID" 2>/dev/null; then
        echo "Stopping Kafka pid=$KAFKA_PID"
        kill -TERM "$KAFKA_PID"

        for _ in 1 2 3 4 5 6 7 8 9 10; do
            if ! kill -0 "$KAFKA_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done

        if kill -0 "$KAFKA_PID" 2>/dev/null; then
            echo "Kafka did not stop gracefully. Killing..."
            kill -KILL "$KAFKA_PID" 2>/dev/null || true
        fi
    fi

    rm -f "$KAFKA_PID_FILE"
fi

# 2. Validate Kafka files
if [ ! -x "$KAFKA_BIN/kafka-storage.sh" ]; then
    echo "Missing kafka-storage.sh: $KAFKA_BIN/kafka-storage.sh"
    exit 1
fi

if [ ! -f "$KAFKA_CONFIG" ]; then
    echo "Missing Kafka config: $KAFKA_CONFIG"
    exit 1
fi

# 3. Prepare log directory
echo "Preparing log dir: $LOG_DIR"
sudo mkdir -p "$LOG_DIR"
sudo chown -R "$USER:$USER" "$LOG_DIR"

echo "Cleaning log dir: $LOG_DIR"
rm -rf "$LOG_DIR"/*

# 4. Update log.dirs in server.properties
echo "Updating log.dirs in $KAFKA_CONFIG"

if grep -q '^log\.dirs=' "$KAFKA_CONFIG"; then
    sed -i "s|^log\.dirs=.*|log.dirs=$LOG_DIR|" "$KAFKA_CONFIG"
else
    echo "log.dirs=$LOG_DIR" >> "$KAFKA_CONFIG"
fi

# 5. Generate new cluster UUID
KAFKA_CLUSTER_ID="$("$KAFKA_BIN/kafka-storage.sh" random-uuid)"
echo "Generated cluster id: $KAFKA_CLUSTER_ID"

# 6. Format storage
echo "Formatting Kafka storage"
"$KAFKA_BIN/kafka-storage.sh" format \
    --standalone \
    -t "$KAFKA_CLUSTER_ID" \
    -c "$KAFKA_CONFIG"

echo "=== Transition complete ==="
echo "fs-name  : $FS_NAME"
echo "log.dirs : $(grep '^log\.dirs=' "$KAFKA_CONFIG")"
