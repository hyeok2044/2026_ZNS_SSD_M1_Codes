#!/usr/bin/env bash
set -euo pipefail

log(){ echo "[$(date '+%Y-%m-%d %H:%M:%S.%3N')] $@"; }
# 0. inputs and initialization

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <fs-name> <kafka-log-dir> <scenario>"
    echo "Example: $0 ext4 /mnt/ext4/kafka-logs producer_only"
    echo "Example: $0 ext4 /mnt/ext4/kafka-logs producer_consumer"
    exit 1
fi

FS_NAME="$1"
LOG_DIR="$2"
SCENARIO="$3"

if [ "$SCENARIO" != "producer_only" ] && [ "$SCENARIO" != "producer_consumer" ]; then
    echo "Invalid scenario: $SCENARIO"
    exit 1
fi

INITIAL_BPS=${INITIAL_BPS:-71680000}   # 70 MB/s-ish
INCR_BPS=${INCR_BPS:-10240000}           # 10 MBPS Increment
MAX_BPS=${MAX_BPS:-122880000}          # 120 MB/s-ish

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
TOPIC=${TOPIC:-${FS_NAME}-test}

WARMUP_SEC=${WARMUP_SEC:-5}
MEASUREMENT_SEC=${MEASUREMENT_SEC:-15}

KAFKA_HOME=${KAFKA_HOME:-$HOME/kafka_2.13-4.2.0}
KAFKA_BIN=${KAFKA_BIN:-$KAFKA_HOME/bin}
IOSTAT_DEV=${IOSTAT_DEV:-nvme0n1}
GRACE=${GRACE:-10}

OUT=${OUT:-results/${FS_NAME}/${SCENARIO}/$(date +%Y%m%d_%H%M%S)}
mkdir -p "$OUT"

echo "=== Experiment start ==="
echo "fs-name    : $FS_NAME"
echo "topic      : $TOPIC"
echo "bootstrap  : $BOOTSTRAP"
echo "log.dirs   : $LOG_DIR"
echo "output dir : $OUT"
echo "initial_bps : $INITIAL_BPS"
echo "incr_bps    : $INCR_BPS"
echo "max_bps     : $MAX_BPS"
echo "warmup sec : $WARMUP_SEC"
echo "measure sec: $MEASUREMENT_SEC"
echo "iostat dev : $IOSTAT_DEV"
echo

# 0-1. Kafka initialization
log "Starting Kafka..."
"$KAFKA_BIN/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties" \
    > "$OUT/kafka-server.log" 2>&1 &
KAFKA_PID=$!

echo "$KAFKA_PID" > /tmp/kafka-server.pid

cleanup() {
    echo "Cleaning up Kafka..."

    kill -TERM "$KAFKA_PID" 2>/dev/null || true
    wait "$KAFKA_PID" 2>/dev/null || true
    
    rm -f /tmp/kafka-server.pid
}
trap cleanup EXIT

echo "Waiting for Kafka..."
for _ in {1..30}; do
    if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
        log "Kafka is ready."
        break
    fi
    sleep 1
done

# PAYLOAD Loop
for PAYLOAD in 1024000 102400 10240 1024; do


    INITIAL_MPS=$((INITIAL_BPS / PAYLOAD))
    INCR_MPS=$((INCR_BPS / PAYLOAD))
    MAX_MPS=$((MAX_BPS / PAYLOAD))

    [ "$INITIAL_MPS" -lt 1 ] && INITIAL_MPS=1
    [ "$INCR_MPS" -lt 1 ] && INCR_MPS=1
    [ "$MAX_MPS" -lt 1 ] && MAX_MPS=1
    # 1. Create Directory
    DIR="$OUT/$PAYLOAD"
    mkdir -p "$DIR"

    echo
    echo "============================================================"
    echo "[PAYLOAD START] payload=$PAYLOAD bytes"
    echo "[DIR] $DIR"
    echo "============================================================"

    # 2. Create Kafka Topics
    log "[1/6] Recreating Kafka topic: $TOPIC"

    echo "  - deleting topic if exists..."
   "$KAFKA_BIN/kafka-topics.sh" \
        --bootstrap-server "$BOOTSTRAP" \
        --delete \
        --topic "$TOPIC" \
        --if-exists || true

    echo "  - waiting for topic deletion..."
    sleep 3

    echo "  - creating topic partitions=8 replication-factor=1..."
    "$KAFKA_BIN/kafka-topics.sh" \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --topic "$TOPIC" \
        --partitions 8 \
        --replication-factor 1 \
        --config max.message.bytes=2097152

    echo "  - waiting for topic stabilization..."
    sleep 10

    # 3. Start measurement
    log "[2/6] Starting system metrics"
    echo "  - iostat device: $IOSTAT_DEV"
    echo "  - iostat output: $DIR/iostat.json"
    iostat -dx 1 -y -t -o JSON "$IOSTAT_DEV" > "$DIR/iostat.json" &
    IO_PID=$!
    echo "  - iostat pid: $IO_PID"

    echo "  - vmstat output: $DIR/vmstat.txt"
    vmstat 1 | awk '{ print strftime("%s"), $0 }' > "$DIR/vmstat.txt" &
    VM_PID=$!
    echo "  - vmstat pid: $VM_PID"

    # 4. Consumer-Producer experiment
    CO_PID=""

    if [ "$SCENARIO" = "producer_consumer" ]; then
        log "[3/6] Starting consumer"

        ./consumer \
            --bootstrap-servers "$BOOTSTRAP" \
            --topic "$TOPIC" \
            --payload-size "$PAYLOAD" \
            --warmup-sec "$WARMUP_SEC" \
            --measurement-sec "$MEASUREMENT_SEC" \
            > "$DIR/consumer.jsonl" &
        CO_PID=$!

        echo "  - consumer pid: $CO_PID"

        echo "  - waiting 3 sec before producer..."
        sleep 3
    else
        log "[3/6] Skip consumer for producer_only"
    fi



    # 5. Producer experiment
    log "[4/6] Starting producer"
    echo "  - payload_size=$PAYLOAD"
    echo "  - initial_mps=$INITIAL_MPS incr_mps=$INCR_MPS max_mps=$MAX_MPS"
    echo "  - warmup_sec=$WARMUP_SEC measurement_sec=$MEASUREMENT_SEC"

    ./producer \
        --bootstrap-servers "$BOOTSTRAP" \
        --topic "$TOPIC" \
        --scenario "$SCENARIO" \
        --payload-size "$PAYLOAD" \
        --initial-mps "$INITIAL_MPS" \
        --incr-mps "$INCR_MPS" \
        --max-mps "$MAX_MPS" \
        --warmup-sec "$WARMUP_SEC" \
        --measurement-sec "$MEASUREMENT_SEC" \
        | tee "$DIR/producer.jsonl"

#    echo "  - using producer stub: sleep 30"
#    sleep 30

    echo "  - producer finished"
    echo "  - grace sleep: $GRACE sec"
    sleep "$GRACE"

    # 6. Epilogue
    log "[5/6] Stopping processes"
    if [ -n "$CO_PID" ]; then
        echo "  - stopping consumer pid=$CO_PID"
        kill -INT "$CO_PID" 2>/dev/null || true
    fi

    echo "  - stopping iostat pid=$IO_PID, vmstat pid=$VM_PID"
    kill -TERM "$IO_PID" "$VM_PID" 2>/dev/null || true

    echo "  - waiting for processes to exit..."
    for _ in 1 2 3 4 5; do
        if ! kill -0 "$CO_PID" 2>/dev/null &&
           ! kill -0 "$IO_PID" 2>/dev/null &&
           ! kill -0 "$VM_PID" 2>/dev/null; then
            echo "  - all processes stopped"
            break
        fi
        sleep 1
    done

    echo "  - force killing remaining processes if any..."
    kill -KILL "$CO_PID" "$IO_PID" "$VM_PID" 2>/dev/null || true
    wait "$CO_PID" "$IO_PID" "$VM_PID" 2>/dev/null || true

    log "[6/6] Payload complete"
    echo "  - results:"
    echo "    $DIR/iostat.json"
    echo "    $DIR/vmstat.txt"
#    echo "    $DIR/consumer.log"
    echo "    $DIR/producer.log"

    log "  - break time: 60 sec"
    sleep 60
done

echo "=== Experiment complete ==="
echo "result dir: $OUT"
