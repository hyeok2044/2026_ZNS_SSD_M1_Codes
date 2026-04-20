#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
TOPIC=${TOPIC:-ext4-test}
OPS=${OPS:-200000}
TOTAL_BYTES=${TOTAL_BYTES:-$((50 * 1024 * 1024 * 1024))}   # 50 GiB per payload
KAFKA_BIN=${KAFKA_BIN:-$HOME/kafka_2.13-4.2.0/bin}
GRACE=${GRACE:-10}

OUT="$(date +%Y%m%d_%H%M%S)_exp_result"
mkdir -p "$OUT"

for PAYLOAD in 1024 10240 102400 1024000; do
    MSG=$((TOTAL_BYTES / PAYLOAD))
    DIR="$OUT/$PAYLOAD"
    mkdir -p "$DIR"
    echo "=== payload=$PAYLOAD msg_count=$MSG ==="

    # 1. 토픽 지우고 다시 생성
    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" --if-exists
    sleep 3
    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --topic "$TOPIC" --partitions 8 --replication-factor 1
    sleep 30

    # 2. 성능 측정 도구 실행
    iostat 1 -y -x -t -o JSON > /tmp/iostat.json &
    IO_PID=$!
    vmstat 1 -y > /tmp/vmstat.txt &
    VM_PID=$!

    # 3. Consumer/Producer 실행
    ./consumer --bootstrap-servers "$BOOTSTRAP" --topic "$TOPIC" > "$DIR/consumer.log" 2>&1 & CO_PID=$!
    ./producer --bootstrap-servers "$BOOTSTRAP" --topic "$TOPIC" --payload-size "$PAYLOAD" --message-count "$MSG" --ops "$OPS" > "$DIR/producer.log" 2>&1
    sleep "$GRACE"

    # 4. 성능 측정 도구 종료 및 결과 저장
    kill -INT "$CO_PID" 2>/dev/null || true
    kill -TERM "$IO_PID" "$VM_PID" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
        kill -0 "$CO_PID" 2>/dev/null \
            || kill -0 "$IO_PID" 2>/dev/null \
            || kill -0 "$VM_PID" 2>/dev/null \
            || break
        sleep 1
    done
    kill -KILL "$CO_PID" "$IO_PID" "$VM_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    mv /tmp/iostat.json /tmp/vmstat.txt "$DIR/"

    echo "실험 완료. 60초 대기 후 다음 실험을 진행합니다."
    sleep 60
done

echo "→ $OUT"
