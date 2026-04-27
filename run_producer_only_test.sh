#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
TOPIC=${TOPIC:-producer-test}
SCENARIO=${SCENARIO:-producer_only}

PAYLOAD=${PAYLOAD:-1024}

INITIAL_BPS=${INITIAL_BPS:-102400000}     # 100 * 1024000
INCR_BPS=${INCR_BPS:-102400000}           # 100 * 1024000
MAX_BPS=${MAX_BPS:-512000000}             # 500 * 1024000, 단독 테스트라 낮게

WARMUP_SEC=${WARMUP_SEC:-5}
MEASUREMENT_SEC=${MEASUREMENT_SEC:-10}

OUT=${OUT:-results/producer_only_test/$(date +%Y%m%d_%H%M%S)}
mkdir -p "$OUT"

INITIAL_MPS=$((INITIAL_BPS / PAYLOAD))
INCR_MPS=$((INCR_BPS / PAYLOAD))
MAX_MPS=$((MAX_BPS / PAYLOAD))

[ "$INITIAL_MPS" -lt 1 ] && INITIAL_MPS=1
[ "$INCR_MPS" -lt 1 ] && INCR_MPS=1
[ "$MAX_MPS" -lt 1 ] && MAX_MPS=1

echo "=== Producer only test ==="
echo "bootstrap      : $BOOTSTRAP"
echo "topic          : $TOPIC"
echo "scenario       : $SCENARIO"
echo "payload        : $PAYLOAD"
echo "initial_bps    : $INITIAL_BPS"
echo "incr_bps       : $INCR_BPS"
echo "max_bps        : $MAX_BPS"
echo "initial_mps    : $INITIAL_MPS"
echo "incr_mps       : $INCR_MPS"
echo "max_mps        : $MAX_MPS"
echo "warmup_sec     : $WARMUP_SEC"
echo "measurement_sec: $MEASUREMENT_SEC"
echo "out            : $OUT"
echo

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
  > "$OUT/producer.log" 2>&1

echo "producer finished"
echo "log: $OUT/producer.log"
