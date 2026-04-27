# EXT4(CNS), F2FS(CNS)에 대한 Kafka Broker의 I/O 성능 비교 분석

## 이동혁(2021086917), 최현준(2021037401)

## Kafka 설치 및 준비

### Java 설치

```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
java -version
```

Kafka는 `4.2.0` 버전을 사용한다.

```bash
scp -r .\kafka_2.13-4.2.0.tgz h@192.168.0.23:~/

tar -zxvf kafka_2.13-4.2.0.tgz
cd ~/kafka_2.13-4.2.0
```

### Kafka log directory 준비

```bash
sudo mkdir -p /mnt/ext4/kafka-logs
sudo chown -R $USER:$USER /mnt/ext4/kafka-logs

sudo mkdir -p /mnt/f2fs/kafka-logs
sudo chown -R $USER:$USER /mnt/f2fs/kafka-logs
```

### Kafka 설정

```bash
vi ~/kafka_2.13-4.2.0/config/server.properties
```

필수 설정:

```properties
num.partitions=8
offsets.topic.replication.factor=1
log.dirs=/mnt/ext4/kafka-logs
```

`log.dirs`는 실험 대상 파일 시스템에 맞춰 매번 변경된다.

### 추가: Broker 메시지 제한 관련

```c
// ~/kafka_2.13-4.2.0/config/broker.properties

message.max.bytes=2097152
replica.fetch.max.bytes=2097152
```

아래에 append

```c
// ~/kafka_2.13-4.2.0/config/producer.properties
# Maximum size of a request in bytes.
# Should accommodate your largest batch size plus overhead.
# 1MB is default and suitable for most cases.
max.request.size=1048576정

max.request.size=2097152
```

아래 처럼 수정

```c
// ~/kafka_2.13-4.2.0/config/consumer.properties
# Set soft limits to the amount of bytes per fetch request and partition.
# Both max.partition.fetch.bytes and fetch.max.bytes limits can be exceeded when
# the first batch in the first non-empty partition is larger than the configured
# value to ensure that the consumer can make progress.
# Configuring message.max.bytes (broker config) or max.message.bytes (topic config)
# <= fetch.max.bytes prevents oversized fetch responses.
fetch.max.bytes=52428800
max.partition.fetch.bytes=1048576

max.partition.fetch.bytes=2097152
```

아래 처럼 수정

다음과 같이 바꾸어야 1MB 로드도 성공적으로 처리할 수 있다.

### Kafka storage format

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

~/kafka_2.13-4.2.0/bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c ~/kafka_2.13-4.2.0/config/server.properties
```

---

## 부하 생성기 빌드

### rdkafka / glib 설치

```bash
sudo apt install librdkafka-dev pkg-config libglib2.0-dev build-essential -y
```

### 코드 다운로드

```bash
git clone https://github.com/hyeok2044/2026_ZNS_SSD_M1_Codes
cd 2026_ZNS_SSD_M1_Codes
```

### Build

```bash
make clean
make
```

---

## Run

```bash
# optional: iostat 대상 device 지정
IOSTAT_DEV=nvme0n1 ./run_all.sh
```

---

# 📌 Experiment Methodology

## 1. 전체 실험 구조

본 실험은 Kafka 기반에서 파일 시스템(ext4, f2fs)의 성능을 비교하기 위해 다음과 같은 구조로 수행된다.

- File System: `ext4`, `f2fs`
- Scenario:
  - `producer_only`
  - `producer_consumer`

- Payload Size:
  - 1024B, 10240B, 102400B, 1024000B

- Throughput Control:
  - BPS → MPS 변환 기반 linear ramp-up

---

## 2. 실험 단계

각 payload에 대해 아래 단계를 반복 수행한다.

### (1) Kafka 초기화

- 기존 topic 삭제 후 재생성
- log.dirs를 대상 파일 시스템으로 설정
- Kafka storage format 수행
- clean state 보장

---

### (2) 시스템 메트릭 수집 시작

다음 시스템 지표를 수집한다:

- iostat
- vmstat

```
iostat -dx 1 -y -t -o JSON
vmstat 1
```

---

### (3) Consumer 실행 (optional)

`producer_consumer` 시나리오일 경우:

- consumer를 먼저 실행
- producer 시작 전에 안정화 대기 (3초)

---

### (4) Producer 실행 (핵심)

Producer는 다음 방식으로 부하를 생성한다:

#### 🔹 Ramp-up 방식

```
target_mps = initial → max (linear 증가)
```

각 step마다:

```
warmup phase → measurement phase
```

#### 🔹 Phase 정의

- Warmup:
  - cache, buffer 안정화
  - 측정 제외

- Measurement:
  - 실제 throughput 측정
  - JSONL로 결과 기록

---

### (5) Producer 출력

각 measurement phase마다 다음을 기록:

```
{
  "timestamp_us": ...,
  "payload_size": ...,
  "target_mps": ...,
  "actual_mps": ...,
  "sent_count": ...,
  "acked_count": ...,
  "duration_sec": ...,
  "state": "measurement"
}
```

---

### (6) Consumer 출력

Consumer는 ramp-up에 관여하지 않고, 다음을 지속적으로 기록:

```
{
  "timestamp_us": ...,
  "payload_size": ...,
  "consume_mps": ...,
  "consumed_count": ...,
  "state": "running"
}
```

---

### (7) 종료 및 정리

- Producer 종료 후 grace period
- Consumer 종료
- iostat / vmstat 종료
- 다음 payload로 이동

---

## 3. Throughput 제어 방식

실험은 MPS가 아닌 **BPS 기반으로 정의**된다.

```
MPS = BPS / payload_size
```

예시:

```
INITIAL_BPS=71680000   # 70 MB/s
INCR_BPS=5120000       # 5 MB/s
MAX_BPS=122880000      # 120 MB/s
```

→ payload에 따라 자동으로 MPS 변환

---

## 4. Saturation 판단 기준

다음 조건 중 하나를 만족하면 saturation으로 간주:

- actual_mps < target_mps \* 0.95
- I/O wait 증가 (vmstat wa 상승)
- disk util ≈ 100% (iostat)
- write latency 증가 (await 증가)

---

## 5. 결과 저장 구조

```
results/
  ext4/
    producer_only/
      timestamp/
        payload/
  f2fs/
    producer_consumer/
      timestamp/
        payload/
```

---

## 6. 핵심 설계 원칙

- 모든 실험은 clean state에서 시작
- throughput은 BPS 기준으로 통일
- producer는 load generator
- consumer는 observer
- JSONL 기반 결과 저장

---

# 한 줄 요약

→ Producer가 linear ramp-up으로 부하를 증가시키고, Consumer 및 시스템 지표를 통해 saturation 지점을 관측하는 실험 구조
