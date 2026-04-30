# EXT4(CNS), F2FS(CNS)에 대한 Kafka Broker의 I/O 성능 비교 분석

## 이동혁(2021086917), 최현준(2021037401)

---

## Kafka 설치 및 준비

### Java 설치

```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
java -version
```

Kafka 버전:

```bash
kafka_2.13-4.2.0
```

설치:

```bash
scp -r .\kafka_2.13-4.2.0.tgz h@192.168.0.23:~/

tar -zxvf kafka_2.13-4.2.0.tgz
cd ~/kafka_2.13-4.2.0
```

---

### Kafka log directory 준비

```bash
sudo mkdir -p /mnt/ext4/kafka-logs
sudo chown -R $USER:$USER /mnt/ext4/kafka-logs

sudo mkdir -p /mnt/f2fs/kafka-logs
sudo chown -R $USER:$USER /mnt/f2fs/kafka-logs
```

---

### Kafka 설정

```bash
vi ~/kafka_2.13-4.2.0/config/server.properties
```

기본 설정:

```properties
num.partitions=8
offsets.topic.replication.factor=1
log.dirs=/mnt/ext4/kafka-logs
```

---

### 메시지 크기 관련 설정

```properties
message.max.bytes=20971520
replica.fetch.max.bytes=20971520
max.request.size=20971520
fetch.max.bytes=52428800
max.partition.fetch.bytes=20971520
```

---

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

### 의존성 설치

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

## 실행

```bash
IOSTAT_DEV=nvme0n1 ./run_all.sh
```

---

# 📌 Experiment Methodology

## 실험 구성

```text
File System: ext4, f2fs
Scenario: producer_only, producer_consumer
Payload: 1KB ~ 1MB
```

---

## 실험 단계

### Kafka 초기화

```text
- topic 삭제 후 재생성
- log.dirs 변경
- storage format
```

---

### 메트릭 수집

```bash
iostat -dx 1 -y -t -o JSON
vmstat 1 -t
```

---

### Consumer 실행 (optional)

```text
producer_consumer일 경우 선실행
```

---

### Producer 실행

```text
target_mps = linear ramp-up
```

Phase:

```text
Warmup → Measurement
```

---

## Throughput 제어

```text
MPS = BPS / payload_size
```

예시:

```bash
INITIAL_BPS=${INITIAL_BPS:-92160000}   # 90 MB/s-ish
INCR_BPS=${INCR_BPS:-10240000}           # 10 MBPS Increment
MAX_BPS=${MAX_BPS:-153600000}          # 150 MB/s-ish
```

---

# 결과 분석

## 디렉토리 구조

```text
results/
  ext4/
    producer_consumer/
      timestamp/
        payload/
  f2fs/
    producer_only/
```

---

## Producer.jsonl

```json
{
  "timestamp_us": 1714123456789,
  "scenario": "producer_consumer",
  "payload_size": 10240,
  "target_mps": 1000,
  "actual_mps": 980.5,
  "latency_avg_us": 2100,
  "latency_p50_us": 1800,
  "latency_p90_us": 3500,
  "latency_p99_us": 9000,
  "sent_count": 30000,
  "acked_count": 29415,
  "duration_sec": 30,
  "state": "measurement"
}
```

---

## Consumer.jsonl

```json
{
  "timestamp_us": 1777271047670577,
  "payload_size": 10240,
  "consume_mps": 10000.12,
  "consumed_count": 10042,
  "state": "running"
}
```

---

## vmstat

```text
1777269637  3  0  256 1205628 94516 4877768 0 0 171 21981 1513 5 8 2 81 9 0 0
```

---

## iostat

```json
{
  "timestamp": "04/27/2026 06:10:22 AM",
  "disk": [
    {
      "disk_device": "nvme0n1",
      "w/s": 672.00,
      "wkB/s": 82156.00,
      "util": 99.00
    }
  ]
}
```
