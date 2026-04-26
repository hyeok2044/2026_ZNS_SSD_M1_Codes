#!/usr/bin/env bash
set -euo pipefail

./kafka_storage_transition.sh ext4 /mnt/ext4/kafka-logs
./run_experiment.sh ext4 /mnt/ext4/kafka-logs

./kafka_storage_transition.sh f2fs /mnt/f2fs/kafka-logs
./run_experiment.sh f2fs /mnt/f2fs/kafka-logs
