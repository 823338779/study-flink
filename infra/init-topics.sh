#!/bin/bash
set -euo pipefail
for topic in payment-events shop-payment-totals; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$topic" --partitions 2 --replication-factor 1
  description=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "$topic")
  printf '%s\n' "$description"
  if [[ ! "$description" =~ PartitionCount:\ 2[[:space:]] ]]; then
    printf 'Expected exactly 2 partitions for %s\n' "$topic" >&2
    exit 1
  fi
done
