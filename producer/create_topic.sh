#!/bin/bash
set -euo pipefail

BOOTSTRAP=kafka1:9092

# Replication factor 3 with min.insync.replicas=2: acks=all writes keep succeeding with one
# broker down, and an acknowledged record always exists on at least two brokers.
kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic stocks --partitions 10 --replication-factor 3
kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic error --partitions 1 --replication-factor 3

# Applied with --alter so re-runs also update topics that already exist. Retention is capped by
# both time (how long Flink may lag or be down without losing data) and size per partition,
# because a full replay is ~23 GB of CSV and Kafka shares the Docker disk.
kafka-configs --bootstrap-server $BOOTSTRAP --alter --entity-type topics --entity-name stocks \
  --add-config min.insync.replicas=2,retention.ms=900000,retention.bytes=67108864,segment.bytes=16777216
kafka-configs --bootstrap-server $BOOTSTRAP --alter --entity-type topics --entity-name error \
  --add-config min.insync.replicas=2

kafka-topics --bootstrap-server $BOOTSTRAP --describe --topic stocks
