#!/bin/bash
set -euo pipefail

BOOTSTRAP=kafka1:9092

# Replication factor 3 with min.insync.replicas=2: acks=all writes keep succeeding with one
# broker down, and an acknowledged record always exists on at least two brokers.
kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic stocks --partitions 10 --replication-factor 3
# Rows the producer rejects, one JSON record each (~11M per trading day, mostly prices without a
# trading date). They are best-effort logs written with acks=1, so replication factor 2 is enough.
kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic rejected-rows --partitions 3 --replication-factor 2
# Written by the Flink job. A single partition keeps all breakouts in order; the volume is a few
# events per symbol per day, so one partition is far from a bottleneck.
kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic breakouts --partitions 1 --replication-factor 3
kafka-topics --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic late-ticks --partitions 1 --replication-factor 3

# Applied with --alter so re-runs also update topics that already exist.
# stocks records carry the 2021 trade time as their Kafka timestamp (event time for Flink), and
# time-based retention compares record timestamps with the wall clock, so it would delete every
# closed segment immediately. Retention is therefore size-only (64 MB per partition), because a
# full replay is ~23 GB of CSV and Kafka shares the Docker disk.
kafka-configs --bootstrap-server $BOOTSTRAP --alter --entity-type topics --entity-name stocks \
  --add-config min.insync.replicas=2,retention.ms=-1,retention.bytes=67108864,segment.bytes=16777216
# Capped at 1 day and 256 MB per partition (stored lz4-compressed) so the logs cannot fill the disk.
kafka-configs --bootstrap-server $BOOTSTRAP --alter --entity-type topics --entity-name rejected-rows \
  --add-config retention.ms=86400000,retention.bytes=268435456,segment.bytes=67108864
# Flink's Kafka sink stamps records with their event time; LogAppendTime replaces that with the
# broker's clock so 7-day retention means 7 days after the event was produced.
for topic in breakouts late-ticks; do
  kafka-configs --bootstrap-server $BOOTSTRAP --alter --entity-type topics --entity-name $topic \
    --add-config min.insync.replicas=2,retention.ms=604800000,message.timestamp.type=LogAppendTime
done

kafka-topics --bootstrap-server $BOOTSTRAP --describe --topic stocks
