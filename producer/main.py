import os
import glob
import datetime
import json
import sys
import time
from collections import Counter
from confluent_kafka import Producer, KafkaError

BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092,kafka4:9092,kafka5:9092"

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    # acks=all + idempotence: a write is only confirmed once min.insync.replicas brokers have it,
    # and retries after a broker failure cannot create duplicates or reorder a partition.
    'acks': 'all',
    'enable.idempotence': True,
    'compression.type': 'lz4',
    'linger.ms': 5,
}

# Rejected rows are best-effort logs (~11M per trading day), so they go through their own producer
# with acks=1 and large batches instead of slowing down the acks=all path for trades.
rejected_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 1,
    'compression.type': 'lz4',
    'linger.ms': 100,
    'queue.buffering.max.messages': 1000000,
}

BATCH_SIZE = int(os.environ["BATCH_SIZE"])
PROGRESS_EVERY = 1000000
EPOCH = datetime.datetime(1970, 1, 1)

delivered = 0
failed = 0
rejected_logged = 0
rejected_log_failed = 0


def on_delivery(err, msg):
    global delivered, failed
    if err is not None:
        failed += 1
        print(f"Delivery failed for key {msg.key()} on topic {msg.topic()}: {err}")
    else:
        delivered += 1


def on_rejected_delivery(err, msg):
    global rejected_logged, rejected_log_failed
    if err is not None:
        rejected_log_failed += 1
    else:
        rejected_logged += 1


def produce(kafka_producer, topic, key, value, callback, timestamp=0):
    while True:
        try:
            kafka_producer.produce(topic, key=key, value=value, timestamp=timestamp, on_delivery=callback)
            break
        except BufferError:
            # Local queue is full; serve delivery callbacks to free space and retry.
            kafka_producer.poll(0.1)
    kafka_producer.poll(0)


def is_positive_number(value):
    try:
        return float(value) > 0
    except ValueError:
        return False


def event_time_ms(trading_date, trading_time):
    traded_at = datetime.datetime.strptime(f"{trading_date} {trading_time}", "%d-%m-%Y %H:%M:%S.%f")
    return (traded_at - EPOCH) // datetime.timedelta(milliseconds=1)


def log_rejected(reason, message, file_name, line_no, values):
    record = {
        "logged_at": datetime.datetime.now().isoformat(),
        "reason": reason,
        "message": message,
        "file": file_name,
        "line": line_no,
        "symbol": values[0],
        "sec_type": values[1],
        "date": values[2],
        "time": values[3],
        "last": values[21],
        "trading_time": values[23],
        "trading_date": values[26],
    }
    produce(rejected_producer, "rejected-rows", values[0] or None, json.dumps(record).encode("utf-8"),
            on_rejected_delivery)


def validate(values):
    """Returns None for a usable trade, "no_trade" for rows without a trade price, or (reason, message)."""
    stock_id, sec_type, last = values[0], values[1], values[21]
    # Falls back to the quote time when the trade time is missing; the reference
    # calculator in accuracy-testing/ applies the same rule, so keep them in sync.
    trading_time = values[23] or values[3]
    trading_date = values[26]
    if stock_id == '':
        return "missing_id", "ID cannot be null"
    if sec_type == '':
        return "missing_sec_type", "SecType cannot be null"
    if last == '':
        # Bid/ask, volume-only and empty updates: most of the CSV rows. They carry no trade, so they
        # are only counted; logging each one made the producer the bottleneck.
        return "no_trade"
    if not is_positive_number(last):
        # The CSVs contain "no trade yet" placeholder rows with Last=0 at 00:00:00;
        # treating them as trades drags the EMAs towards 0.
        return "non_positive_price", f"Last must be a positive number, got {last}"
    if trading_time == '':
        return "missing_trading_time", "Trading time cannot be null"
    if trading_date == '':
        return "missing_trading_date", "Trading date cannot be null"
    return None


def print_summary(event, file_name, row_counts):
    print(json.dumps({"event": event, "file": file_name, "rows": dict(row_counts), "delivered": delivered,
                      "failed": failed, "rejected_logged": rejected_logged,
                      "rejected_log_failed": rejected_log_failed}))


producer = Producer(conf)
rejected_producer = Producer(rejected_conf)

path = os.getcwd()
csv_files = sorted(glob.glob(os.path.join(path, "data", "*.csv")))

try:
    for csv_file in csv_files:
        file_name = os.path.basename(csv_file)
        row_counts = Counter()
        with open(csv_file, 'r') as file:
            is_first_line = True
            counter = 1
            data_counter = 1
            for line_no, line in enumerate(file, start=1):
                if line[0] == "#":
                    continue

                if is_first_line:
                    is_first_line = False
                    continue

                values = line.strip().split(",")  # Strip whitespace and split by comma
                problem = validate(values)

                if problem == "no_trade":
                    row_counts["no_trade"] += 1
                elif problem is not None:
                    reason, message = problem
                    row_counts[reason] += 1
                    log_rejected(reason, message, file_name, line_no, values)
                else:
                    row_counts["trade"] += 1
                    stock_id, sec_type, last = values[0], values[1], values[21]
                    trading_time = values[23] or values[3]
                    trading_date = values[26]
                    current_datetime = datetime.datetime.now()
                    current_time = current_datetime.strftime('%H:%M:%S.%f')
                    current_date = current_datetime.strftime("%d/%m/%Y")

                    value = {"ID": stock_id, "SecType": sec_type, "Last": last, "Trading time": trading_time, "trading_date": trading_date, "current_time": current_time, "current_date": current_date}
                    # The trade time becomes the Kafka record timestamp, so the Flink Kafka source can
                    # track event time per partition (see consumer/flink_job_same_window.py).
                    produce(producer, "stocks", stock_id, json.dumps(value).encode('utf-8'), on_delivery,
                            event_time_ms(trading_date, trading_time))
                    data_counter += 1
                if data_counter%BATCH_SIZE==0:
                    time.sleep(1)
                    data_counter+=1
                if counter%BATCH_SIZE==0:
                    producer.flush()
                if counter % PROGRESS_EVERY == 0:
                    print_summary("progress", file_name, row_counts)

                counter += 1
        producer.flush()
        rejected_producer.flush()
        print_summary("file_done", file_name, row_counts)

except KafkaError as e:
    print(f"Kafka error: {e}")

    print("Transaction aborted due to Kafka error.")
except Exception as e:
    print(f"General error: {e}")
    print("Transaction aborted due to general error.")

if failed:
    sys.exit(1)
