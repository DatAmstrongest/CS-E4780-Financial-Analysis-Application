import os
import glob
import datetime
import json
import sys
import time
from confluent_kafka import Producer, KafkaError

conf = {
    'bootstrap.servers': "kafka1:9092,kafka2:9092,kafka3:9092,kafka4:9092,kafka5:9092",
    # acks=all + idempotence: a write is only confirmed once min.insync.replicas brokers have it,
    # and retries after a broker failure cannot create duplicates or reorder a partition.
    'acks': 'all',
    'enable.idempotence': True,
    'compression.type': 'lz4',
    'linger.ms': 5,
}

DATE_TIME_FORMAT = "%H:%M:%S.%f" 
BATCH_SIZE = int(os.environ["BATCH_SIZE"])

delivered = 0
failed = 0


def on_delivery(err, msg):
    global delivered, failed
    if err is not None:
        failed += 1
        print(f"Delivery failed for key {msg.key()} on topic {msg.topic()}: {err}")
    else:
        delivered += 1


def produce(topic, key, value):
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
            break
        except BufferError:
            # Local queue is full; serve delivery callbacks to free space and retry.
            producer.poll(0.1)
    producer.poll(0)


def is_positive_number(value):
    try:
        return float(value) > 0
    except ValueError:
        return False


def send_error(stock_id, reason):
    value = f"{datetime.datetime.now()}: {reason}"
    produce("error", stock_id, value.encode("utf-8"))


# Create the Producer instance
producer = Producer(conf)


path = os.getcwd()
csv_files = sorted(glob.glob(os.path.join(path, "data", "*.csv")))

try:
    for csv_file in csv_files:
        with open(csv_file, 'r') as file:
            is_first_line = True
            counter = 1
            data_counter = 1
            for line in file:
                if line[0] == "#":
                    print("Comment line skipped.")
                    continue
                
                if is_first_line:
                    is_first_line = False
                    continue
                
                values = line.strip().split(",")  # Strip whitespace and split by comma

                stock_id = values[0]
                sec_type = values[1]
                last = values[21]
                # Falls back to the quote time when the trade time is missing; the reference
                # calculator in accuracy-testing/ applies the same rule, so keep them in sync.
                trading_time = values[23] or values[3]
                trading_date = values[26]

                # Prepare value based on validation
                if stock_id == '':
                    send_error(stock_id, "ID cannot be null")
                elif sec_type == '':
                    send_error(stock_id, "SecType cannot be null")
                elif last == '':
                    send_error(stock_id, "Last cannot be null")
                elif not is_positive_number(last):
                    # The CSVs contain "no trade yet" placeholder rows with Last=0 at 00:00:00;
                    # treating them as trades drags the EMAs towards 0.
                    send_error(stock_id, f"Last must be a positive number, got {last}")
                elif trading_time == '':
                    send_error(stock_id, "Trading time cannot be null")
                elif trading_date == '':
                    send_error(stock_id, "Trading date cannot be null")
                else:
                    current_datetime = datetime.datetime.now()
                    current_time = current_datetime.strftime('%H:%M:%S.%f')
                    current_date = current_datetime.strftime("%d/%m/%Y")

                    value = {"ID": stock_id, "SecType": sec_type, "Last": last, "Trading time": trading_time, "trading_date": trading_date, "current_time": current_time, "current_date": current_date}
                    produce("stocks", stock_id, json.dumps(value).encode('utf-8'))
                    data_counter += 1
                if data_counter%BATCH_SIZE==0:
                    time.sleep(1)
                    data_counter+=1
                if counter%BATCH_SIZE==0:
                    producer.flush()

                counter += 1
        producer.flush()
        print(f"{os.path.basename(csv_file)}: delivered={delivered} failed={failed}")

except KafkaError as e:
    print(f"Kafka error: {e}")

    print("Transaction aborted due to Kafka error.")
except Exception as e:
    print(f"General error: {e}")
    print("Transaction aborted due to general error.")

if failed:
    sys.exit(1)
