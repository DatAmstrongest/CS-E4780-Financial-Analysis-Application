"""Offline reference for the Flink job in consumer/flink_job_same_window.py.

Replays the CSVs with the same rules as the producer and the Flink job:
- rows are validated like producer/main.py (non-empty fields, Last > 0, Time fallback)
- ticks go to 5-minute tumbling windows by their own trading timestamp
- per window and symbol, the price is the average of all prices at the latest timestamp
- a window is evaluated once data OUT_OF_ORDERNESS past its end has been seen
  (the Flink watermark); ticks arriving after that are dropped and counted as late
"""
import argparse
import glob
import os
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg2

WINDOW = timedelta(minutes=5)
OUT_OF_ORDERNESS = timedelta(seconds=100)
DATA_PATH = "../producer/data"
BREAKOUT_PATH = "./actual/breakout.csv"
EMA_PATH = "./actual/ema.csv"


def load_symbol_id_map():
    conn = psycopg2.connect(
        dbname="stocksdb",
        user="postgres",
        password="password",
        host="localhost",
        port="5433"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, id FROM stock;")
    symbol_id_map = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    return symbol_id_map


def parse_tick(line):
    """Returns (symbol, price, timestamp), or None for rows the producer does not send to Kafka."""
    values = line.rstrip("\n").split(",")
    if len(values) < 27:
        return None
    symbol, sec_type, price = values[0], values[1], values[21]
    trading_time = values[23] or values[3]
    trading_date = values[26]
    if not (symbol and sec_type and price and trading_time and trading_date):
        return None
    try:
        price = float(price)
    except ValueError:
        return None
    if price <= 0:
        return None
    ts = datetime.strptime(f"{trading_date.strip()} {trading_time.strip()}", "%d-%m-%Y %H:%M:%S.%f")
    return symbol, price, ts


def window_start_of(ts):
    return ts.replace(minute=ts.minute - ts.minute % 5, second=0, microsecond=0)


def calculate_ema(price, prev_ema, smoothing_factor):
    alpha = 2 / (1 + smoothing_factor)
    return (price * alpha) + (prev_ema * (1 - alpha))


class ReferenceCalculator:
    def __init__(self, symbols, ema_file, breakout_file):
        self.symbols = symbols
        self.ema_file = ema_file
        self.breakout_file = breakout_file
        # window start -> symbol -> [latest timestamp, prices at that timestamp]
        self.open_windows = defaultdict(dict)
        self.emas = {}
        self.max_ts = None
        self.closed_until = None
        self.late_ticks = 0
        self.unknown_symbols = set()
        self.ema_rows = 0
        self.breakouts = 0

    def add(self, symbol, price, ts):
        start = window_start_of(ts)
        if self.closed_until is not None and start < self.closed_until:
            self.late_ticks += 1
            return
        window = self.open_windows[start]
        latest = window.get(symbol)
        if latest is None or ts > latest[0]:
            window[symbol] = [ts, [price]]
        elif ts == latest[0]:
            latest[1].append(price)

        if self.max_ts is None or ts > self.max_ts:
            self.max_ts = ts
            self.close_ready_windows()

    def close_ready_windows(self):
        watermark = self.max_ts - OUT_OF_ORDERNESS
        while self.open_windows:
            start = min(self.open_windows)
            if start + WINDOW > watermark:
                break
            self.close_window(self.open_windows.pop(start))
            self.closed_until = start + WINDOW

    def close_window(self, window):
        for symbol, (ts, prices) in window.items():
            stock_id = self.symbols.get(symbol)
            if stock_id is None:
                self.unknown_symbols.add(symbol)
                continue
            price = sum(prices) / len(prices)
            prev_ema38, prev_ema100 = self.emas.get(symbol, (0, 0))
            ema38 = calculate_ema(price, prev_ema38, 38)
            ema100 = calculate_ema(price, prev_ema100, 100)
            self.emas[symbol] = (ema38, ema100)

            dt = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
            self.ema_file.write(f"{stock_id},{dt},{prev_ema38},{prev_ema100},{ema38},{ema100}\n")
            self.ema_rows += 1
            if ema38 > ema100 and prev_ema38 <= prev_ema100:
                self.breakout_file.write(f"{stock_id},{dt},bull\n")
                self.breakouts += 1
            elif ema38 < ema100 and prev_ema38 >= prev_ema100:
                self.breakout_file.write(f"{stock_id},{dt},bear\n")
                self.breakouts += 1


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--until", help='stop after this trading time, e.g. "2021-11-08 12:00"; '
                                        "useful when the system has only replayed part of the data")
    args = parser.parse_args()
    until = datetime.fromisoformat(args.until) if args.until else None

    os.makedirs(os.path.dirname(BREAKOUT_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(EMA_PATH), exist_ok=True)

    with open(EMA_PATH, "w") as ema_file, open(BREAKOUT_PATH, "w") as breakout_file:
        ema_file.write("stock_id,dt,prev_ema38,prev_ema100,ema38,ema100\n")
        breakout_file.write("stock_id,dt,breakout_type\n")
        calculator = ReferenceCalculator(load_symbol_id_map(), ema_file, breakout_file)

        for csv_file in sorted(glob.glob(os.path.join(DATA_PATH, "*.csv"))):
            print(f"Reading {os.path.basename(csv_file)}")
            with open(csv_file, "r") as file:
                for line in file:
                    if line.startswith("#"):
                        continue
                    tick = parse_tick(line)
                    if tick is None:
                        continue
                    calculator.add(*tick)
                    if until is not None and calculator.max_ts - OUT_OF_ORDERNESS >= until:
                        break
            if until is not None and calculator.max_ts is not None and calculator.max_ts - OUT_OF_ORDERNESS >= until:
                break

    print(f"EMA rows: {calculator.ema_rows}, breakouts: {calculator.breakouts}, "
          f"late ticks dropped: {calculator.late_ticks}, "
          f"windows left open at end of data: {len(calculator.open_windows)}, "
          f"symbols missing from the stock table: {len(calculator.unknown_symbols)}")


if __name__ == "__main__":
    main()
