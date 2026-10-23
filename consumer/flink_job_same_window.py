import json
import logging
from datetime import datetime, timedelta

import psycopg2
from pyflink.common import Configuration, Duration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.datastream.connectors.kafka import (KafkaOffsetResetStrategy, KafkaOffsetsInitializer,
                                                 KafkaRecordSerializationSchema, KafkaSink, KafkaSource)
from pyflink.datastream.functions import FlatMapFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

from jdbc_compat import row_jdbc_sink

# Set up logging to write to the TaskManager log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlinkTaskManagerLog")

KAFKA_BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092,kafka4:9092,kafka5:9092"
WINDOW_MS = 5 * 60 * 1000
# Measured disorder in the replayed data is below 1 s; keep in sync with
# accuracy-testing/actual_ema_breakout_calculator.py.
OUT_OF_ORDERNESS = Duration.of_seconds(5)
CHECKPOINT_DIR = "file:///opt/flink/checkpoints"

EPOCH = datetime(1970, 1, 1)
MICROSECOND = timedelta(microseconds=1)

# (symbol, trading time in µs, price, producer send time in µs)
TICK_TYPE = Types.TUPLE([Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.LONG()])
EMA_TYPE = RowTypeInfo(
    [Types.INT(), Types.SQL_TIMESTAMP(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.SQL_TIMESTAMP()],
    ['stock_id', 'dt', 'prev_ema38', 'prev_ema100', 'ema38', 'ema100', 'latency_start'])
PRICE_TYPE = RowTypeInfo([Types.INT(), Types.SQL_TIMESTAMP(), Types.DOUBLE()], ['stock_id', 'dt', 'price'])
BREAKOUT_TYPE = RowTypeInfo(
    [Types.INT(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.SQL_TIMESTAMP()],
    ['stock_id', 'dt', 'breakout_type', 'latency_start'])

PRICE_TAG = OutputTag('stock-price', PRICE_TYPE)
BREAKOUT_TAG = OutputTag('breakout', BREAKOUT_TYPE)
BREAKOUT_EVENT_TAG = OutputTag('breakout-event', Types.STRING())
LATE_TICK_TAG = OutputTag('late-tick', Types.STRING())


def to_datetime(micros):
    return EPOCH + timedelta(microseconds=micros)


class ParseTick(FlatMapFunction):
    """Turns the producer's JSON into a compact tuple; malformed records are logged and skipped
    so that one bad message cannot put the job into a restart loop."""

    def flat_map(self, message):
        try:
            tick = json.loads(message)
            traded_at = datetime.strptime(f"{tick['trading_date']} {tick['Trading time']}", "%d-%m-%Y %H:%M:%S.%f")
            sent_at = datetime.strptime(f"{tick['current_date']} {tick['current_time']}", "%d/%m/%Y %H:%M:%S.%f")
            yield tick['ID'], (traded_at - EPOCH) // MICROSECOND, float(tick['Last']), (sent_at - EPOCH) // MICROSECOND
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Skipping malformed record {message!r}: {e}")


class EmaBreakoutFunction(KeyedProcessFunction):
    """Per symbol: buffers the prices of the open 5-minute windows, and when the watermark passes the
    end of a window, updates EMA38/EMA100 from the last price and checks for a crossover.

    All state lives in Flink keyed state, so it is part of every checkpoint and restored on failover.
    """

    def open(self, runtime_context: RuntimeContext):
        self.ema = runtime_context.get_state(
            ValueStateDescriptor('ema', Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()])))
        # trading time (µs) -> (sum of prices, number of ticks, send time of the first tick in µs)
        self.prices = runtime_context.get_map_state(
            MapStateDescriptor('prices', Types.LONG(), Types.TUPLE([Types.DOUBLE(), Types.LONG(), Types.LONG()])))
        metrics = runtime_context.get_metrics_group()
        self.late_ticks = metrics.counter('late_ticks')
        self.breakouts = metrics.counter('breakouts')
        self.symbol_id_map = self.load_symbol_id_map()

    @staticmethod
    def load_symbol_id_map():
        conn = psycopg2.connect(dbname="stocksdb", user="postgres", password="password", host="timescaledb",
                                port="5432")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT symbol, id FROM stock;")
            symbol_id_map = {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            conn.close()
        logger.info(f"Loaded {len(symbol_id_map)} symbols from the database.")
        return symbol_id_map

    def process_element(self, tick, ctx: 'KeyedProcessFunction.Context'):
        symbol, traded_at, price, sent_at = tick
        window_end = (traded_at // 1000 // WINDOW_MS + 1) * WINDOW_MS
        watermark = ctx.timer_service().current_watermark()
        if window_end - 1 <= watermark:
            self.late_ticks.inc()
            yield LATE_TICK_TAG, json.dumps({
                "symbol": symbol, "dt": str(to_datetime(traded_at)), "price": price,
                "watermark": str(EPOCH + timedelta(milliseconds=watermark)),
            })
            return

        entry = self.prices.get(traded_at)
        if entry is None:
            self.prices.put(traded_at, (price, 1, sent_at))
        else:
            self.prices.put(traded_at, (entry[0] + price, entry[1] + 1, entry[2]))
        ctx.timer_service().register_event_time_timer(window_end - 1)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        window_end = (timestamp + 1) * 1000
        closed = sorted(ts for ts in self.prices.keys() if ts < window_end)
        if not closed:
            return
        symbol = ctx.get_current_key()
        stock_id = self.symbol_id_map.get(symbol)
        averages = {}
        for ts in closed:
            total, count, sent_at = self.prices.get(ts)
            averages[ts] = (total / count, sent_at)
            self.prices.remove(ts)
        if stock_id is None:
            logger.warning(f"Stock ID for symbol {symbol} not found, dropping window ending {to_datetime(window_end)}.")
            return

        for ts, (price, _) in averages.items():
            yield PRICE_TAG, Row(stock_id, to_datetime(ts), price)

        last_ts = closed[-1]
        last_price, sent_at = averages[last_ts]
        prev_ema38, prev_ema100 = self.ema.value() or (0.0, 0.0)
        ema38 = calculate_ema(last_price, prev_ema38, 38)
        ema100 = calculate_ema(last_price, prev_ema100, 100)
        self.ema.update((ema38, ema100))

        dt = to_datetime(last_ts)
        latency_start = to_datetime(sent_at)
        yield Row(stock_id, dt, prev_ema38, prev_ema100, ema38, ema100, latency_start)

        if ema38 > ema100 and prev_ema38 <= prev_ema100:
            breakout_type = 'bull'
        elif ema38 < ema100 and prev_ema38 >= prev_ema100:
            breakout_type = 'bear'
        else:
            return
        self.breakouts.inc()
        yield BREAKOUT_TAG, Row(stock_id, dt, breakout_type, latency_start)
        yield BREAKOUT_EVENT_TAG, json.dumps({
            "symbol": symbol, "stock_id": stock_id, "dt": str(dt), "breakout_type": breakout_type,
            "ema38": ema38, "ema100": ema100, "prev_ema38": prev_ema38, "prev_ema100": prev_ema100,
        })


def calculate_ema(last_price, prev_ema, smoothing_factor):
    alpha = 2 / (1 + smoothing_factor)
    return (last_price * alpha) + (prev_ema * (1 - alpha))


def jdbc_sink(sql, type_info):
    return row_jdbc_sink(
        sql,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url("jdbc:postgresql://timescaledb:5432/stocksdb")
            .with_driver_name("org.postgresql.Driver")
            .with_user_name("postgres")
            .with_password("password")
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(200)
            .with_batch_size(1000)
            .build()
    )


def kafka_sink(topic):
    return KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_property('acks', 'all') \
        .build()


def process_kafka_stream():
    config = Configuration()
    # PyFlink buffers up to 1 s of records per bundle by default before handing them to Python.
    config.set_string('python.fn-execution.bundle.time', '100')
    # No attempt limit: replacing a lost task manager can take minutes, and a fixed budget
    # (e.g. 10 attempts x 10 s) runs out before the new one registers.
    config.set_string('restart-strategy.type', 'exponential-delay')
    config.set_string('restart-strategy.exponential-delay.initial-backoff', '1 s')
    config.set_string('restart-strategy.exponential-delay.max-backoff', '30 s')
    # Keeps the last checkpoint after a cancel so the job can be resubmitted from it with `flink run -s`.
    # (CheckpointConfig.set_externalized_checkpoint_retention points at a class missing from Flink 1.20.)
    config.set_string('execution.checkpointing.externalized-checkpoint-retention', 'RETAIN_ON_CANCELLATION')
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(4)
    env.set_buffer_timeout(10)

    env.enable_checkpointing(30000, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_storage_dir(CHECKPOINT_DIR)
    checkpoint_config.set_min_pause_between_checkpoints(10000)
    checkpoint_config.set_checkpoint_timeout(120000)

    # Offsets are committed back to Kafka when a checkpoint completes; on failover the source resumes
    # from the offsets stored in the checkpoint, together with the matching EMA state.
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics('stocks') \
        .set_group_id('flink-consumer') \
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Event time is the Kafka record timestamp (set by the producer to the trade time), so the source
    # tracks a watermark per partition and emits the minimum. A watermark per parallel instance would
    # follow its fastest partition, and ticks from slower partitions would be dropped as late.
    # No idleness timeout: symbols are hashed over all partitions, so no partition goes quiet on its own.
    ticks = env.from_source(kafka_source, WatermarkStrategy.for_bounded_out_of_orderness(OUT_OF_ORDERNESS),
                            'Kafka Source') \
        .uid('kafka-source') \
        .flat_map(ParseTick(), output_type=TICK_TYPE)

    emas = ticks \
        .key_by(lambda tick: tick[0], key_type=Types.STRING()) \
        .process(EmaBreakoutFunction(), output_type=EMA_TYPE) \
        .name('EMA and breakout detection') \
        .uid('ema-breakout')

    # The JDBC sink is at-least-once: after a failover, rows since the last checkpoint are written again,
    # so every insert must be idempotent.
    emas.add_sink(jdbc_sink(
        "INSERT INTO ema (stock_id, dt, prev_ema38, prev_ema100, ema38, ema100, latency_start) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (stock_id, dt) DO NOTHING",
        EMA_TYPE)).name('ema -> TimescaleDB')
    emas.get_side_output(PRICE_TAG).add_sink(jdbc_sink(
        "INSERT INTO stock_price (stock_id, dt, price) VALUES (?, ?, ?) ON CONFLICT (stock_id, dt) DO NOTHING",
        PRICE_TYPE)).name('stock_price -> TimescaleDB')
    emas.get_side_output(BREAKOUT_TAG).add_sink(jdbc_sink(
        "INSERT INTO breakouts (stock_id, dt, breakout_type, latency_start, latency_end) "
        "VALUES (?, ?, ?, ?, NOW()) ON CONFLICT (stock_id, dt) DO NOTHING",
        BREAKOUT_TYPE)).name('breakouts -> TimescaleDB')
    emas.get_side_output(BREAKOUT_EVENT_TAG).sink_to(kafka_sink('breakouts')).name('breakouts -> Kafka')
    emas.get_side_output(LATE_TICK_TAG).sink_to(kafka_sink('late-ticks')).name('late ticks -> Kafka')

    env.execute("Python Flink Kafka Stream Processing with EMA Calculation")


if __name__ == '__main__':
    process_kafka_stream()
