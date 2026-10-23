CREATE EXTENSION IF NOT EXISTS timescaledb;
SET datestyle = 'ISO, DMY';

CREATE TABLE stock (
	id SERIAL PRIMARY KEY,
	symbol TEXT UNIQUE NOT NULL,
	security_type TEXT NOT NULL
);

CREATE TABLE ema (
	stock_id INTEGER NOT NULL,
	dt TIMESTAMP NOT NULL,
	prev_ema38 DOUBLE PRECISION,
	prev_ema100 DOUBLE PRECISION,
	ema38 DOUBLE PRECISION,
	ema100 DOUBLE PRECISION,
	latency_start TIMESTAMP NOT NULL,
	PRIMARY KEY (stock_id, dt),
	CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stock (id)
);

CREATE TABLE stock_price (
	stock_id INTEGER NOT NULL,
	dt TIMESTAMP NOT NULL,
	price DOUBLE PRECISION NOT NULL,
	PRIMARY KEY (stock_id, dt),
	CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stock (id)
);

CREATE TABLE breakouts (
	id SERIAL,
	stock_id INTEGER NOT NULL,
	dt TIMESTAMP NOT NULL,
	breakout_type TEXT NOT NULL,
	latency_start TIMESTAMP NOT NULL,
	latency_end TIMESTAMP NOT NULL,
	PRIMARY KEY (stock_id, dt),
	CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stock (id)
);

CREATE UNIQUE INDEX ON ema (stock_id, dt);
CREATE INDEX ON breakouts(stock_id, dt);
CREATE UNIQUE INDEX ON stock_price (stock_id, dt);

SELECT create_hypertable('stock_price', 'dt');
SELECT create_hypertable('ema', 'dt');
SELECT create_hypertable('breakouts', 'dt');
SELECT set_chunk_time_interval('ema', INTERVAL '1 day');
-- No retention policies: they drop chunks older than NOW() - interval, and the replayed data is
-- from 2021, so they deleted rows minutes after they were written.

-- Breakouts are detected in the Flink job (consumer/flink_job_same_window.py) and inserted
-- directly; latency_end is set by the insert's NOW().

-- View for breakouts, can be updated as needed FIXME
/*DROP MATERIALIZED VIEW IF EXISTS breakout_summary;
CREATE MATERIALIZED VIEW breakout_summary
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('5 minutes', dt) AS bucket,
           stock_id,
           COUNT(*) FILTER (WHERE breakout_type = 'bull') AS bull_breakouts,
           COUNT(*) FILTER (WHERE breakout_type = 'bear') AS bear_breakouts
    FROM breakouts
    GROUP BY bucket, stock_id;
SELECT add_continuous_aggregate_policy('breakout_summary', start_offset => INTERVAL '5 minutes');*/
