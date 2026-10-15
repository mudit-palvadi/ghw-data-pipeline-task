-- ============================================================
-- Real-time financial data -> Tiger Cloud
-- Schema for the ingestion pipeline (challenge: "Ingest real-time
-- financial data into Tiger Cloud", GHW: Hacktoberfest 2026)
--
-- Run against your Tiger Cloud service. Matches the tables used by
-- the Tiger Data tutorial:
-- https://www.tigerdata.com/docs/build/examples/ingest-real-time-financial-data
-- ============================================================

-- 1. Hypertable for the raw ticks -------------------------------------------
--    One row per trade / price update received from the websocket.
--    `segmentby = symbol` groups rows by trading pair inside chunks;
--    `orderby = time DESC` keeps recent data first for time-range queries.
CREATE TABLE IF NOT EXISTS crypto_ticks (
    "time"     TIMESTAMPTZ      NOT NULL,
    symbol     TEXT             NOT NULL,
    price      DOUBLE PRECISION NOT NULL,
    day_volume NUMERIC          NULL
) WITH (
    tsdb.hypertable,
    tsdb.segmentby = 'symbol',
    tsdb.orderby   = 'time DESC'
);

-- High-frequency tick data: one chunk per day keeps recent-chunk indexes hot.
SELECT set_chunk_time_interval('crypto_ticks', INTERVAL '1 day');

-- Query pattern: WHERE symbol = 'X' AND time > ... ORDER BY time DESC
CREATE INDEX IF NOT EXISTS idx_crypto_ticks_symbol_time
    ON crypto_ticks (symbol, "time" DESC);

-- 2. Relational table for asset metadata -------------------------------------
CREATE TABLE IF NOT EXISTS crypto_assets (
    symbol TEXT UNIQUE,
    "name" TEXT
);

INSERT INTO crypto_assets (symbol, "name") VALUES
    ('BTC/USD',  'Bitcoin'),
    ('ETH/USD',  'Ethereum'),
    ('SOL/USD',  'Solana'),
    ('BNB/USD',  'BNB'),
    ('XRP/USD',  'XRP'),
    ('DOGE/USD', 'Dogecoin'),
    ('ADA/USD',  'Cardano')
ON CONFLICT (symbol) DO NOTHING;

-- 3. Continuous aggregate: daily OHLCV candles --------------------------------
--    Open / High / Low / Close / Volume per trading day, computed by the
--    database in the background — dashboards read the small pre-aggregated
--    hypertable instead of scanning every tick.
CREATE MATERIALIZED VIEW IF NOT EXISTS one_day_candle
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', "time") AS bucket,
    symbol,
    FIRST(price, "time")         AS "open",
    MAX(price)                   AS high,
    MIN(price)                   AS low,
    LAST(price, "time")          AS "close",
    LAST(day_volume, "time")     AS day_volume
FROM crypto_ticks
GROUP BY bucket, symbol
WITH NO DATA;

-- Refresh policy: re-materialize daily, keeping a 3-day lookback window
-- (the tutorial's policy — late-arriving ticks for yesterday still land).
SELECT add_continuous_aggregate_policy('one_day_candle',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

-- Real-time aggregation: union materialized rows with not-yet-refreshed raw
-- ticks, so the latest candle is always current.
ALTER MATERIALIZED VIEW one_day_candle SET (timescaledb.materialized_only = false);

-- 4. Continuous aggregate: 5-minute candles -----------------------------------
--    Fine-grained candles for live dashboards — this is the real-time view
--    of the market while the pipeline is streaming.
CREATE MATERIALIZED VIEW IF NOT EXISTS five_min_candle
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '5 minutes', "time") AS bucket,
    symbol,
    FIRST(price, "time")         AS "open",
    MAX(price)                   AS high,
    MIN(price)                   AS low,
    LAST(price, "time")          AS "close",
    LAST(day_volume, "time")     AS day_volume
FROM crypto_ticks
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('five_min_candle',
    start_offset      => NULL,              -- refresh everything
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');

ALTER MATERIALIZED VIEW five_min_candle SET (timescaledb.materialized_only = false);

-- 5. Columnstore compression for the aggregates -------------------------------
ALTER MATERIALIZED VIEW one_day_candle SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'symbol',
    timescaledb.orderby   = 'bucket DESC'
);

ALTER MATERIALIZED VIEW five_min_candle SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'symbol',
    timescaledb.orderby   = 'bucket DESC'
);

-- (The crypto_ticks hypertable itself gets a columnstore policy
--  automatically from tsdb.hypertable — chunks convert to the columnstore
--  after the default interval.)
