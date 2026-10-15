-- ============================================================
-- Proof queries — run these after the pipeline has streamed data.
-- ============================================================

-- 1. Hypertable + aggregates metadata
SELECT hypertable_name, num_chunks, compression_enabled
FROM   timescaledb_information.hypertables
ORDER  BY hypertable_name;

SELECT view_name, materialized_only
FROM   timescaledb_information.continuous_aggregates
ORDER  BY view_name;

-- 2. Ingestion proof: tick counts and freshness per symbol
SELECT symbol,
       count(*)                          AS ticks,
       min("time")                       AS first_tick,
       max("time")                       AS last_tick,
       round(avg(price)::numeric, 2)     AS avg_price
FROM   crypto_ticks
GROUP  BY symbol
ORDER  BY ticks DESC;

-- 3. End-to-end freshness: how far does the newest tick lag behind now()?
SELECT count(*)                    AS total_ticks,
       max("time")                 AS latest_tick,
       now() - max("time")         AS ingest_lag
FROM   crypto_ticks;

-- 4. Latest tick per symbol
SELECT DISTINCT ON (symbol)
       symbol, "time", price, day_volume
FROM   crypto_ticks
ORDER  BY symbol, "time" DESC;

-- 5. Live 5-minute candles (real-time aggregation — includes raw ticks
--    newer than the last materialization)
SELECT bucket, symbol, "open", high, low, "close",
       round(day_volume::numeric, 2) AS day_volume
FROM   five_min_candle
WHERE  bucket >= now() - INTERVAL '1 hour'
ORDER  BY bucket DESC, symbol
LIMIT  30;

-- 6. Daily candles
SELECT bucket, symbol, "open", high, low, "close",
       round(day_volume::numeric, 2) AS day_volume
FROM   one_day_candle
ORDER  BY bucket DESC, symbol;

-- 7. Compression settings of the raw hypertable
SELECT * FROM hypertable_compression_stats('crypto_ticks');
