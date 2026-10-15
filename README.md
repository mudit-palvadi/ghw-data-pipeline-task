# Real-time financial data -> Tiger Cloud

A data pipeline that ingests **real-time financial data** (live crypto trades)
from a websocket into **Tiger Cloud (TimescaleDB)** — built for the
*Global Hack Week: Hacktoberfest* challenge
["Ingest real-time financial data into Tiger Cloud"](https://www.mlh.com/events/global-hack-week-open-source-90/challenges/01a11b64-a4cd-bf31-7f19-8f2f8904f69d),
following the
[Tiger Data tutorial](https://www.tigerdata.com/docs/build/examples/ingest-real-time-financial-data).

```
 Binance / Twelve Data            Python pipeline                  Tiger Cloud (TimescaleDB)
 ┌────────────────┐   websocket   ┌──────────────────┐   batch    ┌─────────────────────────┐
 │ BTC/USD        │ ────────────▶ │ ingest.py        │ ─────────▶ │ crypto_ticks (hypertable)│
 │ ETH/USD        │   live ticks  │  in-memory batch │  INSERT    │   └─ five_min_candle     │
 │ SOL/USD        │               │  (100 ticks)     │  (100 rows)│   └─ one_day_candle (CAGG)│
 └────────────────┘               └──────────────────┘            └─────────────────────────┘
```

Ticks are collected in memory and written in one multi-row `INSERT` whenever
the batch reaches 100 records — the batching pattern from the tutorial.
Continuous aggregates pre-compute **OHLCV candlestick** data in the database,
so dashboards read a small pre-aggregated hypertable instead of scanning
every tick.

## Live run results

The pipeline was run live against a Tiger Cloud service (`first-hypertable`):

| Metric | Value |
| --- | --- |
| Ticks ingested | **7,500** (75 batches of 100) |
| Throughput | ~49 ticks/s |
| Symbols | BTC/USD (3,525), ETH/USD (3,467), SOL/USD (708) |
| Run time | 153 s |
| Freshness at check | `now() - max(time)` = under 1 minute |

Full run log: [`logs/ingest-run.log`](logs/ingest-run.log) ·
Rendered evidence page: [`logs/evidence.html`](logs/evidence.html)

Screenshots: [`screenshots/candlestick-BTC-USD.png`](screenshots/candlestick-BTC-USD.png)
(1-minute candles rendered from the stored ticks),
[`screenshots/console-overview.png`](screenshots/console-overview.png)
(Tiger Cloud console).

## Files

| File | Purpose |
| --- | --- |
| `schema.sql` | Hypertable + assets table + two continuous aggregates + policies |
| `apply_schema.py` | Applies `schema.sql` to the service in `.env` |
| `ingest.py` | The pipeline: websocket -> batch -> Tiger Cloud |
| `chart.py` | Candlestick chart from the stored ticks (matplotlib) |
| `verify.sql` | Proof queries (counts, freshness, candles) |
| `make_evidence.py` | Renders log + live query results to `logs/evidence.html` |
| `.env.example` | Connection template (copy to `.env`, fill in) |

## Quickstart

```bash
pip install -r requirements.txt
cp .env.example .env          # then fill in TIGER_URI (and TWELVEDATA_API_KEY if used)
python apply_schema.py        # create tables, aggregates, policies
python ingest.py --duration 300          # stream for 5 minutes (Binance, no API key)
python ingest.py --source twelvedata     # or the guide's Twelve Data websocket (needs key)
python chart.py --symbol BTC/USD --minutes 60
```

### Sources

- **`binance`** (default) — Binance *public* market-data websocket
  (`wss://stream.binance.com:9443`, `<pair>@trade` + `<pair>@ticker` streams).
  No API key needed; each tick carries the latest rolling 24h volume.
- **`twelvedata`** — the source used in the official Tiger Data tutorial
  (`pip install twelvedata websocket-client`, free API key from
  twelvedata.com). Same batching path.

### Schema notes

- `crypto_ticks` — hypertable, `segmentby='symbol'`, `orderby='time DESC'`,
  1-day chunks; columnstore policy is created automatically by
  `tsdb.hypertable`.
- `crypto_assets` — standard PostgreSQL table for asset names (relational
  metadata beside the time-series data).
- `five_min_candle`, `one_day_candle` — continuous aggregates with
  `FIRST(price, time)` / `LAST(price, time)` OHLCV bars, refresh policies,
  real-time aggregation (`materialized_only = false`), columnstore enabled.

## Notes

- `.env` holds real credentials and is **gitignored**; never commit it.
- `day_volume` comes from Binance's rolling 24h ticker stream; with Twelve
  Data it comes directly from the price event.
- The ingest rate on a free Tiger Cloud service is fine for demo runs; for
  production, raise `--max-batch-size` and/or run one connection per asset
  class (stocks / crypto) as the tutorial suggests.
