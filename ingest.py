#!/usr/bin/env python3
"""
Real-time financial data -> Tiger Cloud ingestion pipeline.

Streams live market data over a websocket and batch-inserts it into the
`crypto_ticks` hypertable on a Tiger Cloud (TimescaleDB) service.

The batching pattern mirrors the Tiger Data tutorial
"ingest real-time financial data" — ticks are collected in an in-memory list
and written in one multi-row INSERT whenever the batch fills up.

Sources
-------
binance     Public Binance market-data websocket. No API key required. (default)
twelvedata  Twelve Data websocket — the source used in the official Tiger Data
            tutorial. Requires TWELVEDATA_API_KEY in the environment.

Usage
-----
    python ingest.py                            # stream until Ctrl+C
    python ingest.py --duration 300             # stream for 5 minutes
    python ingest.py --source twelvedata
    python ingest.py --symbols BTC/USD ETH/USD SOL/USD DOGE/USD
    python ingest.py --max-batch-size 500

Connection settings are read from the environment (or a local .env):
    TIGER_URI (or TIGER_HOST / TIGER_PORT / TIGER_DB / TIGER_USER / TIGER_PASSWORD)
    TWELVEDATA_API_KEY (only for --source twelvedata)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Callable, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

DB_TABLE = "crypto_ticks"
DB_COLUMNS = ["time", "symbol", "price", "day_volume"]

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def log(message: str) -> None:
    stamp = datetime.now().strftime("%H:%M:%S")
    print(f"{stamp} | {message}", flush=True)


def banner(message: str) -> None:
    log(message)


def connect_db() -> "psycopg2.extensions.connection":
    """Open a psycopg2 connection using TIGER_URI or individual settings."""
    uri = os.getenv("TIGER_URI")
    if uri:
        return psycopg2.connect(uri, connect_timeout=15)
    return psycopg2.connect(
        host=os.getenv("TIGER_HOST"),
        port=os.getenv("TIGER_PORT"),
        dbname=os.getenv("TIGER_DB", "tsdb"),
        user=os.getenv("TIGER_USER", "tsdbadmin"),
        password=os.getenv("TIGER_PASSWORD"),
        connect_timeout=15,
    )


def binance_symbol(symbol: str) -> str:
    """'BTC/USD' -> 'btcusdt', 'ADA/BTC' -> 'adabtc'."""
    base, quote = symbol.split("/")
    if quote == "USD":
        quote = "USDT"
    return (base + quote).lower()


# ---------------------------------------------------------------------------
# batching
# ---------------------------------------------------------------------------
class TickBatcher:
    """Collects ticks in memory; bulk-inserts when the batch reaches the limit."""

    def __init__(self, conn, max_batch_size: int = 100, reconnect: Optional[Callable] = None):
        self.conn = conn
        self.max_batch_size = max_batch_size
        self.reconnect = reconnect
        self.batch: list[tuple] = []
        self.batches_inserted = 0
        self.ticks_seen = 0
        self.ticks_inserted = 0
        self.started_at = time.time()

    def add(self, ts: datetime, symbol: str, price: float, day_volume: Optional[float]) -> None:
        self.ticks_seen += 1
        self.batch.append((ts, symbol, price, day_volume))
        if len(self.batch) >= self.max_batch_size:
            self.flush()

    def flush(self) -> None:
        """Write the current batch in a single INSERT and reset it."""
        if not self.batch:
            return
        try:
            self._insert(self.batch)
        except psycopg2.Error as exc:
            log(f"insert failed ({exc.__class__.__name__}); reconnecting")
            if self.reconnect is None:
                raise
            self.conn = self.reconnect()
            self._insert(self.batch)  # retry once on the fresh connection
        self.batches_inserted += 1
        self.ticks_inserted += len(self.batch)
        log(f"batch #{self.batches_inserted} inserted - {len(self.batch)} ticks "
            f"(total {self.ticks_inserted} ticks in Tiger Cloud)")
        self.batch = []

    def _insert(self, rows: list[tuple]) -> None:
        cursor = self.conn.cursor()
        sql = f"INSERT INTO {DB_TABLE} ({','.join(DB_COLUMNS)}) VALUES %s"
        execute_values(cursor, sql, rows)
        self.conn.commit()
        cursor.close()

    def summary(self) -> None:
        elapsed = time.time() - self.started_at
        rate = self.ticks_inserted / elapsed if elapsed > 0 else 0
        log("-" * 62)
        log(f"received {self.ticks_seen} ticks | inserted {self.ticks_inserted} ticks "
            f"in {self.batches_inserted} batches")
        log(f"run time {elapsed:.0f}s - average {rate:.1f} ticks/s")


# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------
class BinanceSource:
    """Public Binance market-data websocket (no API key).

    Subscribes to `<pair>@trade` for every symbol, plus `<pair>@ticker` so each
    tick can carry the latest rolling 24h volume as `day_volume`.
    """

    def __init__(self, symbols: list[str], batcher: TickBatcher):
        self.symbols = symbols
        self.batcher = batcher
        self._to_pretty = {binance_symbol(s).upper(): s for s in symbols}
        self._day_volume: dict[str, float] = {}
        self._stop = False

    def _streams_url(self) -> str:
        streams = []
        for sym in self.symbols:
            b = binance_symbol(sym)
            streams.extend([f"{b}@trade", f"{b}@ticker"])
        return BINANCE_WS_BASE + "/".join(streams)

    def _on_message(self, ws, raw: str) -> None:
        data = json.loads(raw).get("data", {})
        event = data.get("e")
        if event == "trade":
            pair = data["s"]
            ts = datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc)
            self.batcher.add(
                ts,
                self._to_pretty.get(pair, pair),
                float(data["p"]),
                self._day_volume.get(pair),
            )
        elif event == "24hrTicker":
            self._day_volume[data["s"]] = float(data["v"])
        if self._stop:
            ws.close()

    def run(self, duration: Optional[float]) -> None:
        import websocket  # websocket-client

        deadline = time.time() + duration if duration else None
        url = self._streams_url()
        log(f"connecting to Binance public websocket: {len(self.symbols)} pairs "
            f"({', '.join(self.symbols)})")
        while True:
            try:
                ws_app = websocket.WebSocketApp(url, on_message=self._on_message)
                if deadline:
                    # let the stream end by itself when the demo window closes
                    ws_app.run_forever(ping_interval=30)
                else:
                    ws_app.run_forever(ping_interval=30)
            except KeyboardInterrupt:
                raise
            except Exception as exc:  # reconnect on network hiccups
                log(f"websocket error: {exc}; reconnecting in 5s")
                time.sleep(5)
            if deadline and time.time() >= deadline:
                self._stop = True
            if self._stop:
                return
            log("websocket closed; reconnecting in 5s")
            time.sleep(5)

    def stop_after(self, deadline: float) -> None:
        """Called from a timer thread when --duration elapses."""
        while time.time() < deadline and not self._stop:
            time.sleep(0.5)
        self._stop = True
        log("duration reached - stopping stream")


class TwelveDataSource:
    """Twelve Data websocket — the source used by the Tiger Data tutorial."""

    def __init__(self, symbols: list[str], batcher: TickBatcher, api_key: str):
        self.symbols = symbols
        self.batcher = batcher
        self.api_key = api_key
        self._stop = False

    def _on_event(self, event: dict) -> None:
        kind = event.get("event")
        if kind == "price":
            ts = datetime.fromtimestamp(int(event["timestamp"]), tz=timezone.utc)
            self.batcher.add(
                ts,
                event["symbol"],
                float(event["price"]),
                float(event["day_volume"]) if event.get("day_volume") is not None else None,
            )
        elif kind == "subscribe-status" and event.get("status") == "ok":
            ok = ", ".join(item["symbol"] for item in event.get("success") or [])
            log(f"subscribed: {ok}")
        elif kind == "heartbeat":
            pass

    def run(self, duration: Optional[float]) -> None:
        from twelvedata import TDClient

        td = TDClient(apikey=self.api_key)
        ws = td.websocket(symbols=self.symbols, on_event=self._on_event)
        ws.connect()
        log(f"connected to Twelve Data websocket ({', '.join(self.symbols)})")
        deadline = time.time() + duration if duration else None
        while not self._stop:
            ws.heartbeat()
            if deadline and time.time() >= deadline:
                self._stop = True
            time.sleep(10)
        try:
            ws.disconnect()
        except Exception:
            pass

    def stop_after(self, deadline: float) -> None:
        while time.time() < deadline and not self._stop:
            time.sleep(0.5)
        self._stop = True


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Stream real-time financial ticks into Tiger Cloud.")
    parser.add_argument("--source", choices=["binance", "twelvedata"], default="binance",
                        help="market-data source (default: binance, no API key needed)")
    parser.add_argument("--symbols", nargs="+", default=["BTC/USD", "ETH/USD", "SOL/USD"],
                        help="trading pairs, e.g. BTC/USD ETH/USD (default: BTC/USD ETH/USD SOL/USD)")
    parser.add_argument("--max-batch-size", type=int, default=100,
                        help="ticks per batch INSERT (default: 100)")
    parser.add_argument("--duration", type=float, default=None,
                        help="stop after N seconds (default: run until Ctrl+C)")
    args = parser.parse_args()

    load_dotenv()

    if args.source == "twelvedata" and not os.getenv("TWELVEDATA_API_KEY"):
        log("TWELVEDATA_API_KEY is not set - add it to .env or use --source binance")
        return 1

    conn = connect_db()
    batcher = TickBatcher(conn, max_batch_size=args.max_batch_size, reconnect=connect_db)

    log("real-time financial data -> Tiger Cloud (TimescaleDB)")
    log(f"target table: {DB_TABLE} | batch size: {args.max_batch_size} | source: {args.source}")

    if args.source == "binance":
        source = BinanceSource(args.symbols, batcher)
    else:
        source = TwelveDataSource(args.symbols, batcher, os.environ["TWELVEDATA_API_KEY"])

    if args.duration:
        import threading
        threading.Thread(
            target=source.stop_after, args=(time.time() + args.duration,), daemon=True
        ).start()

    try:
        source.run(args.duration)
    except KeyboardInterrupt:
        log("interrupted - flushing final batch")
    finally:
        batcher.flush()
        batcher.summary()
        batcher.conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
