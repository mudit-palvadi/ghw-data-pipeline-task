#!/usr/bin/env python3
"""
Render a candlestick chart from the ticks stored in Tiger Cloud.

Queries 1-minute OHLC bars for one symbol straight from the `crypto_ticks`
hypertable and saves a candlestick chart (with a trade-count subplot).

Usage:
    python chart.py                       # BTC/USD, last 60 minutes
    python chart.py --symbol ETH/USD --minutes 30
    python chart.py --out screenshots/eth-1m.png
"""

from __future__ import annotations

import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from psycopg2 import connect

QUERY = """
SELECT time_bucket(INTERVAL '1 minute', "time") AS bucket,
       FIRST(price, "time")                 AS open,
       MAX(price)                           AS high,
       MIN(price)                           AS low,
       LAST(price, "time")                  AS close,
       count(*)                             AS trades
FROM   crypto_ticks
WHERE  symbol = %s
  AND  "time" >= now() - make_interval(mins => %s)
GROUP  BY bucket
ORDER  BY bucket
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Candlestick chart from Tiger Cloud ticks.")
    parser.add_argument("--symbol", default="BTC/USD")
    parser.add_argument("--minutes", type=int, default=60)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    load_dotenv()
    out = args.out or f"screenshots/candlestick-{args.symbol.replace('/', '-')}.png"

    with connect(os.environ["TIGER_URI"], connect_timeout=15) as conn:
        df = pd.read_sql(QUERY, conn, params=(args.symbol, args.minutes))

    if df.empty:
        print(f"no ticks for {args.symbol} in the last {args.minutes} minutes")
        return 1

    df["bucket"] = pd.to_datetime(df["bucket"], utc=True).dt.tz_convert(None)
    df = df.set_index("bucket")
    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(float)
    df["trades"] = df["trades"].astype(int)

    fig, (ax_price, ax_vol) = plt.subplots(
        2, 1, figsize=(13, 7), sharex=True,
        gridspec_kw={"height_ratios": [4, 1]},
    )
    fig.patch.set_facecolor("#0d1117")
    for ax in (ax_price, ax_vol):
        ax.set_facecolor("#0d1117")
        ax.tick_params(colors="#c9d1d9")
        ax.grid(color="#21262d", linewidth=0.6)
        for spine in ax.spines.values():
            spine.set_color("#30363d")

    width = 1 / 1440 * 0.7  # a fraction of a 1-minute bar in days
    for ts, row in df.iterrows():
        up = row["close"] >= row["open"]
        color = "#3fb950" if up else "#f85149"
        ax_price.vlines(ts, row["low"], row["high"], color=color, linewidth=1.1)
        body_low, body_high = sorted((row["open"], row["close"]))
        ax_price.add_patch(
            plt.Rectangle(
                (mdates.date2num(ts) - width / 2, body_low), width,
                max(body_high - body_low, row["high"] * 1e-5),  # keep doji visible
                facecolor=color, edgecolor=color,
            )
        )
        ax_vol.bar(ts, row["trades"], width=width, color=color, alpha=0.85)

    ax_price.set_title(
        f"{args.symbol} — 1-minute candles from Tiger Cloud "
        f"({len(df)} bars, {df['trades'].sum():,} ticks)",
        color="#e6edf3", fontsize=13, pad=12,
    )
    ax_price.set_ylabel("price (USDT)", color="#8b949e")
    ax_vol.set_ylabel("trades", color="#8b949e")
    ax_vol.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax_vol.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax_vol.set_xlabel("UTC time", color="#8b949e")

    fig.tight_layout()
    fig.savefig(out, dpi=150, facecolor=fig.get_facecolor())
    print(f"saved {out}  ({len(df)} bars, {df['trades'].sum():,} ticks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
