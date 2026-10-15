#!/usr/bin/env python3
"""
Render proof-of-work evidence as a single HTML page (for screenshots).

Combines:
  1. the live pipeline log (logs/ingest-run.log)
  2. live verification queries run against Tiger Cloud right now

Output: logs/evidence.html
"""

from __future__ import annotations

import html
import os

import psycopg2
from dotenv import load_dotenv

LOG = "logs/ingest-run.log"
OUT = "logs/evidence.html"

QUERIES = [
    (
        "Tick counts per symbol",
        """
        SELECT symbol, count(*) AS ticks,
               min("time") AS first_tick, max("time") AS last_tick,
               round(avg(price)::numeric, 2) AS avg_price
        FROM crypto_ticks GROUP BY symbol ORDER BY ticks DESC
        """,
    ),
    (
        "Ingest freshness",
        """
        SELECT count(*) AS total_ticks, max("time") AS latest_tick,
               now() - max("time") AS ingest_lag
        FROM crypto_ticks
        """,
    ),
    (
        "five_min_candle (continuous aggregate)",
        """
        SELECT bucket, symbol, "open", high, low, "close",
               round(day_volume::numeric, 0) AS day_volume
        FROM five_min_candle ORDER BY bucket DESC, symbol LIMIT 8
        """,
    ),
]


def table_html(columns, rows) -> str:
    head = "".join(f"<th>{html.escape(str(c))}</th>" for c in columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def main() -> None:
    load_dotenv()
    conn = psycopg2.connect(os.environ["TIGER_URI"], connect_timeout=15)

    sections = []
    for title, sql in QUERIES:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
        sections.append(f"<h2>{html.escape(title)}</h2>{table_html(columns, rows)}")
    conn.close()

    with open(LOG, encoding="utf-8") as fh:
        log_text = fh.read()

    page = f"""<!doctype html>
<html><head><meta charset="utf-8"><style>
  body {{ background:#0d1117; color:#c9d1d9; font-family:'Segoe UI',system-ui,sans-serif;
         margin:24px; font-size:14px; }}
  h1 {{ color:#e6edf3; font-size:20px; margin:0 0 4px; }}
  h2 {{ color:#e6edf3; font-size:15px; margin:20px 0 6px; }}
  .sub {{ color:#8b949e; margin-bottom:16px; }}
  .card {{ background:#161b22; border:1px solid #30363d; border-radius:8px;
          padding:16px 18px; margin-bottom:16px; }}
  pre {{ margin:0; font-family:Consolas,'Cascadia Mono',monospace; font-size:12.5px;
        line-height:1.5; white-space:pre-wrap; color:#7ee787; }}
  .pre-dim {{ color:#8b949e; }}
  table {{ border-collapse:collapse; width:100%; font-size:12.5px; }}
  th,td {{ border:1px solid #30363d; padding:4px 8px; text-align:left; }}
  th {{ background:#21262d; color:#e6edf3; }}
  td {{ font-family:Consolas,monospace; }}
  .ok {{ color:#3fb950; font-weight:600; }}
</style></head><body>
  <h1>Real-time financial data - Tiger Cloud ingestion pipeline</h1>
  <div class="sub">GHW: Hacktoberfest challenge proof &middot; service <b>first-hypertable</b>
   &middot; hypertable <b>crypto_ticks</b> &middot; <span class="ok">LIVE</span></div>
  <div class="card"><pre>{html.escape(log_text)}</pre></div>
  <div class="card">
    <div class="sub" style="margin:0 0 8px">Queried from Tiger Cloud just now:</div>
    {''.join(sections)}
  </div>
</body></html>"""

    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write(page)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
