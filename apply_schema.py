#!/usr/bin/env python3
"""
Apply schema.sql to the Tiger Cloud service.

Usage:
    python apply_schema.py [path-to-sql-file]
"""

from __future__ import annotations

import os
import sys

import psycopg2
from dotenv import load_dotenv


def main() -> int:
    sql_file = sys.argv[1] if len(sys.argv) > 1 else "schema.sql"
    load_dotenv()

    with open(sql_file, encoding="utf-8") as fh:
        sql = fh.read()

    conn = psycopg2.connect(os.environ["TIGER_URI"], connect_timeout=15)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()
    print(f"applied {sql_file} to Tiger Cloud")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
