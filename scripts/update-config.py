#!/usr/bin/env python3
"""
Update dashboard configuration values in the database.

The dashboard_config table stores backend variables like cost_per_credit
that are referenced by dashboard SQL queries. This script provides a
simple way to view and update those values without touching the dashboard.

Usage:
    # View current config
    python3 update-config.py --list

    # Set cost per credit
    python3 update-config.py --set cost_per_credit=0.0006

    # Set multiple values
    python3 update-config.py --set cost_per_credit=0.00045 --set monthly_budget=50000
"""

import os
import sys
import argparse
import logging

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def connect(args):
    password = args.password or os.getenv("PGPASSWORD")
    if not password:
        logger.error("Password required via --password or PGPASSWORD env var")
        sys.exit(1)
    return psycopg2.connect(
        host=args.host, port=args.port,
        database=args.database, user=args.user, password=password,
    )


def list_config(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT key, value, description, updated_at FROM dashboard_config ORDER BY key")
        rows = cur.fetchall()
    if not rows:
        print("No configuration values set.")
        return
    print(f"\n{'Key':<25} {'Value':<20} {'Updated':<22} Description")
    print("-" * 90)
    for key, value, desc, updated in rows:
        print(f"{key:<25} {value:<20} {str(updated):<22} {desc or ''}")
    print()


def set_values(conn, pairs):
    with conn.cursor() as cur:
        for pair in pairs:
            if "=" not in pair:
                logger.error(f"Invalid format '{pair}' -- use key=value")
                continue
            key, value = pair.split("=", 1)
            cur.execute("""
                INSERT INTO dashboard_config (key, value, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET value = %s, updated_at = CURRENT_TIMESTAMP
            """, (key.strip(), value.strip(), value.strip()))
            logger.info(f"Set {key.strip()} = {value.strip()}")
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Manage dashboard configuration")
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
    parser.add_argument("--database", default=os.getenv("PGDATABASE", "circleci_usage"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    parser.add_argument("--password", default=None)
    parser.add_argument("--list", action="store_true", help="List current config values")
    parser.add_argument("--set", action="append", metavar="KEY=VALUE",
                        help="Set a config value (repeatable)")
    args = parser.parse_args()

    if not args.list and not args.set:
        parser.print_help()
        sys.exit(0)

    conn = connect(args)
    try:
        if args.set:
            set_values(conn, args.set)
        if args.list or not args.set:
            list_config(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
