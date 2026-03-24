#!/usr/bin/env python3
"""
Audit log loader for CircleCI streaming audit logs.

Downloads JSON audit log files from S3 or local directory, flattens them,
and loads into PostgreSQL for Grafana dashboards.

Modes:
  s3      - Pull logs from an S3 bucket
  local   - Load from a local directory of JSON files
  seed    - Generate realistic sample data for local testing
"""

import os
import sys
import json
import logging
import argparse
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS circleci_audit_logs (
    id              VARCHAR(255) PRIMARY KEY,
    version         INTEGER,
    action          VARCHAR(255) NOT NULL,
    actor_id        VARCHAR(255),
    actor_type      VARCHAR(100),
    actor_name      VARCHAR(255),
    target_id       VARCHAR(255),
    target_type     VARCHAR(100),
    target_name     VARCHAR(255),
    scope_id        VARCHAR(255),
    scope_type      VARCHAR(100),
    scope_name      VARCHAR(255),
    success         BOOLEAN,
    request_id      VARCHAR(255),
    payload         JSONB,
    metadata        JSONB,
    created_at      TIMESTAMP NOT NULL,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_action     ON circleci_audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_actor_name ON circleci_audit_logs(actor_name);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON circleci_audit_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_success    ON circleci_audit_logs(success);
"""

COLUMNS = [
    "id", "version", "action", "actor_id", "actor_type", "actor_name",
    "target_id", "target_type", "target_name", "scope_id", "scope_type",
    "scope_name", "success", "request_id", "payload", "metadata", "created_at",
]


def connect_pg(host, port, database, user, password):
    conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    logger.info("Connected to PostgreSQL")
    return conn


def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    logger.info("Audit log schema created")


def parse_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    actor = raw.get("actor") or {}
    target = raw.get("target") or {}
    scope = raw.get("scope") or {}
    request = raw.get("request") or {}
    return {
        "id":           raw.get("id"),
        "version":      raw.get("version"),
        "action":       raw.get("action"),
        "actor_id":     actor.get("id"),
        "actor_type":   actor.get("type"),
        "actor_name":   actor.get("name"),
        "target_id":    target.get("id"),
        "target_type":  target.get("type"),
        "target_name":  target.get("name"),
        "scope_id":     scope.get("id"),
        "scope_type":   scope.get("type"),
        "scope_name":   scope.get("name"),
        "success":      raw.get("success"),
        "request_id":   request.get("id"),
        "payload":      json.dumps(raw.get("payload")) if raw.get("payload") else None,
        "metadata":     json.dumps(raw.get("metadata")) if raw.get("metadata") else None,
        "created_at":   raw.get("occurred_at"),
    }


def upsert_rows(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    values = [tuple(r[c] for c in COLUMNS) for r in rows]
    sql = f"""
        INSERT INTO circleci_audit_logs ({', '.join(COLUMNS)})
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    return len(values)


def _parse_file_body(body: str) -> List[Dict[str, Any]]:
    rows = []
    for line in body.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(parse_event(json.loads(line)))
        except json.JSONDecodeError:
            try:
                rows.append(parse_event(json.loads(body)))
                return rows
            except json.JSONDecodeError:
                logger.warning("Skipping unparseable content")
                return rows
    return rows


def load_from_s3(conn, bucket, prefix, region, profile):
    try:
        import boto3
    except ImportError:
        logger.error("boto3 required for S3 mode: pip install boto3")
        return 1

    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    if region:
        session_kwargs["region_name"] = region

    s3 = boto3.Session(**session_kwargs).client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    total = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") or "connectivity_test" in key:
                continue
            logger.info(f"Processing s3://{bucket}/{key}")
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
            rows = _parse_file_body(body)
            if rows:
                inserted = upsert_rows(conn, rows)
                total += inserted
                logger.info(f"  Loaded {inserted} events")

    logger.info(f"S3 load complete: {total} events")
    return 0


def load_from_local(conn, directory):
    total = 0
    for fname in sorted(os.listdir(directory)):
        fpath = os.path.join(directory, fname)
        if not os.path.isfile(fpath) or "connectivity_test" in fname:
            continue
        logger.info(f"Processing {fpath}")
        with open(fpath) as f:
            body = f.read()
        rows = _parse_file_body(body)
        if rows:
            inserted = upsert_rows(conn, rows)
            total += inserted
            logger.info(f"  Loaded {inserted} events")

    logger.info(f"Local load complete: {total} events")
    return 0


# -- Seed mode ---------------------------------------------------------------

SEED_ACTIONS = [
    "workflow.job.start", "workflow.job.finish", "workflow.job.scheduled",
    "workflow.start", "context.create", "context.env_var.store",
    "context.env_var.delete", "context.secrets.accessed",
    "project.settings.update", "project.env_var.create", "project.add",
    "project.follow", "user.logged_in", "trigger_event.create",
    "checkout-key.create", "schedule.create", "workflow.cancel",
    "organization.settings.update",
]

SEED_ACTORS = [
    {"id": "a1", "type": "user", "name": "Developer A"},
    {"id": "a2", "type": "user", "name": "Developer B"},
    {"id": "a3", "type": "user", "name": "Developer C"},
    {"id": "a4", "type": "user", "name": "Admin User"},
    {"id": "a5", "type": "system", "name": "circleci-scheduler"},
]

SEED_PROJECTS = ["app-backend", "app-frontend", "infra-deploy", "shared-libs", "docs-site"]
SEED_CONTEXTS = ["aws-prod", "docker-hub", "signing-keys", "deploy-staging"]


def _uuid_from_seed(seed: str) -> str:
    h = hashlib.md5(seed.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def seed(conn, days, events_per_day):
    events = []
    now = datetime.now(timezone.utc)

    for day_offset in range(days, 0, -1):
        day_base = now - timedelta(days=day_offset)
        count = events_per_day + random.randint(-10, 10)
        for i in range(max(count, 5)):
            action = random.choice(SEED_ACTIONS)
            actor = random.choice(SEED_ACTORS)
            ts = day_base + timedelta(hours=random.randint(8, 20), minutes=random.randint(0, 59))

            if action.startswith("context"):
                target = {"id": f"ctx-{random.randint(1,4)}", "type": "context", "name": random.choice(SEED_CONTEXTS)}
            elif action.startswith("project") or action.startswith("workflow") or action.startswith("trigger") or action.startswith("checkout") or action.startswith("schedule"):
                proj = random.choice(SEED_PROJECTS)
                target = {"id": f"proj-{proj[:8]}", "type": "project", "name": proj}
            else:
                target = {"id": f"u-{random.randint(100,999)}", "type": "user", "name": random.choice(SEED_ACTORS)["name"]}

            payload = {}
            if action == "context.secrets.accessed":
                payload = {"context_name": random.choice(SEED_CONTEXTS), "environment_variable_titles": "SECRET_KEY,API_TOKEN"}

            events.append(parse_event({
                "id": _uuid_from_seed(f"{day_offset}-{i}-{action}"),
                "version": 1,
                "action": action,
                "actor": actor,
                "target": target,
                "scope": {"id": "org-1", "type": "organization", "name": "my-org"},
                "success": random.random() > 0.03,
                "request": {"id": _uuid_from_seed(f"req-{day_offset}-{i}")},
                "payload": payload,
                "metadata": {},
                "occurred_at": ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }))

    inserted = upsert_rows(conn, events)
    logger.info(f"Seeded {inserted} audit log events ({days} days)")
    return 0


def print_summary(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM circleci_audit_logs")
        total = cur.fetchone()[0]
        cur.execute("SELECT MIN(created_at), MAX(created_at) FROM circleci_audit_logs")
        dr = cur.fetchone()
        cur.execute("SELECT action, COUNT(*) FROM circleci_audit_logs GROUP BY action ORDER BY COUNT(*) DESC LIMIT 10")
        actions = cur.fetchall()
    print(f"\n=== Audit Log Summary ===")
    print(f"Total events: {total:,}")
    if dr[0]:
        print(f"Date range:   {dr[0]} to {dr[1]}")
    print(f"\nTop actions:")
    for action, cnt in actions:
        print(f"  {action}: {cnt:,}")


def main():
    parser = argparse.ArgumentParser(description="CircleCI audit log loader")
    sub = parser.add_subparsers(dest="mode", required=True)

    db_args = argparse.ArgumentParser(add_help=False)
    db_args.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    db_args.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
    db_args.add_argument("--database", default=os.getenv("PGDATABASE", "circleci_usage"))
    db_args.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    db_args.add_argument("--password", default=None)
    db_args.add_argument("--create-schema", action="store_true")
    db_args.add_argument("--summary", action="store_true")

    s3_p = sub.add_parser("s3", parents=[db_args])
    s3_p.add_argument("--bucket", required=True)
    s3_p.add_argument("--prefix", default="")
    s3_p.add_argument("--region", default="us-east-2")
    s3_p.add_argument("--profile", default=None)

    local_p = sub.add_parser("local", parents=[db_args])
    local_p.add_argument("--directory", required=True)

    seed_p = sub.add_parser("seed", parents=[db_args])
    seed_p.add_argument("--days", type=int, default=30)
    seed_p.add_argument("--events-per-day", type=int, default=40)

    args = parser.parse_args()
    password = args.password or os.getenv("PGPASSWORD", "postgres")
    conn = connect_pg(args.host, args.port, args.database, args.user, password)

    try:
        if args.create_schema:
            create_schema(conn)
        if args.mode == "s3":
            rc = load_from_s3(conn, args.bucket, args.prefix, args.region, args.profile)
        elif args.mode == "local":
            rc = load_from_local(conn, args.directory)
        elif args.mode == "seed":
            rc = seed(conn, args.days, args.events_per_day)
        else:
            rc = 1
        if args.summary:
            print_summary(conn)
        return rc
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
