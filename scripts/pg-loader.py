#!/usr/bin/env python3
"""
PostgreSQL loader for CircleCI Usage API data.

Creates the schema, loads CSV exports into PostgreSQL, and manages
the dashboard_config table used for backend variables (e.g. cost_per_credit).
"""

import os
import sys
import glob
import logging
import argparse
from typing import Optional, Dict, Any, List

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


USAGE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS circleci_usage (
    id SERIAL PRIMARY KEY,
    organization_id VARCHAR(255),
    organization_name VARCHAR(255),
    organization_created_date TIMESTAMP,
    project_id VARCHAR(255),
    project_name VARCHAR(255),
    project_created_date TIMESTAMP,
    last_build_finished_at TIMESTAMP,
    vcs_name VARCHAR(100),
    vcs_url TEXT,
    vcs_branch VARCHAR(255),
    pipeline_id VARCHAR(255),
    pipeline_created_at TIMESTAMP,
    pipeline_number NUMERIC,
    is_unregistered_user BOOLEAN,
    pipeline_trigger_source VARCHAR(100),
    pipeline_trigger_user_id VARCHAR(255),
    workflow_id VARCHAR(255),
    workflow_name VARCHAR(255),
    workflow_first_job_queued_at TIMESTAMP,
    workflow_first_job_started_at TIMESTAMP,
    workflow_stopped_at TIMESTAMP,
    is_workflow_successful BOOLEAN,
    job_name VARCHAR(255),
    job_run_number NUMERIC,
    job_id VARCHAR(255),
    job_run_date TIMESTAMP,
    job_run_queued_at TIMESTAMP,
    job_run_started_at TIMESTAMP,
    job_run_stopped_at TIMESTAMP,
    job_build_status VARCHAR(50),
    resource_class VARCHAR(100),
    operating_system VARCHAR(100),
    executor VARCHAR(100),
    parallelism INTEGER,
    job_run_seconds NUMERIC,
    median_cpu_utilization_pct DECIMAL(5,2),
    max_cpu_utilization_pct DECIMAL(5,2),
    median_ram_utilization_pct DECIMAL(5,2),
    max_ram_utilization_pct DECIMAL(5,2),
    compute_credits DECIMAL(10,2),
    dlc_credits DECIMAL(10,2),
    user_credits DECIMAL(10,2),
    storage_credits DECIMAL(10,2),
    network_credits DECIMAL(10,2),
    lease_credits DECIMAL(10,2),
    lease_overage_credits DECIMAL(10,2),
    ipranges_credits DECIMAL(10,2),
    total_credits DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_circleci_usage_job_id'
    ) THEN
        ALTER TABLE circleci_usage ADD CONSTRAINT uq_circleci_usage_job_id UNIQUE (job_id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_usage_organization_id ON circleci_usage(organization_id);
CREATE INDEX IF NOT EXISTS idx_usage_project_name ON circleci_usage(project_name);
CREATE INDEX IF NOT EXISTS idx_usage_workflow_name ON circleci_usage(workflow_name);
CREATE INDEX IF NOT EXISTS idx_usage_workflow_id ON circleci_usage(workflow_id);
CREATE INDEX IF NOT EXISTS idx_usage_job_name ON circleci_usage(job_name);
CREATE INDEX IF NOT EXISTS idx_usage_job_build_status ON circleci_usage(job_build_status);
CREATE INDEX IF NOT EXISTS idx_usage_resource_class ON circleci_usage(resource_class);
CREATE INDEX IF NOT EXISTS idx_usage_executor ON circleci_usage(executor);
CREATE INDEX IF NOT EXISTS idx_usage_pipeline_created_at ON circleci_usage(pipeline_created_at);
CREATE INDEX IF NOT EXISTS idx_usage_job_run_started_at ON circleci_usage(job_run_started_at);
CREATE INDEX IF NOT EXISTS idx_usage_total_credits ON circleci_usage(total_credits);
CREATE INDEX IF NOT EXISTS idx_usage_trigger_user ON circleci_usage(pipeline_trigger_user_id);
"""

CONFIG_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dashboard_config (
    key VARCHAR(255) PRIMARY KEY,
    value VARCHAR(255) NOT NULL,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dashboard_config (key, value, description)
VALUES ('cost_per_credit', '0.0006', 'Dollar cost per CircleCI credit')
ON CONFLICT (key) DO NOTHING;
"""

VIEW_SQL = """
CREATE OR REPLACE VIEW job_performance AS
SELECT
    job_name,
    resource_class,
    executor,
    COUNT(*) as job_count,
    AVG(job_run_seconds) as avg_duration_seconds,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY job_run_seconds) as median_duration_seconds,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY job_run_seconds) as p95_duration_seconds,
    AVG(median_cpu_utilization_pct) as avg_cpu_utilization,
    AVG(median_ram_utilization_pct) as avg_ram_utilization,
    SUM(total_credits) as total_credits_used,
    AVG(total_credits) as avg_credits_per_job,
    SUM(CASE WHEN job_build_status = 'success' THEN 1 ELSE 0 END) as successful_jobs,
    SUM(CASE WHEN job_build_status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
    ROUND(
        SUM(CASE WHEN job_build_status = 'success' THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100, 2
    ) as success_rate_pct
FROM circleci_usage
GROUP BY job_name, resource_class, executor
ORDER BY total_credits_used DESC;

CREATE OR REPLACE VIEW cost_analysis AS
SELECT
    organization_name,
    project_name,
    DATE_TRUNC('day', pipeline_created_at) as usage_date,
    resource_class,
    executor,
    COUNT(*) as job_count,
    SUM(total_credits) as total_credits,
    AVG(total_credits) as avg_credits_per_job,
    SUM(compute_credits) as total_compute_credits,
    SUM(dlc_credits) as total_dlc_credits,
    SUM(user_credits) as total_user_credits,
    SUM(storage_credits) as total_storage_credits,
    SUM(network_credits) as total_network_credits,
    SUM(lease_credits) as total_lease_credits
FROM circleci_usage
WHERE pipeline_created_at IS NOT NULL
GROUP BY organization_name, project_name, DATE_TRUNC('day', pipeline_created_at),
         resource_class, executor
ORDER BY usage_date DESC, total_credits DESC;
"""


COLUMN_MAPPING = [
    "organization_id", "organization_name", "organization_created_date",
    "project_id", "project_name", "project_created_date", "last_build_finished_at",
    "vcs_name", "vcs_url", "vcs_branch",
    "pipeline_id", "pipeline_created_at", "pipeline_number",
    "is_unregistered_user", "pipeline_trigger_source", "pipeline_trigger_user_id",
    "workflow_id", "workflow_name",
    "workflow_first_job_queued_at", "workflow_first_job_started_at", "workflow_stopped_at",
    "is_workflow_successful",
    "job_name", "job_run_number", "job_id",
    "job_run_date", "job_run_queued_at", "job_run_started_at", "job_run_stopped_at",
    "job_build_status", "resource_class", "operating_system", "executor", "parallelism",
    "job_run_seconds",
    "median_cpu_utilization_pct", "max_cpu_utilization_pct",
    "median_ram_utilization_pct", "max_ram_utilization_pct",
    "compute_credits", "dlc_credits", "user_credits", "storage_credits",
    "network_credits", "lease_credits", "lease_overage_credits",
    "ipranges_credits", "total_credits",
]

DATETIME_COLUMNS = [
    "organization_created_date", "project_created_date", "last_build_finished_at",
    "pipeline_created_at", "workflow_first_job_queued_at", "workflow_first_job_started_at",
    "workflow_stopped_at", "job_run_date", "job_run_queued_at", "job_run_started_at",
    "job_run_stopped_at",
]

BOOLEAN_COLUMNS = ["is_unregistered_user", "is_workflow_successful"]

NUMERIC_COLUMNS = [
    "pipeline_number", "parallelism", "job_run_number", "job_run_seconds",
    "median_cpu_utilization_pct", "max_cpu_utilization_pct",
    "median_ram_utilization_pct", "max_ram_utilization_pct",
    "compute_credits", "dlc_credits", "user_credits", "storage_credits",
    "network_credits", "lease_credits", "lease_overage_credits",
    "ipranges_credits", "total_credits",
]


def connect(host, port, database, user, password):
    conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    logger.info("Connected to PostgreSQL")
    return conn


def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(USAGE_SCHEMA_SQL)
        cur.execute(CONFIG_SCHEMA_SQL)
        cur.execute(VIEW_SQL)
    conn.commit()
    logger.info("Schema created (circleci_usage, dashboard_config, views)")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [col.lower().replace(" ", "_") for col in cleaned.columns]

    for col in DATETIME_COLUMNS:
        if col in cleaned.columns:
            cleaned[col] = pd.to_datetime(cleaned[col], errors="coerce")

    for col in BOOLEAN_COLUMNS:
        if col in cleaned.columns:
            cleaned[col] = cleaned[col].map(
                {"true": True, "false": False, "True": True, "False": False}
            )

    for col in NUMERIC_COLUMNS:
        if col in cleaned.columns:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

    if "parallelism" in cleaned.columns:
        cleaned["parallelism"] = cleaned["parallelism"].astype("Int64")

    cleaned = cleaned.where(pd.notnull(cleaned), None)
    for col in DATETIME_COLUMNS:
        if col in cleaned.columns:
            cleaned[col] = cleaned[col].replace({pd.NaT: None})
    cleaned = cleaned.replace({pd.NA: None, float("nan"): None})

    return cleaned


def insert_batch(conn, df: pd.DataFrame) -> bool:
    if df.empty:
        return True

    available = [c for c in COLUMN_MAPPING if c in df.columns]
    if not available:
        logger.warning("No matching columns found in dataframe")
        return True

    values = []
    for _, row in df.iterrows():
        values.append(tuple(
            None if pd.isna(row.get(col)) else row.get(col, None)
            for col in available
        ))

    sql = f"""
        INSERT INTO circleci_usage ({', '.join(available)})
        VALUES %s
        ON CONFLICT (job_id) DO NOTHING
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)
        conn.commit()
        return True
    except psycopg2.Error as e:
        logger.error(f"Failed to insert batch: {e}")
        conn.rollback()
        return False


def load_csv(conn, csv_path: str, batch_size: int = 1000) -> bool:
    logger.info(f"Loading {csv_path}")
    total = 0
    for chunk_num, chunk in enumerate(pd.read_csv(csv_path, chunksize=batch_size, na_values=["\\N"])):
        cleaned = clean_dataframe(chunk)
        if not insert_batch(conn, cleaned):
            return False
        total += len(cleaned)
        logger.info(f"  Chunk {chunk_num + 1}: {len(cleaned)} rows (total: {total})")
    logger.info(f"Loaded {total} records from {os.path.basename(csv_path)}")
    return True


def load_directory(conn, directory: str, batch_size: int = 1000) -> bool:
    csv_files = sorted(glob.glob(os.path.join(directory, "*.csv")))
    if not csv_files:
        logger.warning(f"No CSV files found in {directory}")
        return True
    for csv_file in csv_files:
        if not load_csv(conn, csv_file, batch_size):
            return False
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM circleci_usage")
        total = cur.fetchone()[0]
    logger.info(f"Directory load complete: {total} records in database")
    return True


def truncate(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE circleci_usage")
    conn.commit()
    logger.info("Truncated circleci_usage table")


def print_summary(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM circleci_usage")
        total = cur.fetchone()[0]
        cur.execute("""
            SELECT MIN(pipeline_created_at), MAX(pipeline_created_at)
            FROM circleci_usage
        """)
        dr = cur.fetchone()
        cur.execute("SELECT COUNT(DISTINCT project_name) FROM circleci_usage")
        projects = cur.fetchone()[0]
        cur.execute("SELECT SUM(total_credits) FROM circleci_usage")
        credits = cur.fetchone()[0] or 0
        cur.execute("""
            SELECT job_build_status, COUNT(*) FROM circleci_usage
            GROUP BY job_build_status ORDER BY COUNT(*) DESC
        """)
        statuses = cur.fetchall()
        cur.execute("SELECT value FROM dashboard_config WHERE key = 'cost_per_credit'")
        row = cur.fetchone()
        cpc = row[0] if row else "not set"

    print(f"\n=== Data Summary ===")
    print(f"Total records:    {total:,}")
    print(f"Projects:         {projects}")
    print(f"Total credits:    {credits:,.2f}")
    print(f"Cost per credit:  {cpc}")
    if dr[0]:
        print(f"Date range:       {dr[0]} to {dr[1]}")
    print(f"\nJob Status Breakdown:")
    for status, count in statuses:
        print(f"  {status}: {count:,}")


def main():
    parser = argparse.ArgumentParser(description="Load CircleCI Usage API data into PostgreSQL")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--csv-file", help="Path to a single CSV file")
    group.add_argument("--directory", help="Path to a directory of CSV files")

    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
    parser.add_argument("--database", default=os.getenv("PGDATABASE", "circleci_usage"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    parser.add_argument("--password", help="Or set PGPASSWORD env var")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--create-schema", action="store_true", help="Create tables and views")
    parser.add_argument("--truncate", action="store_true", help="Truncate before loading")
    parser.add_argument("--summary", action="store_true", help="Print summary after loading")
    args = parser.parse_args()

    password = args.password or os.getenv("PGPASSWORD")
    if not password:
        logger.error("Password required via --password or PGPASSWORD env var")
        sys.exit(1)

    conn = connect(args.host, args.port, args.database, args.user, password)
    try:
        if args.create_schema:
            create_schema(conn)
        if args.truncate:
            truncate(conn)
        if args.directory:
            if not load_directory(conn, args.directory, args.batch_size):
                sys.exit(1)
        else:
            if not load_csv(conn, args.csv_file, args.batch_size):
                sys.exit(1)
        if args.summary:
            print_summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
