#!/usr/bin/env python3
"""
Download CircleCI Usage API data for a date range.

Requires:
  - CIRCLECI_TOKEN or --api-token (API token with org read scope)
  - CIRCLECI_ORG_ID or --org-id

Writes CSV file(s) to --output-dir (default: ./data/).
"""

import gzip
import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta, timezone

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://circleci.com/api/v2"


def start_export(token, org_id, start, end):
    r = requests.post(
        f"{BASE_URL}/organizations/{org_id}/usage_export_job",
        headers={"Circle-Token": token},
        json={"start": start, "end": end},
    )
    r.raise_for_status()
    job_id = r.json()["usage_export_job_id"]
    logger.info(f"Export job started: {job_id} ({start} to {end})")
    return job_id


def poll_until_ready(token, org_id, job_id, timeout=600):
    deadline = time.time() + timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        time.sleep(min(10, 2 ** min(attempt, 5)))
        r = requests.get(
            f"{BASE_URL}/organizations/{org_id}/usage_export_job/{job_id}",
            headers={"Circle-Token": token},
        )
        r.raise_for_status()
        data = r.json()
        state = data.get("state")
        logger.info(f"  Poll {attempt}: {state}")
        if state == "completed":
            return data
        if state in ("failed", "error"):
            raise RuntimeError(f"Export job failed: {data}")
    raise TimeoutError(f"Export job {job_id} did not complete within {timeout}s")


def download_csv(urls, output_dir, prefix):
    paths = []
    for i, url in enumerate(urls):
        r = requests.get(url)
        r.raise_for_status()
        fname = f"{prefix}-{i}.csv" if len(urls) > 1 else f"{prefix}.csv"
        path = os.path.join(output_dir, fname)
        content = r.content
        if content[:2] == b"\x1f\x8b":
            content = gzip.decompress(content)
            logger.info(f"Decompressed gzip ({len(r.content):,} -> {len(content):,} bytes)")
        with open(path, "wb") as f:
            f.write(content)
        logger.info(f"Downloaded {path} ({len(content):,} bytes)")
        paths.append(path)
    return paths


def main():
    parser = argparse.ArgumentParser(description="Download CircleCI Usage API data")
    parser.add_argument("--org-id", default=os.environ.get("CIRCLECI_ORG_ID"),
                        help="CircleCI organization UUID (or set CIRCLECI_ORG_ID)")
    parser.add_argument("--api-token", default=None,
                        help="CircleCI API token (or set CIRCLECI_TOKEN)")
    parser.add_argument("--start-date", help="Start date YYYY-MM-DD (default: 30 days ago)")
    parser.add_argument("--end-date", help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--output-dir", default="./data",
                        help="Directory to write CSV files (default: ./data)")
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()

    token = args.api_token or os.environ.get("CIRCLECI_TOKEN") or os.environ.get("CIRCLE_TOKEN")
    org_id = args.org_id
    if not token or not org_id:
        logger.error("API token and org ID are required")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    now = datetime.now(timezone.utc)
    if args.end_date:
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_dt = now

    if args.start_date:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start_dt = end_dt - timedelta(days=30)

    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    job_id = start_export(token, org_id, start_str, end_str)
    result = poll_until_ready(token, org_id, job_id, args.timeout)
    urls = result.get("download_urls", [])
    if not urls:
        logger.warning("Export completed but no download URLs (no data in range?)")
        sys.exit(0)

    date_label = f"{start_dt.strftime('%Y%m%d')}-to-{end_dt.strftime('%Y%m%d')}"
    paths = download_csv(urls, args.output_dir, f"usage-{date_label}")
    logger.info(f"Done: {len(paths)} file(s) in {args.output_dir}")


if __name__ == "__main__":
    main()
