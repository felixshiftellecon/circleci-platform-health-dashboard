# CI/CD Platform Health Dashboard

A self-hosted Grafana dashboard for monitoring CircleCI usage, costs, performance, and compliance. Powered by CircleCI's Usage API data and Audit Log Streaming, loaded into PostgreSQL.

## Architecture

```text
CircleCI Usage API ──> download-usage.py ──> CSV files ──> pg-loader.py ──> PostgreSQL
CircleCI Audit Logs ──> S3 bucket ──> audit-log-loader.py ────────────────> PostgreSQL
                                                                                │
                                                                           Grafana ──> dashboard.json
```

**Data sources:**

- **Usage API**: Job-level data including credits, duration, resource utilization, workflow/pipeline metadata, and user activity. Exported as CSV via the CircleCI v2 API.
- **Audit Log Streaming**: Organization-level events (context access, project changes, secret access, user actions). Delivered as JSON to an S3 bucket.

**Backend variable storage:**

The `dashboard_config` table in PostgreSQL stores configurable values like `cost_per_credit`. Dashboard SQL queries reference this table directly, so there are no customer-specific values embedded in the dashboard itself.

## Quick Start

### 1. Start the stack

```bash
docker compose up -d
```

This starts PostgreSQL (port 5432) and Grafana (port 3000). Default credentials: `admin` / `admin`.

### 2. Install Python dependencies

```bash
pip install -r scripts/requirements.txt
```

### 3. Initialize the database

```bash
python3 scripts/pg-loader.py --create-schema --csv-file /dev/null
```

This creates the `circleci_usage` table, `dashboard_config` table (with default `cost_per_credit = 0.0006`), and analysis views. The `/dev/null` input is a no-op; the schema is what matters.

For the audit log table:

```bash
python3 scripts/audit-log-loader.py seed --create-schema --days 0
```

### 4. Set your credit cost

```bash
PGPASSWORD=postgres python3 scripts/update-config.py --set cost_per_credit=0.0006
```

Replace `0.0006` with your contracted cost per credit. This value is used by all cost-related panels on the dashboard.

### 5. Download and load Usage API data

```bash
# Download last 30 days of usage data
python3 scripts/download-usage.py \
    --org-id YOUR_ORG_UUID \
    --api-token YOUR_CIRCLECI_TOKEN \
    --start-date 2026-02-01 \
    --end-date 2026-03-01 \
    --output-dir ./data

# Load into PostgreSQL
PGPASSWORD=postgres python3 scripts/pg-loader.py \
    --directory ./data \
    --create-schema \
    --summary
```

The Usage API has a max 32-day range per request. For longer periods, run multiple downloads with consecutive date ranges; the loader deduplicates on `job_id`.

### 6. Load audit logs

**From S3:**

```bash
PGPASSWORD=postgres python3 scripts/audit-log-loader.py s3 \
    --bucket YOUR_AUDIT_LOG_BUCKET \
    --prefix "" \
    --region us-east-2 \
    --create-schema \
    --summary
```

**From local JSON files:**

```bash
PGPASSWORD=postgres python3 scripts/audit-log-loader.py local \
    --directory /path/to/audit-logs/ \
    --create-schema \
    --summary
```

**Seed sample data (for testing):**

```bash
PGPASSWORD=postgres python3 scripts/audit-log-loader.py seed \
    --create-schema \
    --days 30 \
    --events-per-day 40 \
    --summary
```

### 7. Import the dashboard

1. Open Grafana at `http://localhost:3000`
2. Go to **Dashboards > New > Import**
3. Upload `dashboard.json` or paste its contents
4. Select the **PostgreSQL** datasource when prompted
5. Click **Import**

## Dashboard Sections

### Org Health Scorecard
Top-level KPIs across the selected time range:
- **Total Pipelines** -- distinct pipeline count
- **Avg Pipeline Duration** -- mean job duration in seconds
- **Success Rate** -- percentage of jobs that succeeded
- **Credits Consumed** -- total credits used
- **Total Cost** -- credits multiplied by `cost_per_credit` from the database
- **MTTR** -- Mean Time to Recovery (minutes from a failed job to its next success)

### Pipeline Activity
- **Pipelines per Day** -- daily pipeline volume as a bar chart
- **Activity + Cost Correlation** -- overlays daily pipeline count with daily cost

### Performance
- **Slowest Workflows** -- ranked by median duration, with job counts and percentiles
- **Slowest Jobs** -- ranked by median duration, showing resource class and executor

### Cost and Optimization
- **Under-utilised Jobs** -- jobs where avg CPU or RAM utilization is below 40%, ranked by cost
- **Cost by Component** -- daily stacked chart of compute, DLC, storage, network, and lease costs
- **Total Cost (Period)** -- single stat for the selected time range
- **Most Expensive Jobs** -- ranked by total credits consumed
- **Cost of Security Scanning** -- isolates jobs with security/scan/audit in their name

### User Engagement
- **Active Users per Month** -- distinct trigger user IDs per month
- **Active Users per Day** -- daily user activity trend
- **Top Credit Consumers (Users)** -- which users are driving the most credit usage

### Workflow and Project Insights
- **Credits by Workflow** -- total credits per workflow with percentage of total
- **Credits by Project** -- total credits per project with pipeline/workflow/job counts
- **Monthly Activity Summary** -- monthly table with pipelines, workflows, jobs, jobs-per-pipeline, active users, and credits
- **Workflow Success Rate Over Time** -- daily success rate per workflow

### Credit Component Trends
- **DLC Credits Over Time** -- Docker Layer Caching credit usage trend
- **Credits by Component** -- stacked comparison of compute, DLC, storage, and network credits

### Credit Burn and Forecast
- **Avg Daily Burn** -- average credits consumed per day in the selected range
- **Projected Monthly Cost** -- extrapolates daily average to a 30-day projection
- **Daily Credit Burn and Cumulative Usage** -- daily credits with 7-day moving average and cumulative total

### Compliance (Audit Log Stream)
- **Audit Events Timeline** -- event volume by action type over time
- **Secret Access and Policy Events** -- context secret access, env var changes, restriction changes
- **Infrastructure and Governance Events** -- project additions, settings changes, schedule modifications, workflow cancellations

## Managing the Credit Cost

The `cost_per_credit` value lives in the `dashboard_config` PostgreSQL table, not in the Grafana dashboard. All cost panels query this table at render time.

**View current value:**

```bash
PGPASSWORD=postgres python3 scripts/update-config.py --list
```

**Update the value:**

```bash
PGPASSWORD=postgres python3 scripts/update-config.py --set cost_per_credit=0.0006
```

**Or directly via SQL:**

```sql
UPDATE dashboard_config SET value = '0.0006', updated_at = NOW() WHERE key = 'cost_per_credit';
```

Changes take effect immediately on the next dashboard refresh.

## Refreshing Data

### Usage API

Run `download-usage.py` for each 32-day window you need, then load all CSVs:

```bash
# Example: download Feb and March separately
python3 scripts/download-usage.py --org-id $ORG_ID --api-token $TOKEN \
    --start-date 2026-02-01 --end-date 2026-02-28 --output-dir ./data

python3 scripts/download-usage.py --org-id $ORG_ID --api-token $TOKEN \
    --start-date 2026-03-01 --end-date 2026-03-31 --output-dir ./data

# Load all (upserts, safe to re-run)
PGPASSWORD=postgres python3 scripts/pg-loader.py --directory ./data --summary
```

To do a clean reload:

```bash
PGPASSWORD=postgres python3 scripts/pg-loader.py --directory ./data --truncate --summary
```

### Audit Logs

Re-run the S3 or local loader. Events are upserted on their `id`, so re-runs are safe.

## Automating with CircleCI Pipelines

An example config is included at `example-circleci-config.yml`. To use it, copy it into your repo as `.circleci/config.yml`. It defines two workflows:

- **`daily-data-load`** -- runs on a schedule trigger, downloads the last 3 days of Usage API data and loads it into PostgreSQL, then loads audit logs from S3.
- **`manual`** -- runs on push to `main`, useful for testing or one-off backfills.

### Required Contexts

Create two contexts in your CircleCI organization settings:

**`circleci-usage-db`** -- PostgreSQL connection details:

| Variable | Description | Example |
|----------|-------------|---------|
| `PGHOST` | PostgreSQL host | `your-db-host.example.com` |
| `PGPORT` | PostgreSQL port | `5432` |
| `PGDATABASE` | Database name | `circleci_usage` |
| `PGUSER` | Database user | `loader` |
| `PGPASSWORD` | Database password | |

**`circleci-api`** -- CircleCI API credentials:

| Variable | Description |
|----------|-------------|
| `CIRCLECI_API_TOKEN` | CircleCI API token with org read access |
| `CIRCLECI_ORG_ID` | Your CircleCI organization UUID |

**`aws-audit-logs`** -- (for audit log loading):

| Variable | Description | Example |
|----------|-------------|---------|
| `AUDIT_LOG_BUCKET` | S3 bucket name | `my-circleci-audit-logs` |
| `AUDIT_LOG_PREFIX` | S3 key prefix (optional) | `org-123/` |
| `AWS_ACCESS_KEY_ID` | AWS credentials | |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials | |
| `AWS_DEFAULT_REGION` | AWS region | `us-east-2` |

### Creating the Schedule Trigger

Use the CircleCI API v2 to create a scheduled pipeline that runs daily. Replace the placeholder values with your own.

**Find your project slug** in CircleCI Project Settings. The format depends on your VCS integration:
- GitHub OAuth: `github/your-org/your-repo`
- GitHub App: `circleci/ORG_UUID/PROJECT_UUID`

**Create the daily schedule:**

```bash
curl --location --request POST "https://circleci.com/api/v2/project/<project-slug>/schedule" \
  --header "Circle-Token: <YOUR_API_TOKEN>" \
  --header "Content-Type: application/json" \
  --data-raw '{
    "name": "Daily data load",
    "description": "Refreshes Usage API and audit log data in PostgreSQL for the Grafana dashboard",
    "attribution-actor": "system",
    "parameters": {
      "branch": "main",
      "run-schedule": true
    },
    "timetable": {
      "per-hour": 1,
      "hours-of-day": [6],
      "days-of-week": ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    }
  }'
```

This runs the pipeline once daily at ~6:00 AM UTC. Adjust `hours-of-day` and `days-of-week` as needed. Times are in UTC, and CircleCI applies a small random offset (up to 10 minutes) that stays consistent across runs.

**Verify the schedule was created:**

```bash
curl --location --request GET "https://circleci.com/api/v2/project/<project-slug>/schedule" \
  --header "Circle-Token: <YOUR_API_TOKEN>"
```

**Delete a schedule** (if needed):

```bash
curl --location --request DELETE "https://circleci.com/api/v2/schedule/<schedule-id>" \
  --header "Circle-Token: <YOUR_API_TOKEN>"
```

### Timetable Reference

The `timetable` object controls scheduling frequency:

| Field | Type | Description |
|-------|------|-------------|
| `per-hour` | integer (1-60) | Number of times to trigger per hour |
| `hours-of-day` | array of integers (0-23) | Which hours (UTC) to run in |
| `days-of-week` | array of strings | `MON`, `TUE`, `WED`, `THU`, `FRI`, `SAT`, `SUN` |

Examples:
- **Daily at 6 AM UTC**: `{"per-hour": 1, "hours-of-day": [6], "days-of-week": ["MON","TUE","WED","THU","FRI","SAT","SUN"]}`
- **Every 6 hours on weekdays**: `{"per-hour": 1, "hours-of-day": [0, 6, 12, 18], "days-of-week": ["MON","TUE","WED","THU","FRI"]}`
- **Once on Monday mornings**: `{"per-hour": 1, "hours-of-day": [8], "days-of-week": ["MON"]}`

### Backfilling Historical Data

For the initial load, you may want to backfill more than 3 days. The Usage API has a max 32-day range per request, so run multiple downloads:

```bash
# Trigger the manual workflow with default parameters (no run-schedule)
curl --location --request POST "https://circleci.com/api/v2/project/<project-slug>/pipeline" \
  --header "Circle-Token: <YOUR_API_TOKEN>" \
  --header "Content-Type: application/json" \
  --data-raw '{"branch": "main"}'
```

Or modify `download-usage.py` arguments in the config to cover a wider date range for the initial run, then revert.

## File Structure

```text
ci-platform-health-dashboard/
├── README.md                              # This file
├── docker-compose.yml                     # PostgreSQL + Grafana
├── dashboard.json                         # Grafana dashboard (import this)
├── .gitignore
├── example-circleci-config.yml             # Example CI config (copy to .circleci/config.yml)
├── provisioning/
│   └── datasources/
│       └── postgres.yml                   # Grafana datasource auto-provisioning
├── scripts/
│   ├── requirements.txt                   # Python dependencies
│   ├── pg-loader.py                       # Usage API CSV -> PostgreSQL
│   ├── audit-log-loader.py                # Audit log JSON -> PostgreSQL
│   ├── download-usage.py                  # CircleCI Usage API downloader
│   ├── update-config.py                   # Backend config management
│   └── build-dashboard.py                 # Dashboard generator (dev tool)
└── data/                                  # CSV/JSON storage (gitignored)
```

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- A CircleCI API token with org-level read access
- Your CircleCI organization UUID
- (For audit logs) An S3 bucket configured for CircleCI audit log streaming, or local JSON exports
