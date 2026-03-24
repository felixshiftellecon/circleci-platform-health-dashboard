#!/usr/bin/env python3
"""
Build the CI/CD Platform Health dashboard from a base dashboard.

Reads an upstream dashboard JSON and transforms it:
1. Swaps datasource UIDs to the local provisioned datasource
2. Removes demo-specific panels (TIA, Flaky Test Fix)
3. Replaces $cost_per_credit template variable with a DB subquery
4. Adds new panels for user engagement, workflow/project insights
5. Removes the cost_per_credit textbox from Grafana template variables

Set BASE_DASHBOARD_PATH to point at the source dashboard JSON before running.
"""

import json
import sys
import os
import re
import copy

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

BASE_DASHBOARD_PATH = os.environ.get(
    "BASE_DASHBOARD_PATH",
    os.path.join(PROJECT_DIR, "base-dashboard-source.json"),
)
OUTPUT_PATH = os.path.join(PROJECT_DIR, "dashboard.json")

OLD_DS_UID = "P66BDC2B81169D854"
NEW_DS_UID = "circleci-pg-ds"
DS_TYPE = "grafana-postgresql-datasource"

COST_SUBQUERY = "(SELECT value::numeric FROM dashboard_config WHERE key = 'cost_per_credit')"

PANELS_TO_REMOVE = [
    "Smarter Testing",
    "Tests Run",
    "Tests Selected",
    "Tests Skipped",
    "Duration — Traditional",
    "Duration — TIA",
    "Time Saved",
    "Per-Pipeline Duration",
    "Branch Comparison",
    "Flaky Test Fix",
]


def should_remove_panel(panel):
    title = panel.get("title", "")
    for pattern in PANELS_TO_REMOVE:
        if pattern in title:
            return True
    return False


def replace_datasource_uid(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "uid" and v == OLD_DS_UID:
                obj[k] = NEW_DS_UID
            else:
                replace_datasource_uid(v)
    elif isinstance(obj, list):
        for item in obj:
            replace_datasource_uid(item)


def replace_cost_variable(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and "$cost_per_credit" in v:
                obj[k] = v.replace("$cost_per_credit", COST_SUBQUERY)
            else:
                replace_cost_variable(v)
    elif isinstance(obj, list):
        for item in obj:
            replace_cost_variable(item)


def make_ds_ref():
    return {"type": DS_TYPE, "uid": NEW_DS_UID}


def stat_panel(id, title, sql, unit=None, color="purple", gridPos=None, description=None):
    p = {
        "datasource": make_ds_ref(),
        "fieldConfig": {
            "defaults": {
                "color": {"fixedColor": color, "mode": "fixed"},
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": 0}]},
            },
            "overrides": [],
        },
        "gridPos": gridPos or {"h": 4, "w": 4, "x": 0, "y": 0},
        "id": id,
        "options": {
            "colorMode": "background_solid",
            "graphMode": "none",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "showPercentChange": False,
            "textMode": "auto",
        },
        "pluginVersion": "12.2.1",
        "targets": [{"datasource": make_ds_ref(), "editorMode": "code", "format": "table", "rawQuery": True, "rawSql": sql, "refId": "A"}],
        "title": title,
        "type": "stat",
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if description:
        p["description"] = description
    return p


def table_panel(id, title, sql, gridPos=None, overrides=None):
    p = {
        "datasource": make_ds_ref(),
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "displayMode": "auto", "inspect": True}}, "overrides": overrides or []},
        "gridPos": gridPos or {"h": 8, "w": 12, "x": 0, "y": 0},
        "id": id,
        "options": {"showHeader": True, "sortBy": []},
        "pluginVersion": "12.2.1",
        "targets": [{"datasource": make_ds_ref(), "editorMode": "code", "format": "table", "rawQuery": True, "rawSql": sql, "refId": "A"}],
        "title": title,
        "type": "table",
    }
    return p


def timeseries_panel(id, title, sql, gridPos=None, draw_style="line", fill=20, unit=None):
    p = {
        "datasource": make_ds_ref(),
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {
                    "axisBorderShow": False, "axisCenteredZero": False,
                    "axisColorMode": "text", "axisLabel": "", "axisPlacement": "auto",
                    "barAlignment": 0, "barWidthFactor": 0.9,
                    "drawStyle": draw_style, "fillOpacity": fill,
                    "gradientMode": "opacity",
                    "hideFrom": {"legend": False, "tooltip": False, "viz": False},
                    "insertNulls": False, "lineInterpolation": "smooth",
                    "lineWidth": 2, "pointSize": 5,
                    "scaleDistribution": {"type": "linear"},
                    "showPoints": "auto", "showValues": False, "spanNulls": False,
                    "stacking": {"group": "A", "mode": "none"},
                    "thresholdsStyle": {"mode": "off"},
                },
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": 0}]},
            },
            "overrides": [],
        },
        "gridPos": gridPos or {"h": 8, "w": 24, "x": 0, "y": 0},
        "id": id,
        "options": {
            "legend": {"calcs": [], "displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"hideZeros": False, "mode": "multi", "sort": "desc"},
        },
        "pluginVersion": "12.2.1",
        "targets": [{"datasource": make_ds_ref(), "editorMode": "code", "format": "table", "rawQuery": True, "rawSql": sql, "refId": "A"}],
        "title": title,
        "type": "timeseries",
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    return p


def row_panel(id, title, y):
    return {
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
        "id": id,
        "panels": [],
        "title": title,
        "type": "row",
    }


def build_new_panels(start_id, start_y):
    panels = []
    y = start_y
    pid = start_id

    # -- User Engagement row --
    panels.append(row_panel(pid, "User Engagement", y)); pid += 1; y += 1

    panels.append(timeseries_panel(
        pid, "Active Users per Month",
        "SELECT DATE_TRUNC('month', pipeline_created_at)::date as time, "
        "COUNT(DISTINCT pipeline_trigger_user_id) as active_users "
        "FROM circleci_usage "
        "WHERE pipeline_created_at IS NOT NULL AND pipeline_trigger_user_id IS NOT NULL "
        "GROUP BY 1 ORDER BY 1",
        gridPos={"h": 8, "w": 12, "x": 0, "y": y},
        draw_style="bars", fill=60,
    )); pid += 1

    panels.append(timeseries_panel(
        pid, "Active Users per Day",
        "SELECT pipeline_created_at::date as time, "
        "COUNT(DISTINCT pipeline_trigger_user_id) as active_users "
        "FROM circleci_usage "
        "WHERE pipeline_created_at IS NOT NULL AND pipeline_trigger_user_id IS NOT NULL "
        "GROUP BY 1 ORDER BY 1",
        gridPos={"h": 8, "w": 12, "x": 12, "y": y},
    )); pid += 1; y += 8

    panels.append(table_panel(
        pid, "Top Credit Consumers (Users)",
        "SELECT pipeline_trigger_user_id as user_id, "
        "COUNT(*) as total_jobs, "
        "SUM(total_credits) as total_credits, "
        f"SUM(total_credits) * {COST_SUBQUERY} as estimated_cost "
        "FROM circleci_usage "
        "WHERE pipeline_created_at >= $__timeFrom() AND pipeline_created_at <= $__timeTo() "
        "AND pipeline_trigger_user_id IS NOT NULL "
        "GROUP BY pipeline_trigger_user_id "
        "ORDER BY total_credits DESC LIMIT 25",
        gridPos={"h": 8, "w": 24, "x": 0, "y": y},
    )); pid += 1; y += 8

    # -- Workflow & Project Insights row --
    panels.append(row_panel(pid, "Workflow & Project Insights", y)); pid += 1; y += 1

    panels.append(table_panel(
        pid, "Credits by Workflow",
        "SELECT workflow_name, "
        "COUNT(DISTINCT workflow_id) as workflow_runs, "
        "SUM(total_credits) as total_credits, "
        f"SUM(total_credits) * {COST_SUBQUERY} as estimated_cost, "
        "ROUND(100.0 * SUM(total_credits) / NULLIF((SELECT SUM(total_credits) FROM circleci_usage "
        "WHERE pipeline_created_at >= $__timeFrom() AND pipeline_created_at <= $__timeTo()), 0), 2) as pct_of_total "
        "FROM circleci_usage "
        "WHERE pipeline_created_at >= $__timeFrom() AND pipeline_created_at <= $__timeTo() "
        "AND workflow_name IS NOT NULL "
        "GROUP BY workflow_name ORDER BY total_credits DESC LIMIT 25",
        gridPos={"h": 8, "w": 12, "x": 0, "y": y},
    )); pid += 1

    panels.append(table_panel(
        pid, "Credits by Project",
        "SELECT project_name, "
        "COUNT(DISTINCT pipeline_id) as pipelines, "
        "COUNT(DISTINCT workflow_id) as workflows, "
        "COUNT(*) as total_jobs, "
        "SUM(total_credits) as total_credits, "
        f"SUM(total_credits) * {COST_SUBQUERY} as estimated_cost "
        "FROM circleci_usage "
        "WHERE pipeline_created_at >= $__timeFrom() AND pipeline_created_at <= $__timeTo() "
        "AND project_name IS NOT NULL "
        "GROUP BY project_name ORDER BY total_credits DESC LIMIT 25",
        gridPos={"h": 8, "w": 12, "x": 12, "y": y},
    )); pid += 1; y += 8

    panels.append(table_panel(
        pid, "Monthly Activity Summary",
        "SELECT DATE_TRUNC('month', pipeline_created_at)::date as month, "
        "COUNT(DISTINCT pipeline_id) as pipelines, "
        "COUNT(DISTINCT workflow_id) as workflows, "
        "COUNT(*) as jobs, "
        "ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT pipeline_id), 0), 1) as jobs_per_pipeline, "
        "COUNT(DISTINCT pipeline_trigger_user_id) as active_users, "
        "SUM(total_credits) as total_credits, "
        f"SUM(total_credits) * {COST_SUBQUERY} as estimated_cost "
        "FROM circleci_usage "
        "WHERE pipeline_created_at IS NOT NULL "
        "GROUP BY 1 ORDER BY 1 DESC",
        gridPos={"h": 8, "w": 24, "x": 0, "y": y},
    )); pid += 1; y += 8

    panels.append(timeseries_panel(
        pid, "Workflow Success Rate Over Time",
        "SELECT pipeline_created_at::date as time, "
        "workflow_name, "
        "ROUND(100.0 * SUM(CASE WHEN job_build_status = 'success' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) as success_pct "
        "FROM circleci_usage "
        "WHERE pipeline_created_at IS NOT NULL AND workflow_name IS NOT NULL AND job_build_status IS NOT NULL "
        "GROUP BY pipeline_created_at::date, workflow_name "
        "HAVING COUNT(*) >= 3 "
        "ORDER BY time",
        gridPos={"h": 8, "w": 24, "x": 0, "y": y},
        unit="percent",
    )); pid += 1; y += 8

    # -- DLC & Credit Component Trends row --
    panels.append(row_panel(pid, "Credit Component Trends", y)); pid += 1; y += 1

    panels.append(timeseries_panel(
        pid, "DLC Credits Over Time",
        "SELECT pipeline_created_at::date as time, "
        "SUM(dlc_credits) as dlc_credits "
        "FROM circleci_usage "
        "WHERE pipeline_created_at IS NOT NULL "
        "GROUP BY 1 ORDER BY 1",
        gridPos={"h": 8, "w": 12, "x": 0, "y": y},
        draw_style="bars", fill=40,
    )); pid += 1

    panels.append(timeseries_panel(
        pid, "Credits by Component",
        "SELECT pipeline_created_at::date as time, "
        "SUM(compute_credits) as compute, "
        "SUM(dlc_credits) as dlc, "
        "SUM(storage_credits) as storage, "
        "SUM(network_credits) as network "
        "FROM circleci_usage "
        "WHERE pipeline_created_at IS NOT NULL "
        "GROUP BY 1 ORDER BY 1",
        gridPos={"h": 8, "w": 12, "x": 12, "y": y},
        draw_style="line", fill=30,
    )); pid += 1; y += 8

    return panels, pid, y


def main():
    with open(BASE_DASHBOARD_PATH) as f:
        db = json.load(f)

    # 1. Replace datasource UIDs
    replace_datasource_uid(db)

    # 2. Replace $cost_per_credit with DB subquery
    replace_cost_variable(db)

    # 3. Remove demo-specific panels, recompute y positions
    filtered = []
    for panel in db["panels"]:
        if not should_remove_panel(panel):
            filtered.append(panel)
    db["panels"] = filtered

    # Recompute y positions after removal
    y = 0
    for panel in db["panels"]:
        panel["gridPos"]["y"] = y
        if panel["type"] == "row":
            y += 1
        else:
            y += panel["gridPos"]["h"]

    # 4. Find max panel id for new panels
    max_id = max(p["id"] for p in db["panels"])

    # 5. Add new panels
    new_panels, _, _ = build_new_panels(max_id + 1, y)
    db["panels"].extend(new_panels)

    # 6. Clean up template variables
    if "templating" in db and "list" in db["templating"]:
        db["templating"]["list"] = [
            v for v in db["templating"]["list"]
            if v.get("name") != "cost_per_credit"
        ]
        for v in db["templating"]["list"]:
            if v.get("name") == "project_name":
                v["current"] = {"text": "All", "value": "$__all"}

    # 7. Reset dashboard metadata for clean import
    db["uid"] = None
    db["id"] = None
    db["version"] = 1
    db["title"] = "CI/CD Platform Health"
    db["tags"] = ["circleci", "platform-health", "usage"]

    with open(OUTPUT_PATH, "w") as f:
        json.dump(db, f, indent=2)

    print(f"Dashboard written to {OUTPUT_PATH}")
    print(f"  Total panels: {len(db['panels'])}")


if __name__ == "__main__":
    main()
