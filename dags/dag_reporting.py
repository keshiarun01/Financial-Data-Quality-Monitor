"""
DAG 3: Reporting & Alerting Pipeline
Schedule: Triggered by DAG 2, or daily at 8:00 AM ET (13:00 UTC) as fallback.

Flow:
  1. Generate daily scorecard summary from QC results
  2. Generate anomaly summary (top anomalies for dashboard)
  3. Dispatch alerts if any CRITICAL/HIGH failures detected

Completes the three-DAG chain:
  DAG 1 (Ingestion) → DAG 2 (Quality Control) → DAG 3 (Reporting)
"""

from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Default args ─────────────────────────────────────────────

default_args = {
    "owner": "fdqm",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ── Helper ───────────────────────────────────────────────────

def _get_run_date(context) -> date:
    return context["logical_date"].date()


# ── Task callables ───────────────────────────────────────────

def generate_daily_scorecard(**context):
    """
    Aggregate today's QC results into a daily scorecard.
    Computes the health score, per-category breakdown,
    and stores the summary for the Streamlit dashboard.
    """
    from sqlalchemy import text
    from src.database.connection import get_engine
    from src.quality.quality_engine import compute_health_score

    run_date = _get_run_date(context)
    engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT check_name, check_category, status, severity,
                   records_checked, records_failed, failure_rate
            FROM qc_results
            WHERE run_date = :run_date
        """), {"run_date": run_date}).fetchall()

    if not rows:
        print(f"No QC results found for {run_date}")
        context["ti"].xcom_push(key="scorecard", value={
            "run_date": str(run_date),
            "health_score": {"score": 0, "color": "RED", "categories": {}},
            "total_checks": 0,
            "results": [],
        })
        return

    results = [
        {
            "check_name": r[0],
            "check_category": r[1],
            "status": r[2],
            "severity": r[3],
            "records_checked": r[4],
            "records_failed": r[5],
            "failure_rate": float(r[6]) if r[6] else 0.0,
        }
        for r in rows
    ]

    health = compute_health_score(results)

    scorecard = {
        "run_date": str(run_date),
        "health_score": health,
        "total_checks": len(results),
        "passed": sum(1 for r in results if r["status"] == "PASS"),
        "warned": sum(1 for r in results if r["status"] == "WARN"),
        "failed": sum(1 for r in results if r["status"] == "FAIL"),
    }

    # Print the daily report
    print(f"\n{'='*60}")
    print(f"DAILY DATA QUALITY SCORECARD — {run_date}")
    print(f"{'='*60}")
    print(f"")
    print(f"  Overall Health:  {health['score']:.1f}% ({health['color']})")
    print(f"  Total Checks:    {scorecard['total_checks']}")
    print(f"  Passed:          {scorecard['passed']}")
    print(f"  Warnings:        {scorecard['warned']}")
    print(f"  Failed:          {scorecard['failed']}")
    print(f"")
    print(f"  Category Breakdown:")
    for cat, info in health.get("categories", {}).items():
        emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(info.get("color"), "⚪")
        print(
            f"    {emoji} {cat:25s} "
            f"Score: {info.get('score', 'N/A'):>5}%  "
            f"P:{info['pass']} W:{info['warn']} F:{info['fail']}"
        )
    print(f"{'='*60}\n")

    context["ti"].xcom_push(key="scorecard", value=scorecard)
    context["ti"].xcom_push(key="health_score", value=health)
    context["ti"].xcom_push(key="failed_checks", value=[
        r for r in results if r["status"] == "FAIL"
    ])
    context["ti"].xcom_push(key="all_results", value=results)

    return scorecard


def generate_anomaly_summary(**context):
    """
    Generate a summary of the most significant anomalies
    detected in the current run, for dashboard display.
    """
    from sqlalchemy import text
    from src.database.connection import get_engine

    run_date = _get_run_date(context)
    engine = get_engine()

    with engine.connect() as conn:
        # Top anomalies by deviation magnitude
        anomalies = conn.execute(text("""
            SELECT
                ticker_or_series,
                observation_date,
                field_name,
                detection_method,
                observed_value,
                expected_value,
                deviation,
                threshold
            FROM anomaly_log
            WHERE observation_date >= :start_date
            ORDER BY ABS(deviation) DESC
            LIMIT 20
        """), {"start_date": run_date - timedelta(days=7)}).fetchall()

        # Anomaly counts by ticker
        counts = conn.execute(text("""
            SELECT
                ticker_or_series,
                detection_method,
                COUNT(*) as anomaly_count
            FROM anomaly_log
            WHERE observation_date >= :start_date
            GROUP BY ticker_or_series, detection_method
            ORDER BY anomaly_count DESC
        """), {"start_date": run_date - timedelta(days=7)}).fetchall()

    summary = {
        "run_date": str(run_date),
        "top_anomalies": [
            {
                "ticker": a[0],
                "date": str(a[1]),
                "field": a[2],
                "method": a[3],
                "observed": float(a[4]) if a[4] else None,
                "expected": float(a[5]) if a[5] else None,
                "deviation": float(a[6]) if a[6] else None,
            }
            for a in anomalies
        ],
        "anomaly_counts": [
            {
                "ticker": c[0],
                "method": c[1],
                "count": c[2],
            }
            for c in counts
        ],
        "total_anomalies": sum(c[2] for c in counts),
    }

    print(f"\nAnomaly Summary ({run_date}):")
    print(f"  Total anomalies (last 7 days): {summary['total_anomalies']}")
    if summary["top_anomalies"]:
        print(f"  Top anomaly: {summary['top_anomalies'][0]['ticker']} "
              f"deviation={summary['top_anomalies'][0]['deviation']:.2f}")
    for c in summary["anomaly_counts"][:5]:
        print(f"  {c['ticker']:10s} {c['method']:20s} count={c['count']}")

    context["ti"].xcom_push(key="anomaly_summary", value=summary)
    return summary


def dispatch_alerts_task(**context):
    """
    Check if any critical failures warrant an alert and dispatch
    to configured channels (log + email if enabled).
    """
    from src.utils.alerting import dispatch_alerts

    run_date = _get_run_date(context)
    ti = context["ti"]

    health_score = ti.xcom_pull(
        task_ids="generate_daily_scorecard", key="health_score"
    )
    failed_checks = ti.xcom_pull(
        task_ids="generate_daily_scorecard", key="failed_checks"
    )
    all_results = ti.xcom_pull(
        task_ids="generate_daily_scorecard", key="all_results"
    )

    if not health_score:
        print("No health score available — skipping alerts")
        return {"alert_level": "NONE", "skipped": True}

    total_checks = len(all_results) if all_results else 0
    failed = failed_checks or []

    # Only dispatch if there are actual failures
    if not failed:
        print(f"All {total_checks} checks passed — no alerts needed")
        return {"alert_level": "LOW", "total_failures": 0}

    result = dispatch_alerts(
        run_date=run_date,
        health_score=health_score,
        failed_checks=failed,
        total_checks=total_checks,
    )

    context["ti"].xcom_push(key="alert_result", value={
        "alert_level": result["alert_level"],
        "email_sent": result["email_sent"],
        "total_failures": len(failed),
    })

    return result


def generate_daily_report_log(**context):
    """
    Final task: prints a consolidated end-of-pipeline summary.
    This serves as the audit trail for each daily run.
    """
    run_date = _get_run_date(context)
    ti = context["ti"]

    scorecard = ti.xcom_pull(
        task_ids="generate_daily_scorecard", key="scorecard"
    )
    anomaly_summary = ti.xcom_pull(
        task_ids="generate_anomaly_summary", key="anomaly_summary"
    )
    alert_result = ti.xcom_pull(
        task_ids="dispatch_alerts", key="alert_result"
    )

    print(f"\n{'#'*60}")
    print(f"# FDQM DAILY PIPELINE COMPLETE — {run_date}")
    print(f"{'#'*60}")
    print(f"#")

    if scorecard:
        hs = scorecard.get("health_score", {})
        print(f"#  Health Score:     {hs.get('score', 'N/A')}% ({hs.get('color', '?')})")
        print(f"#  Checks:           {scorecard.get('total_checks', 0)} total")
        print(f"#  Results:          P:{scorecard.get('passed', 0)} "
              f"W:{scorecard.get('warned', 0)} F:{scorecard.get('failed', 0)}")

    if anomaly_summary:
        print(f"#  Anomalies (7d):   {anomaly_summary.get('total_anomalies', 0)}")

    if alert_result:
        print(f"#  Alert Level:      {alert_result.get('alert_level', 'NONE')}")
        print(f"#  Email Sent:       {alert_result.get('email_sent', False)}")

    print(f"#")
    print(f"#  Pipeline:  DAG 1 (Ingestion)")
    print(f"#          → DAG 2 (Quality Control)")
    print(f"#          → DAG 3 (Reporting) ✓ COMPLETE")
    print(f"#")
    print(f"{'#'*60}\n")

    return {
        "run_date": str(run_date),
        "status": "COMPLETE",
        "health_score": scorecard.get("health_score", {}).get("score") if scorecard else None,
    }


# ── DAG Definition ───────────────────────────────────────────

with DAG(
    dag_id="dag_reporting",
    default_args=default_args,
    description="Daily reporting: scorecard generation, anomaly summary, and alerting",
    schedule_interval="0 13 * * *",  # 8:00 AM ET = 13:00 UTC (fallback)
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    tags=["reporting", "alerting", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Scorecard generation ─────────────────────────────
    scorecard_task = PythonOperator(
        task_id="generate_daily_scorecard",
        python_callable=generate_daily_scorecard,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Anomaly summary ──────────────────────────────────
    anomaly_task = PythonOperator(
        task_id="generate_anomaly_summary",
        python_callable=generate_anomaly_summary,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Alert dispatch ───────────────────────────────────
    alert_task = PythonOperator(
        task_id="dispatch_alerts",
        python_callable=dispatch_alerts_task,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Final report log ─────────────────────────────────
    report_log = PythonOperator(
        task_id="generate_daily_report_log",
        python_callable=generate_daily_report_log,
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(task_id="end")

    # ── Dependencies ─────────────────────────────────────
    # Scorecard and anomaly summary run in parallel
    start >> [scorecard_task, anomaly_task]

    # Alerts depend on scorecard (needs health score + failed checks)
    scorecard_task >> alert_task

    # Final log waits for everything
    [alert_task, anomaly_task] >> report_log >> end