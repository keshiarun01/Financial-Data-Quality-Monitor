"""
DAG 2: Quality Control & Anomaly Detection Pipeline
Schedule: Triggered by DAG 1, or daily at 7:00 AM ET (12:00 UTC) as fallback.

Flow:
  1. Run null checks across all tables
  2. Run stale data detection
  3. Run anomaly detection (z-score + rolling window) — equities & macro
  4. Run cross-source reconciliation (Yahoo vs Alpha Vantage)
  5. Run holiday-aware gap detection
  6. Aggregate results and compute health score
  7. Trigger Reporting DAG
"""

from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Default args ─────────────────────────────────────────────

default_args = {
    "owner": "fdqm",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# ── Helper ───────────────────────────────────────────────────

def _get_run_date(context) -> date:
    return context["logical_date"].date()


# ── Task callables ───────────────────────────────────────────

def run_ge_validations_task(**context):
    """Run all Great Expectations validation suites."""
    from src.quality.ge_validation import run_all_ge_validations
    run_date = _get_run_date(context)
    results = run_all_ge_validations(run_date)
    context["ti"].xcom_push(key="ge_results", value={
        "suites_run": len(results),
        "all_passed": all(r["success"] for r in results),
        "total_expectations": sum(r["evaluated"] for r in results),
        "total_passed": sum(r["successful"] for r in results),
    })
    return results


def run_null_checks_task(**context):
    from src.quality.null_checks import run_all_null_checks
    run_date = _get_run_date(context)
    results = run_all_null_checks(run_date)
    context["ti"].xcom_push(key="null_results", value={
        "pass": sum(1 for r in results if r["status"] == "PASS"),
        "warn": sum(1 for r in results if r["status"] == "WARN"),
        "fail": sum(1 for r in results if r["status"] == "FAIL"),
    })
    return results


def run_stale_checks_task(**context):
    from src.quality.stale_detection import run_all_stale_checks
    run_date = _get_run_date(context)
    results = run_all_stale_checks(run_date)
    context["ti"].xcom_push(key="stale_results", value={
        "pass": sum(1 for r in results if r["status"] == "PASS"),
        "warn": sum(1 for r in results if r["status"] == "WARN"),
        "fail": sum(1 for r in results if r["status"] == "FAIL"),
    })
    return results


def run_equity_anomaly_task(**context):
    from src.quality.anomaly_detection import run_equity_anomaly_detection
    run_date = _get_run_date(context)
    results = run_equity_anomaly_detection(run_date)
    context["ti"].xcom_push(key="equity_anomaly_results", value={
        "total_anomalies": sum(r.get("records_failed", 0) for r in results),
    })
    return results


def run_macro_anomaly_task(**context):
    from src.quality.anomaly_detection import run_macro_anomaly_detection
    run_date = _get_run_date(context)
    results = run_macro_anomaly_detection(run_date)
    context["ti"].xcom_push(key="macro_anomaly_results", value={
        "total_anomalies": sum(r.get("records_failed", 0) for r in results),
    })
    return results


def run_reconciliation_task(**context):
    from src.quality.cross_source_recon import run_all_reconciliation
    run_date = _get_run_date(context)
    results = run_all_reconciliation(run_date)
    context["ti"].xcom_push(key="recon_results", value={
        "total_breaks": sum(r.get("records_failed", 0) for r in results),
    })
    return results


def run_gap_detection_task(**context):
    from src.quality.gap_detection import run_all_gap_detection
    run_date = _get_run_date(context)
    results = run_all_gap_detection(run_date)
    context["ti"].xcom_push(key="gap_results", value={
        "total_missing": sum(r.get("records_failed", 0) for r in results),
    })
    return results


def aggregate_results_task(**context):
    """
    Pull results from all upstream tasks and compute the
    overall health score.
    """
    from src.quality.quality_engine import compute_health_score
    from sqlalchemy import text
    from src.database.connection import get_engine

    run_date = _get_run_date(context)
    engine = get_engine()

    # Pull today's QC results from the database
    query = text("""
        SELECT check_name, check_category, status, severity,
               records_checked, records_failed
        FROM qc_results
        WHERE run_date = :run_date
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"run_date": run_date}).fetchall()

    results = [
        {
            "check_name": r[0],
            "check_category": r[1],
            "status": r[2],
            "severity": r[3],
            "records_checked": r[4],
            "records_failed": r[5],
        }
        for r in rows
    ]

    health = compute_health_score(results)

    print(f"\n{'='*60}")
    print(f"DAILY DATA HEALTH REPORT — {run_date}")
    print(f"{'='*60}")
    print(f"Overall Score: {health['score']}% ({health['color']})")
    print(f"Total Checks:  {len(results)}")
    print(f"\nPer-Category Breakdown:")
    for cat, info in health["categories"].items():
        print(
            f"  {cat:20s}  Score: {info.get('score', 'N/A'):>5}%  "
            f"({info.get('color', '?'):6s})  "
            f"P:{info['pass']} W:{info['warn']} F:{info['fail']}"
        )
    print(f"{'='*60}\n")

    context["ti"].xcom_push(key="health_score", value=health)
    return health


# ── DAG Definition ───────────────────────────────────────────

with DAG(
    dag_id="dag_quality_control",
    default_args=default_args,
    description="Data quality checks: nulls, staleness, anomalies, reconciliation, gaps",
    schedule_interval="0 12 * * *",  # 7:00 AM ET = 12:00 UTC
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "anomaly-detection", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Great Expectations validation (runs first) ───────
    ge_validation = PythonOperator(
        task_id="run_ge_validations",
        python_callable=run_ge_validations_task,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Parallel quality checks ──────────────────────────
    null_checks = PythonOperator(
        task_id="run_null_checks",
        python_callable=run_null_checks_task,
        execution_timeout=timedelta(minutes=5),
    )

    stale_checks = PythonOperator(
        task_id="run_stale_checks",
        python_callable=run_stale_checks_task,
        execution_timeout=timedelta(minutes=5),
    )

    equity_anomaly = PythonOperator(
        task_id="detect_equity_anomalies",
        python_callable=run_equity_anomaly_task,
        execution_timeout=timedelta(minutes=10),
    )

    macro_anomaly = PythonOperator(
        task_id="detect_macro_anomalies",
        python_callable=run_macro_anomaly_task,
        execution_timeout=timedelta(minutes=10),
    )

    recon = PythonOperator(
        task_id="run_reconciliation",
        python_callable=run_reconciliation_task,
        execution_timeout=timedelta(minutes=10),
    )

    gap_detect = PythonOperator(
        task_id="run_gap_detection",
        python_callable=run_gap_detection_task,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Aggregation ──────────────────────────────────────
    aggregate = PythonOperator(
        task_id="aggregate_results",
        python_callable=aggregate_results_task,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Trigger reporting ────────────────────────────────
    trigger_reporting = TriggerDagRunOperator(
        task_id="trigger_reporting_dag",
        trigger_dag_id="dag_reporting",
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(task_id="end")

    # ── Dependencies ─────────────────────────────────────
    # GE runs first, then all custom checks run in parallel
    start >> ge_validation
    ge_validation >> [null_checks, stale_checks, equity_anomaly,
                      macro_anomaly, recon, gap_detect]

    # All checks must complete before aggregation
    [null_checks, stale_checks, equity_anomaly,
     macro_anomaly, recon, gap_detect] >> aggregate

    aggregate >> trigger_reporting >> end