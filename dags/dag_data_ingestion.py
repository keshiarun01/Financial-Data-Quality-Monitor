"""
DAG 1: Data Ingestion Pipeline
Schedule: Daily at 6:00 AM ET (11:00 UTC)

Flow:
  1. Check if today is a trading day (skip on weekends/holidays)
  2. If market open:
     a. Ingest equities from Yahoo Finance
     b. Ingest equities from Alpha Vantage (recon source)
     c. Ingest daily FRED series (Treasury yields, USD index)
  3. On 1st business day of month: ingest monthly FRED series
  4. Log results and trigger the Quality Control DAG

Uses BranchPythonOperator for market-open logic to avoid
wasting API calls on holidays/weekends.
"""

from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import sys
import os

# Ensure src/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Default args ─────────────────────────────────────────────

default_args = {
    "owner": "fdqm",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ── Helper functions ─────────────────────────────────────────

def _get_logical_date(context) -> date:
    """Extract the logical (execution) date from Airflow context."""
    logical_date = context["logical_date"]
    return logical_date.date()


def check_market_open(**context) -> str:
    """
    BranchPythonOperator callable.
    Returns the task_id of the next task based on whether
    today is a trading day.
    """
    from src.utils.market_calendar import is_trading_day, get_holidays_from_db

    logical_date = _get_logical_date(context)
    holidays = get_holidays_from_db(
        logical_date - timedelta(days=5),
        logical_date + timedelta(days=5),
    )

    if is_trading_day(logical_date, holidays=holidays):
        return "ingest_equities_yahoo"
    else:
        return "log_holiday_skip"


def ingest_equities_yahoo_task(**context):
    """Ingest equity OHLCV data from Yahoo Finance."""
    from src.ingestion.yahoo_finance import ingest_equities

    logical_date = _get_logical_date(context)
    result = ingest_equities(
        start_date=logical_date,
        end_date=logical_date,
        dag_id="dag_data_ingestion",
        task_id="ingest_equities_yahoo",
    )
    context["ti"].xcom_push(key="yahoo_result", value=result)
    return result


def ingest_equities_recon_task(**context):
    """Ingest equity close/volume from Alpha Vantage for reconciliation."""
    from src.ingestion.alpha_vantage import ingest_equities_recon

    logical_date = _get_logical_date(context)
    result = ingest_equities_recon(
        start_date=logical_date,
        end_date=logical_date,
        dag_id="dag_data_ingestion",
        task_id="ingest_equities_recon",
    )
    context["ti"].xcom_push(key="av_result", value=result)
    return result


def ingest_fred_daily_task(**context):
    """Ingest daily FRED series (Treasury yields, USD index)."""
    from src.ingestion.fred_api import ingest_fred_daily

    logical_date = _get_logical_date(context)
    result = ingest_fred_daily(
        start_date=logical_date,
        end_date=logical_date,
        dag_id="dag_data_ingestion",
        task_id="ingest_fred_daily",
    )
    context["ti"].xcom_push(key="fred_daily_result", value=result)
    return result


def ingest_fred_monthly_task(**context):
    """
    Ingest monthly FRED series (CPI, unemployment, PCE, sentiment).
    Fetches the full current month to capture any new releases.
    """
    from src.ingestion.fred_api import ingest_fred_monthly

    logical_date = _get_logical_date(context)
    # Fetch from 1st of previous month to capture late releases
    start_of_prev_month = (logical_date.replace(day=1) - timedelta(days=1)).replace(day=1)

    result = ingest_fred_monthly(
        start_date=start_of_prev_month,
        end_date=logical_date,
        dag_id="dag_data_ingestion",
        task_id="ingest_fred_monthly",
    )
    context["ti"].xcom_push(key="fred_monthly_result", value=result)
    return result


def check_is_first_business_day(**context) -> str:
    """
    Determine if today is the 1st business day of the month.
    Used to decide whether to run monthly FRED ingestion.
    """
    from src.utils.market_calendar import get_expected_trading_days

    logical_date = _get_logical_date(context)
    first_of_month = logical_date.replace(day=1)

    # Get first 5 trading days of the month
    trading_days = get_expected_trading_days(
        first_of_month,
        first_of_month + timedelta(days=10),
    )

    if trading_days and logical_date == trading_days[0]:
        return "ingest_fred_monthly"
    else:
        return "skip_monthly_fred"


def log_holiday_skip(**context):
    """Log that ingestion was skipped due to market holiday."""
    from src.utils.market_calendar import get_holidays_from_db

    logical_date = _get_logical_date(context)
    holidays = get_holidays_from_db(logical_date, logical_date)

    if logical_date in holidays:
        reason = "market holiday"
    elif logical_date.weekday() >= 5:
        reason = "weekend"
    else:
        reason = "unknown"

    print(f"Ingestion skipped: {logical_date} is a {reason}")
    return {"skipped": True, "reason": reason, "date": str(logical_date)}


# ── DAG Definition ───────────────────────────────────────────

with DAG(
    dag_id="dag_data_ingestion",
    default_args=default_args,
    description="Daily financial data ingestion from Yahoo Finance, FRED, and Alpha Vantage",
    schedule_interval="0 11 * * *",  # 6:00 AM ET = 11:00 UTC
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "financial-data", "daily"],
) as dag:

    # ── Market open check ────────────────────────────────────
    branch_market_open = BranchPythonOperator(
        task_id="check_market_open",
        python_callable=check_market_open,
    )

    # ── Holiday skip path ────────────────────────────────────
    holiday_skip = PythonOperator(
        task_id="log_holiday_skip",
        python_callable=log_holiday_skip,
    )

    # ── Market open path: parallel ingestion ─────────────────
    yahoo_task = PythonOperator(
        task_id="ingest_equities_yahoo",
        python_callable=ingest_equities_yahoo_task,
        execution_timeout=timedelta(minutes=10),
    )

    av_task = PythonOperator(
        task_id="ingest_equities_recon",
        python_callable=ingest_equities_recon_task,
        execution_timeout=timedelta(minutes=15),  # Longer due to rate limits
    )

    fred_daily_task = PythonOperator(
        task_id="ingest_fred_daily",
        python_callable=ingest_fred_daily_task,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Monthly FRED check ───────────────────────────────────
    branch_monthly = BranchPythonOperator(
        task_id="check_first_business_day",
        python_callable=check_is_first_business_day,
        trigger_rule="none_failed_min_one_success",
    )

    fred_monthly_task = PythonOperator(
        task_id="ingest_fred_monthly",
        python_callable=ingest_fred_monthly_task,
        execution_timeout=timedelta(minutes=10),
    )

    skip_monthly = EmptyOperator(
        task_id="skip_monthly_fred",
    )

    # ── Join point ───────────────────────────────────────────
    join = EmptyOperator(
        task_id="ingestion_complete",
        trigger_rule="none_failed_min_one_success",
    )

    # ── Trigger Quality Control DAG ──────────────────────────
    trigger_qc = TriggerDagRunOperator(
        task_id="trigger_quality_dag",
        trigger_dag_id="dag_quality_control",
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Task Dependencies ────────────────────────────────────

    # Market open branching
    branch_market_open >> [yahoo_task, holiday_skip]

    # If market is open: run all three ingesters in parallel
    yahoo_task >> av_task  # AV after Yahoo (rate limit friendly)
    yahoo_task >> fred_daily_task

    # After daily ingestion, check if monthly FRED is needed
    [av_task, fred_daily_task] >> branch_monthly
    branch_monthly >> [fred_monthly_task, skip_monthly]

    # Everything joins before triggering QC
    [fred_monthly_task, skip_monthly, holiday_skip] >> join
    join >> trigger_qc