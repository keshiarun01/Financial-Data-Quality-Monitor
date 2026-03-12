"""
Stale data detection.
Checks whether the latest data from each source is within
expected freshness windows. Accounts for weekends and holidays.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional

from sqlalchemy import text

from src.database.connection import get_engine, get_session
from src.models.schema import QCResult
from src.utils.config import get_settings
from src.utils.market_calendar import get_previous_trading_day

import structlog

logger = structlog.get_logger(__name__)

# ── Freshness rules ──────────────────────────────────────────
# Each source defines how fresh data should be relative to
# the previous trading day.
FRESHNESS_RULES = [
    {
        "name": "equities_yahoo",
        "table": "equities",
        "date_column": "trade_date",
        "filter_clause": "source = 'yahoo_finance'",
        "warn_lag_days": 2,
        "fail_lag_days": 3,
        "severity": "CRITICAL",
        "frequency": "daily",
    },
    {
        "name": "equities_recon_av",
        "table": "equities_recon",
        "date_column": "trade_date",
        "filter_clause": "source = 'alpha_vantage'",
        "warn_lag_days": 2,
        "fail_lag_days": 3,
        "severity": "HIGH",
        "frequency": "daily",
    },
    {
        "name": "fred_daily_dgs10",
        "table": "macro_indicators",
        "date_column": "observation_date",
        "filter_clause": "series_id = 'DGS10'",
        "warn_lag_days": 3,
        "fail_lag_days": 5,
        "severity": "HIGH",
        "frequency": "daily",
    },
    {
        "name": "fred_daily_dgs2",
        "table": "macro_indicators",
        "date_column": "observation_date",
        "filter_clause": "series_id = 'DGS2'",
        "warn_lag_days": 3,
        "fail_lag_days": 5,
        "severity": "HIGH",
        "frequency": "daily",
    },
    {
        "name": "fred_daily_usd",
        "table": "macro_indicators",
        "date_column": "observation_date",
        "filter_clause": "series_id = 'DTWEXBGS'",
        "warn_lag_days": 3,
        "fail_lag_days": 5,
        "severity": "MEDIUM",
        "frequency": "daily",
    },
    {
        "name": "fred_monthly_cpi",
        "table": "macro_indicators",
        "date_column": "observation_date",
        "filter_clause": "series_id = 'CPIAUCSL'",
        "warn_lag_days": 45,
        "fail_lag_days": 60,
        "severity": "MEDIUM",
        "frequency": "monthly",
    },
    {
        "name": "fred_monthly_unemployment",
        "table": "macro_indicators",
        "date_column": "observation_date",
        "filter_clause": "series_id = 'UNRATE'",
        "warn_lag_days": 40,
        "fail_lag_days": 55,
        "severity": "MEDIUM",
        "frequency": "monthly",
    },
    {
        "name": "fred_monthly_pce",
        "table": "macro_indicators",
        "date_column": "observation_date",
        "filter_clause": "series_id = 'PCE'",
        "warn_lag_days": 45,
        "fail_lag_days": 60,
        "severity": "MEDIUM",
        "frequency": "monthly",
    },
    {
        "name": "fred_monthly_sentiment",
        "table": "macro_indicators",
        "date_column": "observation_date",
        "filter_clause": "series_id = 'UMCSENT'",
        "warn_lag_days": 35,
        "fail_lag_days": 50,
        "severity": "LOW",
        "frequency": "monthly",
    },
]


def check_staleness(
    rule: Dict,
    as_of_date: date,
) -> Dict:
    """
    Check if data for a given source is stale.

    Compares the most recent data date against the expected
    freshness window relative to as_of_date.
    """
    engine = get_engine()
    table = rule["table"]
    date_col = rule["date_column"]
    filter_clause = rule["filter_clause"]

    query = text(f"""
        SELECT MAX({date_col}) as latest_date, COUNT(*) as total_rows
        FROM {table}
        WHERE {filter_clause}
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchone()

    latest_date = result[0]
    total_rows = result[1]

    if latest_date is None or total_rows == 0:
        return {
            "check_name": f"stale_{rule['name']}",
            "target_table": table,
            "target_column": date_col,
            "status": "FAIL",
            "severity": rule["severity"],
            "records_checked": 0,
            "records_failed": 0,
            "failure_rate": 0.0,
            "details": {
                "message": "No data found",
                "filter": filter_clause,
            },
        }

    # Calculate staleness in calendar days
    if rule["frequency"] == "daily":
        expected_date = get_previous_trading_day(as_of_date)
        lag_days = (expected_date - latest_date).days
    else:
        # Monthly: lag is calendar days from as_of_date
        lag_days = (as_of_date - latest_date).days

    if lag_days <= 0:
        status = "PASS"
    elif lag_days <= rule["warn_lag_days"]:
        status = "PASS"
    elif lag_days <= rule["fail_lag_days"]:
        status = "WARN"
    else:
        status = "FAIL"

    return {
        "check_name": f"stale_{rule['name']}",
        "target_table": table,
        "target_column": date_col,
        "status": status,
        "severity": rule["severity"],
        "records_checked": total_rows,
        "records_failed": 1 if status == "FAIL" else 0,
        "failure_rate": 1.0 if status == "FAIL" else 0.0,
        "details": {
            "latest_date": str(latest_date),
            "as_of_date": str(as_of_date),
            "lag_days": lag_days,
            "warn_threshold_days": rule["warn_lag_days"],
            "fail_threshold_days": rule["fail_lag_days"],
            "frequency": rule["frequency"],
            "filter": filter_clause,
        },
    }


def run_all_stale_checks(
    run_date: date,
    rules: Optional[List[Dict]] = None,
) -> List[Dict]:
    """
    Run all configured staleness checks and persist results.
    """
    check_rules = rules or FRESHNESS_RULES
    results = []

    logger.info("Running stale data checks", num_checks=len(check_rules))

    for rule in check_rules:
        try:
            result = check_staleness(rule, as_of_date=run_date)
            results.append(result)
            logger.info(
                f"Stale check: {result['check_name']} → {result['status']}",
                lag=result["details"].get("lag_days"),
            )
        except Exception as e:
            logger.error(f"Stale check failed for {rule['name']}: {e}")
            results.append({
                "check_name": f"stale_{rule['name']}",
                "target_table": rule["table"],
                "target_column": rule["date_column"],
                "status": "FAIL",
                "severity": rule["severity"],
                "records_checked": 0,
                "records_failed": 0,
                "failure_rate": 0.0,
                "details": {"error": str(e)},
            })

    _save_results(run_date, results)

    passed = sum(1 for r in results if r["status"] == "PASS")
    warned = sum(1 for r in results if r["status"] == "WARN")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    logger.info(f"Stale checks complete: {passed} PASS, {warned} WARN, {failed} FAIL")

    return results


def _save_results(run_date: date, results: List[Dict]):
    """Persist stale check results to qc_results table."""
    with get_session() as session:
        for r in results:
            qc = QCResult(
                run_date=run_date,
                check_category="stale",
                check_name=r["check_name"],
                target_table=r["target_table"],
                target_column=r.get("target_column"),
                status=r["status"],
                severity=r["severity"],
                details=r.get("details"),
                records_checked=r.get("records_checked", 0),
                records_failed=r.get("records_failed", 0),
                failure_rate=r.get("failure_rate", 0.0),
            )
            session.add(qc)