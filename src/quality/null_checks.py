"""
Null and missing value quality checks.
Scans configured table/column pairs for unexpected NULLs
and reports pass/warn/fail based on configurable thresholds.
"""

from datetime import date
from typing import List, Dict, Optional

import pandas as pd
from sqlalchemy import text

from src.database.connection import get_engine
from src.models.schema import QCResult
from src.database.connection import get_session

import structlog

logger = structlog.get_logger(__name__)

# ── Check definitions ────────────────────────────────────────
# Each entry: (table, column, max_null_pct for WARN, severity)
# If null_pct == 0 required, set threshold to 0.0
NULL_CHECK_RULES = [
    # Equities — critical pricing fields
    {"table": "equities", "column": "close_price", "max_null_pct": 0.0, "severity": "CRITICAL"},
    {"table": "equities", "column": "volume", "max_null_pct": 0.0, "severity": "HIGH"},
    {"table": "equities", "column": "open_price", "max_null_pct": 0.01, "severity": "MEDIUM"},
    {"table": "equities", "column": "high_price", "max_null_pct": 0.01, "severity": "MEDIUM"},
    {"table": "equities", "column": "low_price", "max_null_pct": 0.01, "severity": "MEDIUM"},
    {"table": "equities", "column": "adj_close", "max_null_pct": 0.01, "severity": "MEDIUM"},
    # Equities recon
    {"table": "equities_recon", "column": "close_price", "max_null_pct": 0.0, "severity": "CRITICAL"},
    {"table": "equities_recon", "column": "volume", "max_null_pct": 0.0, "severity": "HIGH"},
    # Macro indicators
    {"table": "macro_indicators", "column": "value", "max_null_pct": 0.0, "severity": "CRITICAL"},
    {"table": "macro_indicators", "column": "series_id", "max_null_pct": 0.0, "severity": "CRITICAL"},
    {"table": "macro_indicators", "column": "observation_date", "max_null_pct": 0.0, "severity": "CRITICAL"},
]


def run_null_check(
    table_name: str,
    column_name: str,
    max_null_pct: float,
    severity: str,
    lookback_days: int = 30,
) -> Dict:
    """
    Check a single table/column pair for NULL values.

    Args:
        table_name: Target table to scan
        column_name: Column to check for NULLs
        max_null_pct: Maximum allowed null percentage (0.0 = no nulls allowed)
        severity: CRITICAL, HIGH, MEDIUM, LOW
        lookback_days: Only check data from the last N days

    Returns:
        Dict with check results
    """
    engine = get_engine()

    # Determine the date column for filtering
    date_col = "trade_date" if table_name in ("equities", "equities_recon") else "observation_date"

    query = text(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(*) FILTER (WHERE {column_name} IS NULL) as null_count
        FROM {table_name}
        WHERE {date_col} >= CURRENT_DATE - :lookback
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"lookback": lookback_days}).fetchone()

    total = result[0]
    nulls = result[1]

    if total == 0:
        return {
            "check_name": f"null_check_{table_name}_{column_name}",
            "target_table": table_name,
            "target_column": column_name,
            "status": "WARN",
            "severity": severity,
            "records_checked": 0,
            "records_failed": 0,
            "failure_rate": 0.0,
            "details": {"message": "No data found in lookback window"},
        }

    null_pct = nulls / total

    if nulls == 0:
        status = "PASS"
    elif null_pct <= max_null_pct:
        status = "PASS"
    elif null_pct <= max_null_pct * 2:
        status = "WARN"
    else:
        status = "FAIL"

    return {
        "check_name": f"null_check_{table_name}_{column_name}",
        "target_table": table_name,
        "target_column": column_name,
        "status": status,
        "severity": severity,
        "records_checked": total,
        "records_failed": nulls,
        "failure_rate": round(null_pct, 6),
        "details": {
            "null_count": nulls,
            "total_rows": total,
            "null_pct": round(null_pct * 100, 4),
            "threshold_pct": round(max_null_pct * 100, 4),
        },
    }


def run_all_null_checks(
    run_date: date,
    lookback_days: int = 30,
    rules: Optional[List[Dict]] = None,
) -> List[Dict]:
    """
    Execute all configured null checks and persist results.

    Returns:
        List of result dicts for each check
    """
    check_rules = rules or NULL_CHECK_RULES
    results = []

    logger.info(
        "Running null checks",
        num_checks=len(check_rules),
        lookback_days=lookback_days,
    )

    for rule in check_rules:
        try:
            result = run_null_check(
                table_name=rule["table"],
                column_name=rule["column"],
                max_null_pct=rule["max_null_pct"],
                severity=rule["severity"],
                lookback_days=lookback_days,
            )
            results.append(result)

            logger.info(
                f"Null check: {result['check_name']} → {result['status']}",
                status=result["status"],
                nulls=result["records_failed"],
                total=result["records_checked"],
            )

        except Exception as e:
            logger.error(f"Null check failed for {rule['table']}.{rule['column']}: {e}")
            results.append({
                "check_name": f"null_check_{rule['table']}_{rule['column']}",
                "target_table": rule["table"],
                "target_column": rule["column"],
                "status": "FAIL",
                "severity": rule["severity"],
                "records_checked": 0,
                "records_failed": 0,
                "failure_rate": 0.0,
                "details": {"error": str(e)},
            })

    # Persist to qc_results
    _save_results(run_date, results)

    passed = sum(1 for r in results if r["status"] == "PASS")
    warned = sum(1 for r in results if r["status"] == "WARN")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    logger.info(f"Null checks complete: {passed} PASS, {warned} WARN, {failed} FAIL")

    return results


def _save_results(run_date: date, results: List[Dict]):
    """Persist null check results to qc_results table."""
    with get_session() as session:
        for r in results:
            qc = QCResult(
                run_date=run_date,
                check_category="null_check",
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