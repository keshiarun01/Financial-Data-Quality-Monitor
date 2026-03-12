"""
Holiday-aware gap detection.
Identifies missing trading days in equity data by comparing
actual data dates against expected trading days (weekdays minus
NYSE holidays).
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Set

from sqlalchemy import text

from src.database.connection import get_engine, get_session
from src.models.schema import QCResult
from src.utils.config import get_settings
from src.utils.market_calendar import (
    find_missing_trading_days,
    find_consecutive_gaps,
    classify_gap_severity,
)

import structlog

logger = structlog.get_logger(__name__)


def _get_actual_dates(
    ticker: str,
    start_date: date,
    end_date: date,
) -> Set[date]:
    """Fetch actual trading dates with data for a ticker."""
    engine = get_engine()
    query = text("""
        SELECT DISTINCT trade_date
        FROM equities
        WHERE ticker = :ticker
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        ORDER BY trade_date
    """)

    with engine.connect() as conn:
        result = conn.execute(
            query,
            {"ticker": ticker, "start_date": start_date, "end_date": end_date},
        )
        dates = {row[0] for row in result}
    return dates


def detect_gaps_for_ticker(
    ticker: str,
    start_date: date,
    end_date: date,
) -> Dict:
    """
    Detect missing trading days for a single ticker.

    Compares actual data dates against expected trading days
    (weekdays minus NYSE holidays).

    Returns:
        Dict with gap analysis results
    """
    actual_dates = _get_actual_dates(ticker, start_date, end_date)

    if not actual_dates:
        return {
            "ticker": ticker,
            "status": "FAIL",
            "severity": "CRITICAL",
            "missing_days": [],
            "consecutive_gaps": [],
            "details": {
                "message": f"No data at all for {ticker} in range",
                "start_date": str(start_date),
                "end_date": str(end_date),
            },
        }

    # Find missing trading days (excluding weekends and holidays)
    missing = find_missing_trading_days(actual_dates, start_date, end_date)

    # Group into consecutive gap stretches
    gaps = find_consecutive_gaps(missing)

    # Determine overall severity
    if not missing:
        status = "PASS"
        severity = "LOW"
    elif len(missing) == 1:
        status = "WARN"
        severity = "MEDIUM"
    elif len(missing) <= 3:
        status = "WARN"
        severity = "HIGH"
    else:
        status = "FAIL"
        severity = "CRITICAL"

    return {
        "ticker": ticker,
        "status": status,
        "severity": severity,
        "total_expected": len(actual_dates) + len(missing),
        "total_actual": len(actual_dates),
        "missing_count": len(missing),
        "missing_days": [str(d) for d in missing],
        "consecutive_gaps": gaps,
        "details": {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "total_expected_trading_days": len(actual_dates) + len(missing),
            "total_actual_data_days": len(actual_dates),
            "missing_trading_days": len(missing),
            "gap_stretches": len(gaps),
            "worst_gap_length": max((g["length"] for g in gaps), default=0),
            "missing_dates_sample": [str(d) for d in missing[:10]],
        },
    }


def run_all_gap_detection(
    run_date: date,
    lookback_days: int = 60,
    tickers: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Run holiday-aware gap detection for all configured tickers.

    Args:
        run_date: Date of the QC run
        lookback_days: How many calendar days back to check
        tickers: Optional override of tickers to check
    """
    settings = get_settings()
    tickers = tickers or settings.ingestion.tickers_list

    start_date = run_date - timedelta(days=lookback_days)
    end_date = run_date

    logger.info(
        "Running gap detection",
        tickers=tickers,
        start=str(start_date),
        end=str(end_date),
    )

    qc_results = []

    for ticker in tickers:
        try:
            result = detect_gaps_for_ticker(ticker, start_date, end_date)

            qc_results.append({
                "check_name": f"gap_{ticker}",
                "target_table": "equities",
                "target_column": "trade_date",
                "status": result["status"],
                "severity": result["severity"],
                "records_checked": result.get("total_expected", 0),
                "records_failed": result.get("missing_count", 0),
                "failure_rate": (
                    result.get("missing_count", 0) /
                    max(result.get("total_expected", 1), 1)
                ),
                "details": result["details"],
            })

            logger.info(
                f"Gap detection {ticker}: {result['status']} "
                f"(missing={result.get('missing_count', 0)} days)"
            )

        except Exception as e:
            logger.error(f"Gap detection failed for {ticker}: {e}")
            qc_results.append({
                "check_name": f"gap_{ticker}",
                "target_table": "equities",
                "target_column": "trade_date",
                "status": "FAIL",
                "severity": "HIGH",
                "records_checked": 0,
                "records_failed": 0,
                "failure_rate": 0.0,
                "details": {"error": str(e)},
            })

    # Persist
    _save_results(run_date, qc_results)

    total_missing = sum(r.get("records_failed", 0) for r in qc_results)
    passed = sum(1 for r in qc_results if r["status"] == "PASS")
    logger.info(
        f"Gap detection complete: {passed}/{len(tickers)} tickers clean, "
        f"{total_missing} total missing days"
    )

    return qc_results


def _save_results(run_date: date, results: List[Dict]):
    """Persist gap detection results to qc_results."""
    with get_session() as session:
        for r in results:
            qc = QCResult(
                run_date=run_date,
                check_category="gap",
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