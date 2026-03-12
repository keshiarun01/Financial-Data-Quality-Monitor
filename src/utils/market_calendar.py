"""
Market calendar utilities for the Financial Data Quality Monitor.
Provides functions to determine trading days, identify holidays,
and detect expected data gaps due to weekends/market closures.
"""

from datetime import date, timedelta
from typing import List, Set

import pandas as pd
from sqlalchemy import select

from src.database.connection import get_session
from src.models.schema import MarketHoliday

import structlog

logger = structlog.get_logger(__name__)


def get_holidays_from_db(
    start_date: date,
    end_date: date,
    exchange: str = "NYSE",
) -> Set[date]:
    """
    Fetch market holidays from the database for a given date range.
    Returns a set of holiday dates for fast lookup.
    """
    with get_session() as session:
        stmt = (
            select(MarketHoliday.holiday_date)
            .where(MarketHoliday.exchange == exchange)
            .where(MarketHoliday.holiday_date >= start_date)
            .where(MarketHoliday.holiday_date <= end_date)
        )
        results = session.execute(stmt).scalars().all()
    holidays = set(results)
    logger.debug(
        "Holidays loaded from DB",
        start=str(start_date),
        end=str(end_date),
        count=len(holidays),
    )
    return holidays


def is_weekend(d: date) -> bool:
    """Check if a date falls on a weekend (Saturday=5, Sunday=6)."""
    return d.weekday() >= 5


def is_trading_day(
    d: date,
    holidays: Set[date] | None = None,
    exchange: str = "NYSE",
) -> bool:
    """
    Determine if a given date is a trading day.
    A trading day is a weekday that is not a market holiday.
    """
    if is_weekend(d):
        return False
    if holidays is None:
        holidays = get_holidays_from_db(d, d, exchange)
    return d not in holidays


def get_expected_trading_days(
    start_date: date,
    end_date: date,
    exchange: str = "NYSE",
) -> List[date]:
    """
    Return a list of all expected trading days between start_date
    and end_date (inclusive), excluding weekends and holidays.
    """
    holidays = get_holidays_from_db(start_date, end_date, exchange)
    trading_days = []
    current = start_date
    while current <= end_date:
        if not is_weekend(current) and current not in holidays:
            trading_days.append(current)
        current += timedelta(days=1)

    logger.debug(
        "Expected trading days calculated",
        start=str(start_date),
        end=str(end_date),
        total_calendar_days=(end_date - start_date).days + 1,
        trading_days=len(trading_days),
    )
    return trading_days


def find_missing_trading_days(
    actual_dates: Set[date],
    start_date: date,
    end_date: date,
    exchange: str = "NYSE",
) -> List[date]:
    """
    Compare actual data dates against expected trading days.
    Returns a sorted list of missing trading days (gaps).

    Args:
        actual_dates: Set of dates where data exists
        start_date: Start of the analysis window
        end_date: End of the analysis window
        exchange: Exchange calendar to use

    Returns:
        Sorted list of dates that should have data but don't
    """
    expected = set(get_expected_trading_days(start_date, end_date, exchange))
    missing = sorted(expected - actual_dates)

    if missing:
        logger.warning(
            "Missing trading days detected",
            missing_count=len(missing),
            first_missing=str(missing[0]),
            last_missing=str(missing[-1]),
        )
    return missing


def get_previous_trading_day(
    d: date,
    exchange: str = "NYSE",
) -> date:
    """
    Get the most recent trading day on or before the given date.
    Useful for stale data detection — 'when should we have last received data?'
    """
    holidays = get_holidays_from_db(d - timedelta(days=10), d, exchange)
    current = d
    while is_weekend(current) or current in holidays:
        current -= timedelta(days=1)
    return current


def get_next_trading_day(
    d: date,
    exchange: str = "NYSE",
) -> date:
    """Get the next trading day on or after the given date."""
    holidays = get_holidays_from_db(d, d + timedelta(days=10), exchange)
    current = d
    while is_weekend(current) or current in holidays:
        current += timedelta(days=1)
    return current


def classify_gap_severity(consecutive_missing: int) -> str:
    """
    Classify the severity of a data gap based on consecutive
    missing trading days.

    Returns: 'LOW', 'MEDIUM', 'HIGH', or 'CRITICAL'
    """
    if consecutive_missing <= 0:
        return "LOW"
    elif consecutive_missing == 1:
        return "MEDIUM"
    elif consecutive_missing <= 3:
        return "HIGH"
    else:
        return "CRITICAL"


def find_consecutive_gaps(missing_dates: List[date]) -> List[dict]:
    """
    Group missing dates into consecutive gap stretches.

    Returns a list of dicts:
        [
            {
                "start": date(2026, 1, 5),
                "end": date(2026, 1, 6),
                "length": 2,
                "severity": "HIGH"
            },
            ...
        ]
    """
    if not missing_dates:
        return []

    gaps = []
    current_start = missing_dates[0]
    current_end = missing_dates[0]

    for d in missing_dates[1:]:
        # Check if this date is the next trading day after current_end
        # Simple heuristic: if within 4 calendar days, consider consecutive
        if (d - current_end).days <= 4:
            current_end = d
        else:
            length = len([
                md for md in missing_dates
                if current_start <= md <= current_end
            ])
            gaps.append({
                "start": current_start,
                "end": current_end,
                "length": length,
                "severity": classify_gap_severity(length),
            })
            current_start = d
            current_end = d

    # Don't forget the last gap
    length = len([
        md for md in missing_dates
        if current_start <= md <= current_end
    ])
    gaps.append({
        "start": current_start,
        "end": current_end,
        "length": length,
        "severity": classify_gap_severity(length),
    })

    return gaps