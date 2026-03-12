"""
Historical data backfill script.
Seeds the database with 2 years of equity + macro data so the
dashboard and quality checks have a meaningful baseline.

Run directly:
    python scripts/seed_historical.py

Or via Docker:
    docker compose run --rm app-db-init bash -c \
        "cd /opt/airflow && python scripts/seed_historical.py"

NOTE: Alpha Vantage free tier = 25 calls/day. This script fetches
AV data with outputsize='full' but may hit rate limits. If so,
run it across multiple days or skip AV and backfill equities + FRED first.
"""

import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils.config import get_settings
from src.database.connection import check_connection, create_all_tables

import structlog

logger = structlog.get_logger(__name__)


def backfill_equities(start_date: date, end_date: date):
    """Backfill equity data from Yahoo Finance."""
    from src.ingestion.yahoo_finance import YahooFinanceIngester

    print(f"\n{'='*60}")
    print(f"Backfilling EQUITIES: {start_date} → {end_date}")
    print(f"{'='*60}")

    ingester = YahooFinanceIngester()
    result = ingester.run(start_date=start_date, end_date=end_date)

    print(f"  Fetched:  {result['records_fetched']} rows")
    print(f"  Inserted: {result['inserted']} rows")
    print(f"  Skipped:  {result['skipped']} rows (already existed)")
    print(f"  Status:   {result['status']}")
    return result


def backfill_fred(start_date: date, end_date: date):
    """Backfill all FRED macro indicators."""
    from src.ingestion.fred_api import FREDIngester

    print(f"\n{'='*60}")
    print(f"Backfilling FRED MACRO DATA: {start_date} → {end_date}")
    print(f"{'='*60}")

    ingester = FREDIngester()  # Fetches all series (daily + monthly)
    result = ingester.run(start_date=start_date, end_date=end_date)

    print(f"  Fetched:  {result['records_fetched']} rows")
    print(f"  Inserted: {result['inserted']} rows")
    print(f"  Skipped:  {result['skipped']} rows")
    print(f"  Status:   {result['status']}")
    return result


def backfill_alpha_vantage(start_date: date, end_date: date):
    """
    Backfill reconciliation data from Alpha Vantage.
    
    WARNING: Free tier = 25 requests/day total.
    With 8 tickers, this uses 8 of your 25 daily requests.
    Uses outputsize='full' for maximum history.
    """
    from src.ingestion.alpha_vantage import AlphaVantageIngester

    print(f"\n{'='*60}")
    print(f"Backfilling ALPHA VANTAGE (Recon): {start_date} → {end_date}")
    print(f"{'='*60}")
    print("  ⚠  Free tier: 25 requests/day. This uses 8 requests.")
    print("     If rate limited, remaining tickers will be skipped.")

    ingester = AlphaVantageIngester()
    result = ingester.run(
        start_date=start_date,
        end_date=end_date,
        outputsize="full",
    )

    print(f"  Fetched:  {result['records_fetched']} rows")
    print(f"  Inserted: {result['inserted']} rows")
    print(f"  Skipped:  {result['skipped']} rows")
    print(f"  Status:   {result['status']}")
    return result


def main():
    settings = get_settings()
    backfill_years = settings.ingestion.backfill_years

    end_date = date.today()
    start_date = date(end_date.year - backfill_years, end_date.month, end_date.day)

    print("=" * 60)
    print("Financial Data Quality Monitor — Historical Backfill")
    print("=" * 60)
    print(f"  Period:     {start_date} → {end_date} ({backfill_years} years)")
    print(f"  Tickers:    {settings.ingestion.tickers_list}")
    print(f"  FRED Daily: {settings.ingestion.fred_daily_list}")
    print(f"  FRED Monthly: {settings.ingestion.fred_monthly_list}")

    # Verify DB connection
    print("\nChecking database connection...")
    if not check_connection():
        print("ERROR: Cannot connect to database.")
        sys.exit(1)
    print("  ✓ Connected")

    # Ensure all tables exist (safe to call repeatedly)
    print("\nCreating tables if they don't exist...")
    create_all_tables()
    print("  ✓ Tables verified\n")

    results = {}

    # 1. Yahoo Finance (no rate limit concerns)
    try:
        results["equities"] = backfill_equities(start_date, end_date)
    except Exception as e:
        print(f"  ✗ Equity backfill failed: {e}")
        results["equities"] = {"status": "FAILED", "error": str(e)}

    # 2. FRED (generous rate limits)
    try:
        results["fred"] = backfill_fred(start_date, end_date)
    except Exception as e:
        print(f"  ✗ FRED backfill failed: {e}")
        results["fred"] = {"status": "FAILED", "error": str(e)}

    # 3. Alpha Vantage (rate limited — do last)
    try:
        results["alpha_vantage"] = backfill_alpha_vantage(start_date, end_date)
    except Exception as e:
        print(f"  ✗ Alpha Vantage backfill failed: {e}")
        results["alpha_vantage"] = {"status": "FAILED", "error": str(e)}

    # Summary
    print(f"\n{'='*60}")
    print("BACKFILL SUMMARY")
    print(f"{'='*60}")
    for source, result in results.items():
        status = result.get("status", "UNKNOWN")
        inserted = result.get("inserted", "N/A")
        emoji = "✓" if status == "SUCCESS" else "⚠" if status == "PARTIAL" else "✗"
        print(f"  {emoji} {source:20s}  status={status:10s}  inserted={inserted}")
    print(f"{'='*60}")
    print("Backfill complete!")


if __name__ == "__main__":
    main()