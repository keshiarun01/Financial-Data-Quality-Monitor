"""
Database initialization script for Financial Data Quality Monitor.
Creates all tables and seeds reference data:
  - NYSE market holidays (2020-2027)
  - Data source registry
  
Run directly:
    python scripts/init_db.py

Or via Docker:
    docker compose run --rm app-db-init
"""

import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import date
from sqlalchemy import text

from src.database.connection import (
    get_engine, get_session, create_all_tables, check_connection
)
from src.models.schema import MarketHoliday, DataSourceRegistry

import structlog

logger = structlog.get_logger(__name__)


# ============================================================
# NYSE Market Holidays 2020-2027
# Source: NYSE official holiday schedule
# ============================================================
NYSE_HOLIDAYS = [
    # 2020
    (date(2020, 1, 1), "New Year's Day"),
    (date(2020, 1, 20), "Martin Luther King Jr. Day"),
    (date(2020, 2, 17), "Presidents' Day"),
    (date(2020, 4, 10), "Good Friday"),
    (date(2020, 5, 25), "Memorial Day"),
    (date(2020, 7, 3), "Independence Day (observed)"),
    (date(2020, 9, 7), "Labor Day"),
    (date(2020, 11, 26), "Thanksgiving Day"),
    (date(2020, 12, 25), "Christmas Day"),
    # 2021
    (date(2021, 1, 1), "New Year's Day"),
    (date(2021, 1, 18), "Martin Luther King Jr. Day"),
    (date(2021, 2, 15), "Presidents' Day"),
    (date(2021, 4, 2), "Good Friday"),
    (date(2021, 5, 31), "Memorial Day"),
    (date(2021, 7, 5), "Independence Day (observed)"),
    (date(2021, 9, 6), "Labor Day"),
    (date(2021, 11, 25), "Thanksgiving Day"),
    (date(2021, 12, 24), "Christmas Day (observed)"),
    # 2022
    (date(2022, 1, 17), "Martin Luther King Jr. Day"),
    (date(2022, 2, 21), "Presidents' Day"),
    (date(2022, 4, 15), "Good Friday"),
    (date(2022, 5, 30), "Memorial Day"),
    (date(2022, 6, 20), "Juneteenth"),
    (date(2022, 7, 4), "Independence Day"),
    (date(2022, 9, 5), "Labor Day"),
    (date(2022, 11, 24), "Thanksgiving Day"),
    (date(2022, 12, 26), "Christmas Day (observed)"),
    # 2023
    (date(2023, 1, 2), "New Year's Day (observed)"),
    (date(2023, 1, 16), "Martin Luther King Jr. Day"),
    (date(2023, 2, 20), "Presidents' Day"),
    (date(2023, 4, 7), "Good Friday"),
    (date(2023, 5, 29), "Memorial Day"),
    (date(2023, 6, 19), "Juneteenth"),
    (date(2023, 7, 4), "Independence Day"),
    (date(2023, 9, 4), "Labor Day"),
    (date(2023, 11, 23), "Thanksgiving Day"),
    (date(2023, 12, 25), "Christmas Day"),
    # 2024
    (date(2024, 1, 1), "New Year's Day"),
    (date(2024, 1, 15), "Martin Luther King Jr. Day"),
    (date(2024, 2, 19), "Presidents' Day"),
    (date(2024, 3, 29), "Good Friday"),
    (date(2024, 5, 27), "Memorial Day"),
    (date(2024, 6, 19), "Juneteenth"),
    (date(2024, 7, 4), "Independence Day"),
    (date(2024, 9, 2), "Labor Day"),
    (date(2024, 11, 28), "Thanksgiving Day"),
    (date(2024, 12, 25), "Christmas Day"),
    # 2025
    (date(2025, 1, 1), "New Year's Day"),
    (date(2025, 1, 20), "Martin Luther King Jr. Day"),
    (date(2025, 2, 17), "Presidents' Day"),
    (date(2025, 4, 18), "Good Friday"),
    (date(2025, 5, 26), "Memorial Day"),
    (date(2025, 6, 19), "Juneteenth"),
    (date(2025, 7, 4), "Independence Day"),
    (date(2025, 9, 1), "Labor Day"),
    (date(2025, 11, 27), "Thanksgiving Day"),
    (date(2025, 12, 25), "Christmas Day"),
    # 2026
    (date(2026, 1, 1), "New Year's Day"),
    (date(2026, 1, 19), "Martin Luther King Jr. Day"),
    (date(2026, 2, 16), "Presidents' Day"),
    (date(2026, 4, 3), "Good Friday"),
    (date(2026, 5, 25), "Memorial Day"),
    (date(2026, 6, 19), "Juneteenth"),
    (date(2026, 7, 3), "Independence Day (observed)"),
    (date(2026, 9, 7), "Labor Day"),
    (date(2026, 11, 26), "Thanksgiving Day"),
    (date(2026, 12, 25), "Christmas Day"),
    # 2027
    (date(2027, 1, 1), "New Year's Day"),
    (date(2027, 1, 18), "Martin Luther King Jr. Day"),
    (date(2027, 2, 15), "Presidents' Day"),
    (date(2027, 3, 26), "Good Friday"),
    (date(2027, 5, 31), "Memorial Day"),
    (date(2027, 6, 18), "Juneteenth (observed)"),
    (date(2027, 7, 5), "Independence Day (observed)"),
    (date(2027, 9, 6), "Labor Day"),
    (date(2027, 11, 25), "Thanksgiving Day"),
    (date(2027, 12, 24), "Christmas Day (observed)"),
]


# ============================================================
# Data Source Registry
# ============================================================
DATA_SOURCES = [
    {
        "source_name": "yahoo_finance",
        "source_type": "api",
        "base_url": "https://query1.finance.yahoo.com",
        "rate_limit": "2000 requests/hour",
        "is_active": True,
        "notes": "Primary equity data source via yfinance library",
    },
    {
        "source_name": "alpha_vantage",
        "source_type": "api",
        "base_url": "https://www.alphavantage.co/query",
        "rate_limit": "5 requests/min (free tier), 75/min (premium)",
        "is_active": True,
        "notes": "Reconciliation source for equity close prices and volume",
    },
    {
        "source_name": "fred",
        "source_type": "api",
        "base_url": "https://api.stlouisfed.org/fred",
        "rate_limit": "120 requests/min",
        "is_active": True,
        "notes": "Federal Reserve Economic Data — macro indicators (CPI, yields, unemployment)",
    },
]


def seed_holidays(session) -> int:
    """Insert NYSE holidays if not already present. Returns count of new records."""
    inserted = 0
    for holiday_date, holiday_name in NYSE_HOLIDAYS:
        exists = session.query(MarketHoliday).filter_by(
            holiday_date=holiday_date, exchange="NYSE"
        ).first()
        if not exists:
            session.add(MarketHoliday(
                holiday_date=holiday_date,
                holiday_name=holiday_name,
                exchange="NYSE",
                country="US",
            ))
            inserted += 1
    session.flush()
    logger.info(f"Holidays seeded: {inserted} new records (total defined: {len(NYSE_HOLIDAYS)})")
    return inserted


def seed_data_sources(session) -> int:
    """Insert data source registry entries if not already present."""
    inserted = 0
    for src in DATA_SOURCES:
        exists = session.query(DataSourceRegistry).filter_by(
            source_name=src["source_name"]
        ).first()
        if not exists:
            session.add(DataSourceRegistry(**src))
            inserted += 1
    session.flush()
    logger.info(f"Data sources seeded: {inserted} new records")
    return inserted


def main():
    """Full database initialization: check connection, create tables, seed data."""
    print("=" * 60)
    print("Financial Data Quality Monitor — Database Initialization")
    print("=" * 60)

    # Step 1: Verify connectivity
    print("\n[1/4] Checking database connection...")
    if not check_connection():
        print("ERROR: Cannot connect to database. Check your .env settings.")
        sys.exit(1)
    print("  ✓ Connection verified")

    # Step 2: Create all tables
    print("\n[2/4] Creating database tables...")
    create_all_tables()
    print("  ✓ All tables created")

    # Step 3: Seed reference data
    print("\n[3/4] Seeding market holidays...")
    with get_session() as session:
        holidays_count = seed_holidays(session)
        print(f"  ✓ {holidays_count} new holidays inserted")

    print("\n[4/4] Seeding data source registry...")
    with get_session() as session:
        sources_count = seed_data_sources(session)
        print(f"  ✓ {sources_count} new data sources inserted")

    # Step 4: Verify
    print("\n" + "-" * 60)
    print("Verification — Table Row Counts:")
    engine = get_engine()
    with engine.connect() as conn:
        for table_name in [
            "equities", "equities_recon", "macro_indicators",
            "qc_results", "anomaly_log", "recon_results",
            "ge_validation_results", "market_holidays",
            "data_source_registry", "ingestion_metadata",
        ]:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                print(f"  {table_name:30s} {count:>6} rows")
            except Exception as e:
                print(f"  {table_name:30s} ERROR: {e}")

    print("\n" + "=" * 60)
    print("Database initialization complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()