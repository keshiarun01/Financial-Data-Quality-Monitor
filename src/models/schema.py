"""
SQLAlchemy ORM models for the Financial Data Quality Monitor.
Maps to all tables defined in the architecture: raw data, quality control,
anomaly detection, reconciliation, and reference/metadata tables.

Compatible with SQLAlchemy 1.4+ (as required by Airflow 2.8.x constraints).
"""

from datetime import date, datetime
from sqlalchemy import (
    Column, Integer, String, Numeric, BigInteger, Date, DateTime,
    Boolean, Text, JSON, UniqueConstraint, Index, func
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# ============================================================
# RAW DATA TABLES
# ============================================================

class Equity(Base):
    """Daily equity prices from Yahoo Finance."""
    __tablename__ = "equities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    open_price = Column(Numeric(12, 4))
    high_price = Column(Numeric(12, 4))
    low_price = Column(Numeric(12, 4))
    close_price = Column(Numeric(12, 4))
    adj_close = Column(Numeric(12, 4))
    volume = Column(BigInteger)
    source = Column(String(30), default="yahoo_finance")
    ingested_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint("ticker", "trade_date", "source", name="uq_equity_ticker_date_src"),
        Index("idx_equities_ticker_date", "ticker", "trade_date"),
    )

    def __repr__(self):
        return f"<Equity({self.ticker}, {self.trade_date}, close={self.close_price})>"


class EquityRecon(Base):
    """Equity data from reconciliation source (Alpha Vantage)."""
    __tablename__ = "equities_recon"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    close_price = Column(Numeric(12, 4))
    volume = Column(BigInteger)
    source = Column(String(30), default="alpha_vantage")
    ingested_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint("ticker", "trade_date", "source", name="uq_recon_ticker_date_src"),
        Index("idx_recon_ticker_date", "ticker", "trade_date"),
    )

    def __repr__(self):
        return f"<EquityRecon({self.ticker}, {self.trade_date}, close={self.close_price})>"


class MacroIndicator(Base):
    """Macroeconomic indicators from FRED."""
    __tablename__ = "macro_indicators"

    id = Column(Integer, primary_key=True, autoincrement=True)
    series_id = Column(String(30), nullable=False, index=True)
    indicator_name = Column(String(100), nullable=False)
    observation_date = Column(Date, nullable=False, index=True)
    value = Column(Numeric(15, 6))
    unit = Column(String(50))
    frequency = Column(String(20))
    source = Column(String(30), default="fred")
    ingested_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint("series_id", "observation_date", name="uq_macro_series_date"),
        Index("idx_macro_series_date", "series_id", "observation_date"),
    )

    def __repr__(self):
        return f"<MacroIndicator({self.series_id}, {self.observation_date}, val={self.value})>"


# ============================================================
# QUALITY CONTROL & ANOMALY TABLES
# ============================================================

class QCResult(Base):
    """Master quality control results table."""
    __tablename__ = "qc_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_date = Column(Date, nullable=False, index=True)
    run_timestamp = Column(DateTime, default=func.now())
    check_category = Column(String(50), nullable=False)
    check_name = Column(String(100), nullable=False)
    target_table = Column(String(50), nullable=False)
    target_column = Column(String(50))
    status = Column(String(10), nullable=False)
    severity = Column(String(10))
    details = Column(JSON)
    records_checked = Column(Integer)
    records_failed = Column(Integer)
    failure_rate = Column(Numeric(5, 4))

    __table_args__ = (
        Index("idx_qc_run_date_cat", "run_date", "check_category"),
    )

    def __repr__(self):
        return f"<QCResult({self.check_name}, {self.status}, {self.run_date})>"


class AnomalyLog(Base):
    """Detailed anomaly detection log."""
    __tablename__ = "anomaly_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    detected_at = Column(DateTime, default=func.now())
    detection_method = Column(String(30), nullable=False)
    target_table = Column(String(50), nullable=False)
    ticker_or_series = Column(String(30), nullable=False)
    observation_date = Column(Date, nullable=False)
    field_name = Column(String(50), nullable=False)
    observed_value = Column(Numeric(15, 6))
    expected_value = Column(Numeric(15, 6))
    deviation = Column(Numeric(10, 4))
    threshold = Column(Numeric(10, 4))
    is_confirmed = Column(Boolean, default=False)
    notes = Column(Text)

    __table_args__ = (
        Index("idx_anomaly_date_ticker", "observation_date", "ticker_or_series"),
    )

    def __repr__(self):
        return (
            f"<AnomalyLog({self.ticker_or_series}, {self.observation_date}, "
            f"method={self.detection_method}, dev={self.deviation})>"
        )


class ReconResult(Base):
    """Cross-source reconciliation results."""
    __tablename__ = "recon_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    recon_date = Column(Date, nullable=False, index=True)
    ticker = Column(String(10), nullable=False)
    field_name = Column(String(50), nullable=False)
    source_a = Column(String(30), nullable=False)
    source_b = Column(String(30), nullable=False)
    value_a = Column(Numeric(15, 6))
    value_b = Column(Numeric(15, 6))
    abs_diff = Column(Numeric(15, 6))
    pct_diff = Column(Numeric(10, 6))
    tolerance_pct = Column(Numeric(5, 4), default=0.01)
    status = Column(String(10), nullable=False)
    investigated = Column(Boolean, default=False)

    __table_args__ = (
        Index("idx_recon_date_ticker", "recon_date", "ticker"),
    )

    def __repr__(self):
        return f"<ReconResult({self.ticker}, {self.recon_date}, {self.status})>"


class GEValidationResult(Base):
    """Great Expectations validation run results."""
    __tablename__ = "ge_validation_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_date = Column(Date, nullable=False)
    run_timestamp = Column(DateTime, default=func.now())
    suite_name = Column(String(100), nullable=False)
    success = Column(Boolean, nullable=False)
    evaluated_expectations = Column(Integer)
    successful_expectations = Column(Integer)
    unsuccessful_expectations = Column(Integer)
    result_json = Column(JSON)

    def __repr__(self):
        return f"<GEValidationResult({self.suite_name}, success={self.success})>"


# ============================================================
# REFERENCE & METADATA TABLES
# ============================================================

class MarketHoliday(Base):
    """Market holidays for holiday-aware gap detection."""
    __tablename__ = "market_holidays"

    id = Column(Integer, primary_key=True, autoincrement=True)
    holiday_date = Column(Date, nullable=False)
    holiday_name = Column(String(100))
    exchange = Column(String(30), default="NYSE")
    country = Column(String(5), default="US")

    __table_args__ = (
        UniqueConstraint("holiday_date", "exchange", name="uq_holiday_date_exchange"),
    )

    def __repr__(self):
        return f"<MarketHoliday({self.holiday_date}, {self.holiday_name})>"


class DataSourceRegistry(Base):
    """Registry of all data sources."""
    __tablename__ = "data_source_registry"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(50), unique=True, nullable=False)
    source_type = Column(String(30))
    base_url = Column(String(255))
    rate_limit = Column(String(50))
    is_active = Column(Boolean, default=True)
    last_successful_pull = Column(DateTime)
    notes = Column(Text)

    def __repr__(self):
        return f"<DataSourceRegistry({self.source_name}, active={self.is_active})>"


class IngestionMetadata(Base):
    """Audit log for every ingestion run."""
    __tablename__ = "ingestion_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String(100))
    task_id = Column(String(100))
    run_date = Column(Date, nullable=False)
    source = Column(String(50), nullable=False)
    target_table = Column(String(50), nullable=False)
    records_fetched = Column(Integer)
    records_inserted = Column(Integer)
    records_updated = Column(Integer)
    status = Column(String(20))
    error_message = Column(Text)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_seconds = Column(Numeric(10, 2))

    def __repr__(self):
        return (
            f"<IngestionMetadata({self.source} -> {self.target_table}, "
            f"{self.status}, {self.run_date})>"
        )