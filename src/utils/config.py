"""
Centralized configuration for Financial Data Quality Monitor.
All settings are loaded from environment variables with sensible defaults.
Uses pydantic-settings for validation and type coercion.
"""

from functools import lru_cache
from typing import List
from pydantic_settings import BaseSettings
from pydantic import field_validator


class DatabaseSettings(BaseSettings):
    """Application database connection settings."""
    host: str = "localhost"
    port: int = 5432
    name: str = "financial_dq"
    user: str = "app_user"
    password: str = "app_password"

    model_config = {"env_prefix": "APP_DB_"}

    @property
    def url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @property
    def async_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class APISettings(BaseSettings):
    """External API keys and settings."""
    fred_api_key: str = ""
    alpha_vantage_api_key: str = ""

    model_config = {"env_prefix": ""}


class QualityThresholds(BaseSettings):
    """Thresholds for data quality checks."""
    # Z-score anomaly detection
    zscore_warn_threshold: float = 2.0
    zscore_fail_threshold: float = 3.0

    # Rolling window anomaly detection
    rolling_window_days: int = 20
    rolling_std_multiplier: float = 2.0

    # Lookback for baseline statistics
    anomaly_lookback_days: int = 60

    # Cross-source reconciliation tolerances
    recon_price_tolerance_pct: float = 0.005   # 0.5%
    recon_volume_tolerance_pct: float = 0.05   # 5%

    # Stale data detection (calendar days)
    stale_warn_days: int = 2
    stale_fail_days: int = 3

    model_config = {"env_prefix": ""}


class IngestionSettings(BaseSettings):
    """Data ingestion configuration."""
    equity_tickers: str = "SPY,QQQ,IWM,EFA,AGG,TLT,GLD,VNQ"
    fred_daily_series: str = "DGS10,DGS2,DTWEXBGS"
    fred_monthly_series: str = "CPIAUCSL,UNRATE,PCE,UMCSENT"
    backfill_years: int = 2

    model_config = {"env_prefix": ""}

    @property
    def tickers_list(self) -> List[str]:
        return [t.strip() for t in self.equity_tickers.split(",") if t.strip()]

    @property
    def fred_daily_list(self) -> List[str]:
        return [s.strip() for s in self.fred_daily_series.split(",") if s.strip()]

    @property
    def fred_monthly_list(self) -> List[str]:
        return [s.strip() for s in self.fred_monthly_series.split(",") if s.strip()]


class AlertSettings(BaseSettings):
    """Alerting configuration."""
    enable_email_alerts: bool = False
    alert_email_to: str = ""
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""

    model_config = {"env_prefix": ""}


class Settings(BaseSettings):
    """Master settings object — aggregates all config sections."""
    db: DatabaseSettings = DatabaseSettings()
    api: APISettings = APISettings()
    quality: QualityThresholds = QualityThresholds()
    ingestion: IngestionSettings = IngestionSettings()
    alerts: AlertSettings = AlertSettings()

    # Project metadata
    project_name: str = "Financial Data Quality Monitor"
    version: str = "1.0.0"
    log_level: str = "INFO"

    model_config = {"env_prefix": ""}


@lru_cache()
def get_settings() -> Settings:
    """
    Return a cached Settings instance.
    Call this from anywhere in the codebase:
        from src.utils.config import get_settings
        settings = get_settings()
        print(settings.db.url)
    """
    return Settings()