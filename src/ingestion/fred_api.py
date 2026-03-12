"""
FRED (Federal Reserve Economic Data) ingester.
Fetches macroeconomic indicators: Treasury yields, CPI,
unemployment, consumer sentiment, etc.
"""

from datetime import date
from typing import Dict, List, Optional

import pandas as pd
from fredapi import Fred

from src.ingestion.base_ingester import BaseIngester
from src.utils.config import get_settings

import structlog

logger = structlog.get_logger(__name__)

# Metadata for each FRED series we track
SERIES_METADATA: Dict[str, Dict] = {
    # Daily series
    "DGS10": {
        "name": "10-Year Treasury Constant Maturity Rate",
        "unit": "Percent",
        "frequency": "daily",
    },
    "DGS2": {
        "name": "2-Year Treasury Constant Maturity Rate",
        "unit": "Percent",
        "frequency": "daily",
    },
    "DTWEXBGS": {
        "name": "Trade Weighted U.S. Dollar Index: Broad, Goods and Services",
        "unit": "Index Jan 2006=100",
        "frequency": "daily",
    },
    # Monthly series
    "CPIAUCSL": {
        "name": "Consumer Price Index for All Urban Consumers: All Items",
        "unit": "Index 1982-1984=100",
        "frequency": "monthly",
    },
    "UNRATE": {
        "name": "Unemployment Rate",
        "unit": "Percent",
        "frequency": "monthly",
    },
    "PCE": {
        "name": "Personal Consumption Expenditures",
        "unit": "Billions of Dollars",
        "frequency": "monthly",
    },
    "UMCSENT": {
        "name": "University of Michigan Consumer Sentiment",
        "unit": "Index 1966:Q1=100",
        "frequency": "monthly",
    },
}


class FREDIngester(BaseIngester):
    """
    Ingests macroeconomic indicator data from the FRED API.

    Handles both daily series (Treasury yields, USD index) and
    monthly series (CPI, unemployment, PCE, sentiment).
    """

    def __init__(
        self,
        series_ids: Optional[List[str]] = None,
        frequency_filter: Optional[str] = None,
    ):
        """
        Args:
            series_ids: Specific FRED series to fetch. If None, uses config.
            frequency_filter: 'daily' or 'monthly' to filter series.
                              If None, fetches all configured series.
        """
        super().__init__(rate_limit_seconds=0.5)
        settings = get_settings()

        if series_ids:
            self.series_ids = series_ids
        elif frequency_filter == "daily":
            self.series_ids = settings.ingestion.fred_daily_list
        elif frequency_filter == "monthly":
            self.series_ids = settings.ingestion.fred_monthly_list
        else:
            self.series_ids = (
                settings.ingestion.fred_daily_list
                + settings.ingestion.fred_monthly_list
            )

        self.api_key = settings.api.fred_api_key
        if not self.api_key:
            raise ValueError(
                "FRED_API_KEY not set. Get a free key at: "
                "https://fred.stlouisfed.org/docs/api/api_key.html"
            )

        self.fred = Fred(api_key=self.api_key)

    @property
    def source_name(self) -> str:
        return "fred"

    @property
    def target_table(self) -> str:
        return "macro_indicators"

    @property
    def conflict_columns(self) -> list[str]:
        return ["series_id", "observation_date"]

    def fetch_data(
        self, start_date: date, end_date: date, **kwargs
    ) -> pd.DataFrame:
        """
        Fetch observation data for all configured FRED series.
        Each series is fetched individually with rate limiting.
        """
        logger.info(
            "Fetching FRED data",
            series=self.series_ids,
            start=str(start_date),
            end=str(end_date),
        )

        all_frames = []

        for series_id in self.series_ids:
            self._rate_limit()
            try:
                observations = self.fred.get_series(
                    series_id,
                    observation_start=start_date,
                    observation_end=end_date,
                )

                if observations.empty:
                    logger.warning(f"No data for FRED series {series_id}")
                    continue

                # Convert Series to DataFrame
                df = observations.reset_index()
                df.columns = ["observation_date", "value"]
                df["series_id"] = series_id
                all_frames.append(df)

                logger.debug(
                    f"Fetched {len(df)} observations for {series_id}",
                    series_id=series_id,
                    rows=len(df),
                )

            except Exception as e:
                logger.error(
                    f"Failed to fetch FRED series {series_id}",
                    series_id=series_id,
                    error=str(e),
                )
                continue

        if not all_frames:
            return pd.DataFrame()

        combined = pd.concat(all_frames, ignore_index=True)
        logger.info(
            "FRED fetch complete",
            total_rows=len(combined),
            series_fetched=len(all_frames),
        )
        return combined

    def transform_data(self, raw_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform FRED data to match the macro_indicators table schema.

        Adds metadata (indicator_name, unit, frequency) from the
        SERIES_METADATA lookup table.
        """
        df = raw_df.copy()

        # Ensure date column is date type
        df["observation_date"] = pd.to_datetime(df["observation_date"]).dt.date

        # Drop NaN values (FRED uses '.' for missing, which fredapi converts to NaN)
        before = len(df)
        df = df.dropna(subset=["value"])
        dropped = before - len(df)
        if dropped > 0:
            logger.info(f"Dropped {dropped} NaN observations from FRED data")

        # Round values to 6 decimal places
        df["value"] = df["value"].round(6)

        # Add metadata from lookup
        df["indicator_name"] = df["series_id"].map(
            lambda s: SERIES_METADATA.get(s, {}).get("name", s)
        )
        df["unit"] = df["series_id"].map(
            lambda s: SERIES_METADATA.get(s, {}).get("unit", "")
        )
        df["frequency"] = df["series_id"].map(
            lambda s: SERIES_METADATA.get(s, {}).get("frequency", "unknown")
        )

        # Add source
        df["source"] = self.source_name

        # Select output columns
        output_cols = [
            "series_id", "indicator_name", "observation_date",
            "value", "unit", "frequency", "source",
        ]
        df = df[output_cols]

        logger.info(
            "FRED transform complete",
            output_rows=len(df),
            series=df["series_id"].unique().tolist(),
        )
        return df


# ── Convenience functions for Airflow DAG tasks ─────────────

def ingest_fred_daily(
    start_date: date,
    end_date: date,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> dict:
    """Ingest daily FRED series (Treasury yields, USD index)."""
    ingester = FREDIngester(frequency_filter="daily")
    return ingester.run(
        start_date=start_date,
        end_date=end_date,
        dag_id=dag_id,
        task_id=task_id,
    )


def ingest_fred_monthly(
    start_date: date,
    end_date: date,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> dict:
    """Ingest monthly FRED series (CPI, unemployment, PCE, sentiment)."""
    ingester = FREDIngester(frequency_filter="monthly")
    return ingester.run(
        start_date=start_date,
        end_date=end_date,
        dag_id=dag_id,
        task_id=task_id,
    )


def ingest_fred_all(
    start_date: date,
    end_date: date,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> dict:
    """Ingest all configured FRED series."""
    ingester = FREDIngester()
    return ingester.run(
        start_date=start_date,
        end_date=end_date,
        dag_id=dag_id,
        task_id=task_id,
    )