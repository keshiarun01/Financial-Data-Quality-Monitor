"""
Alpha Vantage data ingester.
Fetches daily close prices and volume as a RECONCILIATION source
to validate against Yahoo Finance data.

NOTE: Free tier is limited to 25 requests/day. This ingester
is designed to work within that constraint by fetching only
close + volume (not full OHLCV).
"""

from datetime import date
from typing import List, Optional

import pandas as pd
import requests

from src.ingestion.base_ingester import BaseIngester
from src.utils.config import get_settings

import structlog

logger = structlog.get_logger(__name__)

AV_BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageIngester(BaseIngester):
    """
    Ingests daily equity close prices from Alpha Vantage.
    Used as the second source for cross-source reconciliation
    against Yahoo Finance.

    Free tier: 25 requests/day — we rate-limit aggressively.
    """

    def __init__(self, tickers: Optional[List[str]] = None):
        # 15-second rate limit to stay well within free tier
        super().__init__(rate_limit_seconds=15.0)
        settings = get_settings()
        self.tickers = tickers or settings.ingestion.tickers_list
        self.api_key = settings.api.alpha_vantage_api_key

        if not self.api_key:
            raise ValueError(
                "ALPHA_VANTAGE_API_KEY not set. Get a free key at: "
                "https://www.alphavantage.co/support/#api-key"
            )

    @property
    def source_name(self) -> str:
        return "alpha_vantage"

    @property
    def target_table(self) -> str:
        return "equities_recon"

    @property
    def conflict_columns(self) -> list[str]:
        return ["ticker", "trade_date", "source"]

    def _fetch_single_ticker(self, ticker: str, outputsize: str = "compact") -> pd.DataFrame:
        """
        Fetch daily data for a single ticker from Alpha Vantage.

        Args:
            ticker: Stock symbol (e.g., 'SPY')
            outputsize: 'compact' (last 100 days) or 'full' (20+ years)

        Returns:
            DataFrame with Date, close, volume columns
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": ticker,
            "outputsize": outputsize,
            "apikey": self.api_key,
        }

        response = requests.get(AV_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Check for API errors / rate limit messages
        if "Error Message" in data:
            raise ValueError(f"Alpha Vantage error for {ticker}: {data['Error Message']}")

        if "Note" in data:
            # This is the rate limit message
            logger.warning(
                "Alpha Vantage rate limit hit",
                ticker=ticker,
                message=data["Note"],
            )
            raise ConnectionError(f"Rate limited by Alpha Vantage: {data['Note']}")

        if "Information" in data:
            logger.warning(
                "Alpha Vantage info message",
                ticker=ticker,
                message=data["Information"],
            )
            raise ConnectionError(f"Alpha Vantage: {data['Information']}")

        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            logger.warning(f"No time series data for {ticker}")
            return pd.DataFrame()

        # Parse the nested JSON into a DataFrame
        rows = []
        for date_str, values in time_series.items():
            rows.append({
                "trade_date": date_str,
                "close_price": float(values["4. close"]),
                "volume": int(values["5. volume"]),
            })

        df = pd.DataFrame(rows)
        df["ticker"] = ticker
        return df

    def fetch_data(
        self, start_date: date, end_date: date, **kwargs
    ) -> pd.DataFrame:
        """
        Fetch daily close/volume from Alpha Vantage for all tickers.
        Respects free-tier rate limits (25 calls/day).
        """
        outputsize = kwargs.get("outputsize", "compact")

        logger.info(
            "Fetching reconciliation data from Alpha Vantage",
            tickers=self.tickers,
            start=str(start_date),
            end=str(end_date),
            outputsize=outputsize,
        )

        all_frames = []

        for ticker in self.tickers:
            self._rate_limit()
            try:
                df = self._fetch_single_ticker(ticker, outputsize=outputsize)
                if not df.empty:
                    all_frames.append(df)
                    logger.debug(f"Fetched {len(df)} rows for {ticker} from AV")
            except ConnectionError:
                logger.warning(
                    f"Rate limited — stopping Alpha Vantage fetch after "
                    f"{len(all_frames)} tickers"
                )
                break
            except Exception as e:
                logger.error(
                    f"Failed to fetch {ticker} from Alpha Vantage",
                    ticker=ticker,
                    error=str(e),
                )
                continue

        if not all_frames:
            return pd.DataFrame()

        combined = pd.concat(all_frames, ignore_index=True)

        # Filter to requested date range
        combined["trade_date"] = pd.to_datetime(combined["trade_date"]).dt.date
        combined = combined[
            (combined["trade_date"] >= start_date)
            & (combined["trade_date"] <= end_date)
        ]

        logger.info(
            "Alpha Vantage fetch complete",
            total_rows=len(combined),
            tickers_fetched=len(all_frames),
        )
        return combined

    def transform_data(self, raw_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform Alpha Vantage data to match equities_recon schema.
        Minimal transformation — data is already close to target shape.
        """
        df = raw_df.copy()

        # Ensure types
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["close_price"] = df["close_price"].round(4)
        df["volume"] = df["volume"].fillna(0).astype(int)

        # Add source
        df["source"] = self.source_name

        # Select output columns
        output_cols = ["ticker", "trade_date", "close_price", "volume", "source"]
        df = df[output_cols]

        # Drop rows with null close
        df = df.dropna(subset=["close_price"])

        logger.info(
            "Alpha Vantage transform complete",
            output_rows=len(df),
        )
        return df


# ── Convenience function for Airflow ─────────────────────────

def ingest_equities_recon(
    start_date: date,
    end_date: date,
    tickers: Optional[List[str]] = None,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> dict:
    """
    Ingest equity reconciliation data from Alpha Vantage.

    Usage in DAG:
        from src.ingestion.alpha_vantage import ingest_equities_recon
        ingest_equities_recon(date(2026, 3, 11), date(2026, 3, 11))
    """
    ingester = AlphaVantageIngester(tickers=tickers)
    return ingester.run(
        start_date=start_date,
        end_date=end_date,
        dag_id=dag_id,
        task_id=task_id,
    )