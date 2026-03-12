"""
Yahoo Finance data ingester.
Fetches daily OHLCV equity data for configured tickers using yfinance.
"""

from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
import yfinance as yf

from src.ingestion.base_ingester import BaseIngester
from src.utils.config import get_settings

import structlog

logger = structlog.get_logger(__name__)


class YahooFinanceIngester(BaseIngester):
    """
    Ingests daily equity price data from Yahoo Finance.
    
    Fetches OHLCV + adjusted close for a list of tickers.
    Default tickers: SPY, QQQ, IWM, EFA, AGG, TLT, GLD, VNQ
    """

    def __init__(self, tickers: Optional[List[str]] = None):
        super().__init__(rate_limit_seconds=0.2)
        settings = get_settings()
        self.tickers = tickers or settings.ingestion.tickers_list

    @property
    def source_name(self) -> str:
        return "yahoo_finance"

    @property
    def target_table(self) -> str:
        return "equities"

    @property
    def conflict_columns(self) -> list[str]:
        return ["ticker", "trade_date", "source"]

    def fetch_data(
        self, start_date: date, end_date: date, **kwargs
    ) -> pd.DataFrame:
        """
        Fetch daily OHLCV data from Yahoo Finance for all configured tickers.
        
        yfinance end_date is exclusive, so we add 1 day to include end_date.
        """
        # yfinance expects string dates; end is exclusive so +1 day
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

        logger.info(
            "Fetching equity data from Yahoo Finance",
            tickers=self.tickers,
            start=start_str,
            end=end_str,
        )

        all_frames = []

        for ticker in self.tickers:
            self._rate_limit()
            try:
                tk = yf.Ticker(ticker)
                hist = tk.history(start=start_str, end=end_str, auto_adjust=False)

                if hist.empty:
                    logger.warning(f"No data returned for {ticker}")
                    continue

                hist = hist.reset_index()
                hist["ticker"] = ticker
                all_frames.append(hist)

                logger.debug(
                    f"Fetched {len(hist)} rows for {ticker}",
                    ticker=ticker,
                    rows=len(hist),
                )

            except Exception as e:
                logger.error(
                    f"Failed to fetch {ticker}",
                    ticker=ticker,
                    error=str(e),
                )
                continue

        if not all_frames:
            return pd.DataFrame()

        combined = pd.concat(all_frames, ignore_index=True)
        logger.info(
            "Yahoo Finance fetch complete",
            total_rows=len(combined),
            tickers_fetched=len(all_frames),
        )
        return combined

    def transform_data(self, raw_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform yfinance output to match the equities table schema.
        
        yfinance columns: Date, Open, High, Low, Close, Adj Close, Volume, ticker
        Target columns: ticker, trade_date, open_price, high_price, low_price,
                        close_price, adj_close, volume, source
        """
        df = raw_df.copy()

        # Standardize column names
        col_map = {
            "Date": "trade_date",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        df = df.rename(columns=col_map)

        # Ensure trade_date is a date (not datetime)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

        # Add source column
        df["source"] = self.source_name

        # Round prices to 4 decimal places
        price_cols = ["open_price", "high_price", "low_price", "close_price", "adj_close"]
        for col in price_cols:
            if col in df.columns:
                df[col] = df[col].round(4)

        # Ensure volume is integer (handle NaN → 0)
        df["volume"] = df["volume"].fillna(0).astype(int)

        # Select only the columns we need
        output_cols = [
            "ticker", "trade_date", "open_price", "high_price",
            "low_price", "close_price", "adj_close", "volume", "source",
        ]
        df = df[output_cols]

        # Drop any rows where close_price is NaN (bad data)
        before_drop = len(df)
        df = df.dropna(subset=["close_price"])
        dropped = before_drop - len(df)
        if dropped > 0:
            logger.warning(f"Dropped {dropped} rows with null close_price")

        logger.info(
            "Transform complete",
            output_rows=len(df),
            columns=output_cols,
        )
        return df


def ingest_equities(
    start_date: date,
    end_date: date,
    tickers: Optional[List[str]] = None,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> dict:
    """
    Convenience function for Airflow tasks.
    
    Usage in a DAG:
        from src.ingestion.yahoo_finance import ingest_equities
        ingest_equities(start_date=date(2026, 3, 11), end_date=date(2026, 3, 11))
    """
    ingester = YahooFinanceIngester(tickers=tickers)
    return ingester.run(
        start_date=start_date,
        end_date=end_date,
        dag_id=dag_id,
        task_id=task_id,
    )