"""
Abstract base class for all data ingesters.
Provides retry logic, rate limiting, standardized logging,
and ingestion metadata tracking.
"""

import time
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from src.database.connection import get_session, get_engine
from src.models.schema import IngestionMetadata

import structlog

logger = structlog.get_logger(__name__)


class BaseIngester(ABC):
    """
    Abstract base class for all data source ingesters.

    Subclasses must implement:
        - source_name: str property
        - target_table: str property
        - fetch_data(): fetches raw data from the source
        - transform_data(): cleans and shapes data for DB insertion

    Provides:
        - Retry with exponential backoff (3 attempts)
        - Rate limiting between API calls
        - Ingestion metadata logging to DB
        - Upsert (INSERT ON CONFLICT DO NOTHING/UPDATE) helper
    """

    def __init__(self, rate_limit_seconds: float = 0.5):
        """
        Args:
            rate_limit_seconds: Minimum seconds between API calls.
        """
        self.rate_limit_seconds = rate_limit_seconds
        self._last_call_time: float = 0.0

    # ── Abstract properties / methods ────────────────────────

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier for this data source (e.g., 'yahoo_finance')."""
        ...

    @property
    @abstractmethod
    def target_table(self) -> str:
        """Target database table name (e.g., 'equities')."""
        ...

    @abstractmethod
    def fetch_data(
        self, start_date: date, end_date: date, **kwargs
    ) -> pd.DataFrame:
        """
        Fetch raw data from the external source.
        Returns a pandas DataFrame with raw data.
        """
        ...

    @abstractmethod
    def transform_data(self, raw_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data into the schema expected by the target table.
        Returns a cleaned DataFrame ready for DB insertion.
        """
        ...

    # ── Rate limiting ────────────────────────────────────────

    def _rate_limit(self):
        """Enforce minimum delay between API calls."""
        elapsed = time.time() - self._last_call_time
        if elapsed < self.rate_limit_seconds:
            sleep_time = self.rate_limit_seconds - elapsed
            logger.debug(
                "Rate limiting",
                source=self.source_name,
                sleep_seconds=round(sleep_time, 2),
            )
            time.sleep(sleep_time)
        self._last_call_time = time.time()

    # ── Retry-wrapped fetch ──────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        reraise=True,
    )
    def _fetch_with_retry(
        self, start_date: date, end_date: date, **kwargs
    ) -> pd.DataFrame:
        """Fetch data with automatic retry on transient failures."""
        self._rate_limit()
        return self.fetch_data(start_date, end_date, **kwargs)

    # ── Upsert helper ────────────────────────────────────────

    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: list[str],
        update_on_conflict: bool = False,
    ) -> Dict[str, int]:
        """
        Insert DataFrame rows into the database using PostgreSQL
        INSERT ... ON CONFLICT. 

        Args:
            df: DataFrame with columns matching the target table.
            table_name: Name of the target table.
            conflict_columns: Columns that form the unique constraint.
            update_on_conflict: If True, update non-key columns on conflict.
                                If False, skip conflicting rows (DO NOTHING).

        Returns:
            Dict with 'inserted' and 'skipped' counts.
        """
        if df.empty:
            logger.warning("Empty DataFrame — nothing to upsert", table=table_name)
            return {"inserted": 0, "skipped": 0}

        records = df.to_dict(orient="records")
        engine = get_engine()

        from src.models.schema import Base
        table = Base.metadata.tables[table_name]

        inserted = 0
        skipped = 0

        with engine.begin() as conn:
            for record in records:
                stmt = pg_insert(table).values(**record)

                if update_on_conflict:
                    update_cols = {
                        c.name: stmt.excluded[c.name]
                        for c in table.columns
                        if c.name not in conflict_columns
                        and c.name != "id"
                        and c.name != "ingested_at"
                    }
                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_columns,
                        set_=update_cols,
                    )
                else:
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=conflict_columns
                    )

                result = conn.execute(stmt)
                if result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1

        logger.info(
            "Upsert complete",
            table=table_name,
            inserted=inserted,
            skipped=skipped,
            total=len(records),
        )
        return {"inserted": inserted, "skipped": skipped}

    # ── Metadata logging ─────────────────────────────────────

    def log_ingestion(
        self,
        run_date: date,
        records_fetched: int,
        records_inserted: int,
        status: str,
        started_at: datetime,
        error_message: Optional[str] = None,
        dag_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> None:
        """Write an ingestion audit record to ingestion_metadata."""
        completed_at = datetime.now()
        duration = (completed_at - started_at).total_seconds()

        with get_session() as session:
            metadata = IngestionMetadata(
                dag_id=dag_id,
                task_id=task_id,
                run_date=run_date,
                source=self.source_name,
                target_table=self.target_table,
                records_fetched=records_fetched,
                records_inserted=records_inserted,
                records_updated=0,
                status=status,
                error_message=error_message,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=round(duration, 2),
            )
            session.add(metadata)

        logger.info(
            "Ingestion metadata logged",
            source=self.source_name,
            status=status,
            records=records_fetched,
            duration_s=round(duration, 2),
        )

    # ── Main orchestrator ────────────────────────────────────

    def run(
        self,
        start_date: date,
        end_date: date,
        dag_id: Optional[str] = None,
        task_id: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Full ingestion pipeline: fetch → transform → load → log.

        Returns:
            Dict with run stats: records_fetched, inserted, skipped, status
        """
        started_at = datetime.now()
        logger.info(
            "Ingestion started",
            source=self.source_name,
            start_date=str(start_date),
            end_date=str(end_date),
        )

        try:
            # Fetch
            raw_df = self._fetch_with_retry(start_date, end_date, **kwargs)
            records_fetched = len(raw_df)

            if raw_df.empty:
                logger.warning("No data returned from source", source=self.source_name)
                self.log_ingestion(
                    run_date=end_date,
                    records_fetched=0,
                    records_inserted=0,
                    status="PARTIAL",
                    started_at=started_at,
                    error_message="No data returned from source",
                    dag_id=dag_id,
                    task_id=task_id,
                )
                return {
                    "records_fetched": 0,
                    "inserted": 0,
                    "skipped": 0,
                    "status": "PARTIAL",
                }

            # Transform
            clean_df = self.transform_data(raw_df, **kwargs)

            # Load
            result = self.upsert_dataframe(
                df=clean_df,
                table_name=self.target_table,
                conflict_columns=self.conflict_columns,
            )

            # Log success
            self.log_ingestion(
                run_date=end_date,
                records_fetched=records_fetched,
                records_inserted=result["inserted"],
                status="SUCCESS",
                started_at=started_at,
                dag_id=dag_id,
                task_id=task_id,
            )

            return {
                "records_fetched": records_fetched,
                **result,
                "status": "SUCCESS",
            }

        except Exception as e:
            logger.exception(
                "Ingestion failed",
                source=self.source_name,
                error=str(e),
            )
            self.log_ingestion(
                run_date=end_date,
                records_fetched=0,
                records_inserted=0,
                status="FAILED",
                started_at=started_at,
                error_message=str(e),
                dag_id=dag_id,
                task_id=task_id,
            )
            raise

    @property
    def conflict_columns(self) -> list[str]:
        """
        Columns forming the unique constraint for upsert.
        Override in subclass if different from default.
        """
        return []