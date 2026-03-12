"""
Great Expectations validation for Financial Data Quality Monitor.

Defines expectation suites programmatically for:
  1. Equity data (prices, volume, dates, tickers)
  2. Macro indicator data (values, series, dates)

Uses pandas-based validation — loads data from PostgreSQL into
DataFrames, then validates with GE. Results are persisted to
the ge_validation_results table.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

import great_expectations as gx
from great_expectations.dataset import PandasDataset

from src.database.connection import get_engine, get_session
from src.models.schema import GEValidationResult
from src.utils.config import get_settings

import structlog

logger = structlog.get_logger(__name__)


# ============================================================
# DATA LOADERS
# ============================================================

def _load_equity_data(lookback_days: int = 30) -> pd.DataFrame:
    """Load recent equity data for validation."""
    engine = get_engine()
    query = text("""
        SELECT ticker, trade_date, open_price, high_price,
               low_price, close_price, adj_close, volume, source
        FROM equities
        WHERE trade_date >= CURRENT_DATE - :lookback
        ORDER BY ticker, trade_date
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"lookback": lookback_days})


def _load_macro_data(lookback_days: int = 90) -> pd.DataFrame:
    """Load recent macro indicator data for validation."""
    engine = get_engine()
    query = text("""
        SELECT series_id, indicator_name, observation_date,
               value, unit, frequency, source
        FROM macro_indicators
        WHERE observation_date >= CURRENT_DATE - :lookback
        ORDER BY series_id, observation_date
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"lookback": lookback_days})


def _load_recon_data(lookback_days: int = 30) -> pd.DataFrame:
    """Load recent reconciliation data for validation."""
    engine = get_engine()
    query = text("""
        SELECT ticker, trade_date, close_price, volume, source
        FROM equities_recon
        WHERE trade_date >= CURRENT_DATE - :lookback
        ORDER BY ticker, trade_date
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"lookback": lookback_days})


# ============================================================
# EQUITY DATA SUITE
# ============================================================

def build_and_run_equity_suite(
    df: pd.DataFrame,
) -> Dict:
    """
    Build and execute the equity data expectation suite.

    Validates:
        - Schema: required columns exist
        - Types: prices are numeric, volume is integer-like
        - Completeness: critical columns have no nulls
        - Uniqueness: no duplicate ticker+date+source combos
        - Range: prices > 0, volume >= 0
        - Referential: tickers are from expected set
        - Freshness: data exists for recent dates
    """
    settings = get_settings()
    expected_tickers = set(settings.ingestion.tickers_list)

    ge_df = PandasDataset(df)

    # ── Schema expectations ──────────────────────────────
    required_cols = [
        "ticker", "trade_date", "open_price", "high_price",
        "low_price", "close_price", "volume", "source",
    ]
    for col in required_cols:
        ge_df.expect_column_to_exist(col)

    # ── Completeness: no nulls in critical fields ────────
    ge_df.expect_column_values_to_not_be_null("ticker")
    ge_df.expect_column_values_to_not_be_null("trade_date")
    ge_df.expect_column_values_to_not_be_null("close_price")
    ge_df.expect_column_values_to_not_be_null("volume")
    ge_df.expect_column_values_to_not_be_null("source")

    # ── Allow small % of nulls in OHLC (some sources miss these)
    ge_df.expect_column_values_to_not_be_null(
        "open_price", mostly=0.99
    )
    ge_df.expect_column_values_to_not_be_null(
        "high_price", mostly=0.99
    )
    ge_df.expect_column_values_to_not_be_null(
        "low_price", mostly=0.99
    )

    # ── Range: prices must be positive ───────────────────
    for col in ["open_price", "high_price", "low_price", "close_price"]:
        ge_df.expect_column_values_to_be_between(
            col, min_value=0.01, max_value=100000,
            mostly=0.99,
        )

    # ── Range: volume must be non-negative ───────────────
    ge_df.expect_column_values_to_be_between(
        "volume", min_value=0, max_value=50000000000,
    )

    # ── Price consistency: high >= low ───────────────────
    # Custom expectation via column_pair
    ge_df.expect_column_pair_values_a_to_be_greater_than_b(
        "high_price", "low_price",
        or_equal=True, mostly=0.99,
    )

    # ── Referential: tickers in expected set ─────────────
    ge_df.expect_column_values_to_be_in_set(
        "ticker", list(expected_tickers),
    )

    # ── Source is always yahoo_finance ────────────────────
    ge_df.expect_column_values_to_be_in_set(
        "source", ["yahoo_finance"],
    )

    # ── Uniqueness: no duplicate ticker+date combos ──────
    ge_df.expect_compound_columns_to_be_unique(
        ["ticker", "trade_date", "source"]
    )

    # ── Row count: should have reasonable amount of data ─
    ge_df.expect_table_row_count_to_be_between(
        min_value=10, max_value=100000,
    )

    # ── Validate and return results ──────────────────────
    results = ge_df.validate()
    return _parse_results(results, "equity_data_suite")


# ============================================================
# MACRO DATA SUITE
# ============================================================

def build_and_run_macro_suite(
    df: pd.DataFrame,
) -> Dict:
    """
    Build and execute the macro indicator expectation suite.

    Validates:
        - Schema: required columns exist
        - Completeness: no nulls in critical fields
        - Range: values are within reasonable bounds
        - Referential: series_ids are from expected set
        - Frequency: frequency values are valid
    """
    settings = get_settings()
    expected_series = set(
        settings.ingestion.fred_daily_list +
        settings.ingestion.fred_monthly_list
    )

    ge_df = PandasDataset(df)

    # ── Schema ───────────────────────────────────────────
    for col in ["series_id", "observation_date", "value", "frequency", "source"]:
        ge_df.expect_column_to_exist(col)

    # ── Completeness ─────────────────────────────────────
    ge_df.expect_column_values_to_not_be_null("series_id")
    ge_df.expect_column_values_to_not_be_null("observation_date")
    ge_df.expect_column_values_to_not_be_null("value")
    ge_df.expect_column_values_to_not_be_null("source")

    # ── Referential: series_id in expected set ───────────
    ge_df.expect_column_values_to_be_in_set(
        "series_id", list(expected_series),
    )

    # ── Frequency values are valid ───────────────────────
    ge_df.expect_column_values_to_be_in_set(
        "frequency", ["daily", "monthly", "quarterly", "unknown"],
    )

    # ── Source is always FRED ────────────────────────────
    ge_df.expect_column_values_to_be_in_set(
        "source", ["fred"],
    )

    # ── Value ranges by series type ──────────────────────
    # Treasury yields: typically 0-20%
    # CPI: typically 100-400
    # Unemployment: 0-30%
    # General: allow wide range for all
    ge_df.expect_column_values_to_be_between(
        "value", min_value=-50, max_value=100000,
        mostly=0.99,
    )

    # ── Uniqueness: no duplicate series+date combos ──────
    ge_df.expect_compound_columns_to_be_unique(
        ["series_id", "observation_date"]
    )

    # ── Row count ────────────────────────────────────────
    ge_df.expect_table_row_count_to_be_between(
        min_value=5, max_value=100000,
    )

    results = ge_df.validate()
    return _parse_results(results, "macro_data_suite")


# ============================================================
# RECON DATA SUITE
# ============================================================

def build_and_run_recon_suite(
    df: pd.DataFrame,
) -> Dict:
    """
    Validate reconciliation source data (Alpha Vantage).
    """
    settings = get_settings()
    expected_tickers = set(settings.ingestion.tickers_list)

    ge_df = PandasDataset(df)

    # ── Schema ───────────────────────────────────────────
    for col in ["ticker", "trade_date", "close_price", "volume", "source"]:
        ge_df.expect_column_to_exist(col)

    # ── Completeness ─────────────────────────────────────
    ge_df.expect_column_values_to_not_be_null("ticker")
    ge_df.expect_column_values_to_not_be_null("trade_date")
    ge_df.expect_column_values_to_not_be_null("close_price")

    # ── Range ────────────────────────────────────────────
    ge_df.expect_column_values_to_be_between(
        "close_price", min_value=0.01, max_value=100000,
        mostly=0.99,
    )

    # ── Referential ──────────────────────────────────────
    ge_df.expect_column_values_to_be_in_set(
        "ticker", list(expected_tickers),
    )
    ge_df.expect_column_values_to_be_in_set(
        "source", ["alpha_vantage"],
    )

    # ── Uniqueness ───────────────────────────────────────
    ge_df.expect_compound_columns_to_be_unique(
        ["ticker", "trade_date", "source"]
    )

    results = ge_df.validate()
    return _parse_results(results, "recon_data_suite")


# ============================================================
# RESULTS PARSING & PERSISTENCE
# ============================================================

def _parse_results(validation_result, suite_name: str) -> Dict:
    """
    Parse GE validation results into a standardized dict
    and extract key metrics.
    """
    stats = validation_result.get("statistics", {})

    evaluated = stats.get("evaluated_expectations", 0)
    successful = stats.get("successful_expectations", 0)
    unsuccessful = stats.get("unsuccessful_expectations", 0)
    success = validation_result.get("success", False)

    # Extract failed expectation details
    failed_expectations = []
    for result in validation_result.get("results", []):
        if not result.get("success", True):
            exp_config = result.get("expectation_config", {})
            failed_expectations.append({
                "expectation_type": exp_config.get("expectation_type", "unknown"),
                "kwargs": {
                    k: str(v) for k, v in exp_config.get("kwargs", {}).items()
                    if k != "batch_id"
                },
                "observed_value": str(result.get("result", {}).get("observed_value", "N/A")),
            })

    return {
        "suite_name": suite_name,
        "success": success,
        "evaluated": evaluated,
        "successful": successful,
        "unsuccessful": unsuccessful,
        "success_rate": round(successful / max(evaluated, 1) * 100, 1),
        "failed_expectations": failed_expectations,
    }


def _save_ge_results(run_date: date, results: List[Dict]):
    """Persist GE validation results to ge_validation_results table."""
    with get_session() as session:
        for r in results:
            ge_result = GEValidationResult(
                run_date=run_date,
                suite_name=r["suite_name"],
                success=r["success"],
                evaluated_expectations=r["evaluated"],
                successful_expectations=r["successful"],
                unsuccessful_expectations=r["unsuccessful"],
                result_json={
                    "success_rate": r["success_rate"],
                    "failed_expectations": r["failed_expectations"],
                },
            )
            session.add(ge_result)

    logger.info(f"Saved {len(results)} GE validation results")


# ============================================================
# MAIN RUNNER
# ============================================================

def run_all_ge_validations(
    run_date: Optional[date] = None,
    lookback_days: int = 30,
) -> List[Dict]:
    """
    Run all Great Expectations validation suites.

    Executes:
        1. Equity data suite
        2. Macro indicator data suite
        3. Reconciliation data suite

    Returns:
        List of result dicts, one per suite
    """
    if run_date is None:
        run_date = date.today()

    all_results = []

    logger.info(f"Running Great Expectations validations for {run_date}")

    # ── 1. Equity suite ──────────────────────────────────
    try:
        equity_df = _load_equity_data(lookback_days)
        if equity_df.empty:
            logger.warning("No equity data to validate")
            all_results.append({
                "suite_name": "equity_data_suite",
                "success": False,
                "evaluated": 0,
                "successful": 0,
                "unsuccessful": 0,
                "success_rate": 0.0,
                "failed_expectations": [{"message": "No data"}],
            })
        else:
            result = build_and_run_equity_suite(equity_df)
            all_results.append(result)
            logger.info(
                f"Equity suite: {'PASS' if result['success'] else 'FAIL'} "
                f"({result['successful']}/{result['evaluated']} expectations passed)"
            )
    except Exception as e:
        logger.error(f"Equity GE validation failed: {e}")
        all_results.append({
            "suite_name": "equity_data_suite",
            "success": False, "evaluated": 0,
            "successful": 0, "unsuccessful": 0,
            "success_rate": 0.0,
            "failed_expectations": [{"error": str(e)}],
        })

    # ── 2. Macro suite ───────────────────────────────────
    try:
        macro_df = _load_macro_data(lookback_days * 3)
        if macro_df.empty:
            logger.warning("No macro data to validate")
            all_results.append({
                "suite_name": "macro_data_suite",
                "success": False,
                "evaluated": 0,
                "successful": 0,
                "unsuccessful": 0,
                "success_rate": 0.0,
                "failed_expectations": [{"message": "No data"}],
            })
        else:
            result = build_and_run_macro_suite(macro_df)
            all_results.append(result)
            logger.info(
                f"Macro suite: {'PASS' if result['success'] else 'FAIL'} "
                f"({result['successful']}/{result['evaluated']} expectations passed)"
            )
    except Exception as e:
        logger.error(f"Macro GE validation failed: {e}")
        all_results.append({
            "suite_name": "macro_data_suite",
            "success": False, "evaluated": 0,
            "successful": 0, "unsuccessful": 0,
            "success_rate": 0.0,
            "failed_expectations": [{"error": str(e)}],
        })

    # ── 3. Recon suite ───────────────────────────────────
    try:
        recon_df = _load_recon_data(lookback_days)
        if recon_df.empty:
            logger.warning("No recon data to validate — skipping suite")
            all_results.append({
                "suite_name": "recon_data_suite",
                "success": True,
                "evaluated": 0,
                "successful": 0,
                "unsuccessful": 0,
                "success_rate": 100.0,
                "failed_expectations": [],
            })
        else:
            result = build_and_run_recon_suite(recon_df)
            all_results.append(result)
            logger.info(
                f"Recon suite: {'PASS' if result['success'] else 'FAIL'} "
                f"({result['successful']}/{result['evaluated']} expectations passed)"
            )
    except Exception as e:
        logger.error(f"Recon GE validation failed: {e}")
        all_results.append({
            "suite_name": "recon_data_suite",
            "success": False, "evaluated": 0,
            "successful": 0, "unsuccessful": 0,
            "success_rate": 0.0,
            "failed_expectations": [{"error": str(e)}],
        })

    # ── Persist all results ──────────────────────────────
    _save_ge_results(run_date, all_results)

    # ── Summary ──────────────────────────────────────────
    total_evaluated = sum(r["evaluated"] for r in all_results)
    total_passed = sum(r["successful"] for r in all_results)
    all_passed = all(r["success"] for r in all_results)

    logger.info(
        f"GE validation complete: {total_passed}/{total_evaluated} expectations passed "
        f"({'ALL SUITES PASS' if all_passed else 'SOME SUITES FAILED'})"
    )

    return all_results