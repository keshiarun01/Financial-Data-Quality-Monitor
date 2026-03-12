"""
Cross-source reconciliation.
Compares equity close prices and volumes between Yahoo Finance
(primary source) and Alpha Vantage (reconciliation source).
Flags breaks exceeding configurable tolerance thresholds.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from src.database.connection import get_engine, get_session
from src.models.schema import QCResult, ReconResult
from src.utils.config import get_settings

import structlog

logger = structlog.get_logger(__name__)


def _load_recon_pairs(
    ticker: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Load matched pairs of Yahoo Finance and Alpha Vantage data
    for a given ticker and date range.

    Returns DataFrame with columns:
        trade_date, yf_close, av_close, yf_volume, av_volume
    """
    engine = get_engine()
    query = text("""
        SELECT
            e.trade_date,
            e.close_price  AS yf_close,
            e.volume       AS yf_volume,
            r.close_price  AS av_close,
            r.volume       AS av_volume
        FROM equities e
        INNER JOIN equities_recon r
            ON e.ticker = r.ticker AND e.trade_date = r.trade_date
        WHERE e.ticker = :ticker
          AND e.trade_date >= :start_date
          AND e.trade_date <= :end_date
          AND e.source = 'yahoo_finance'
          AND r.source = 'alpha_vantage'
        ORDER BY e.trade_date
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            query, conn,
            params={
                "ticker": ticker,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
    return df


def reconcile_ticker(
    ticker: str,
    start_date: date,
    end_date: date,
    price_tolerance: float = 0.005,
    volume_tolerance: float = 0.05,
) -> Dict:
    """
    Reconcile a single ticker between Yahoo Finance and Alpha Vantage.

    Args:
        ticker: Stock symbol
        start_date: Start of recon window
        end_date: End of recon window
        price_tolerance: Max allowed % difference for close prices
        volume_tolerance: Max allowed % difference for volume

    Returns:
        Dict with recon summary and list of individual break records
    """
    df = _load_recon_pairs(ticker, start_date, end_date)

    if df.empty:
        return {
            "ticker": ticker,
            "pairs_checked": 0,
            "price_breaks": 0,
            "volume_breaks": 0,
            "status": "WARN",
            "details": {"message": "No matching pairs found for reconciliation"},
            "break_records": [],
        }

    break_records = []
    price_breaks = 0
    volume_breaks = 0

    for _, row in df.iterrows():
        trade_date = row["trade_date"]

        # ── Close price reconciliation ───────────────────
        yf_close = float(row["yf_close"]) if pd.notna(row["yf_close"]) else None
        av_close = float(row["av_close"]) if pd.notna(row["av_close"]) else None

        if yf_close and av_close and yf_close != 0:
            abs_diff = abs(yf_close - av_close)
            pct_diff = abs_diff / yf_close

            price_status = "BREAK" if pct_diff > price_tolerance else "MATCH"
            if price_status == "BREAK":
                price_breaks += 1

            break_records.append({
                "recon_date": trade_date,
                "ticker": ticker,
                "field_name": "close_price",
                "source_a": "yahoo_finance",
                "source_b": "alpha_vantage",
                "value_a": yf_close,
                "value_b": av_close,
                "abs_diff": round(abs_diff, 6),
                "pct_diff": round(pct_diff, 6),
                "tolerance_pct": price_tolerance,
                "status": price_status,
            })

        # ── Volume reconciliation ────────────────────────
        yf_vol = float(row["yf_volume"]) if pd.notna(row["yf_volume"]) else None
        av_vol = float(row["av_volume"]) if pd.notna(row["av_volume"]) else None

        if yf_vol and av_vol and yf_vol != 0:
            vol_abs_diff = abs(yf_vol - av_vol)
            vol_pct_diff = vol_abs_diff / yf_vol

            vol_status = "BREAK" if vol_pct_diff > volume_tolerance else "MATCH"
            if vol_status == "BREAK":
                volume_breaks += 1

            break_records.append({
                "recon_date": trade_date,
                "ticker": ticker,
                "field_name": "volume",
                "source_a": "yahoo_finance",
                "source_b": "alpha_vantage",
                "value_a": yf_vol,
                "value_b": av_vol,
                "abs_diff": round(vol_abs_diff, 6),
                "pct_diff": round(vol_pct_diff, 6),
                "tolerance_pct": volume_tolerance,
                "status": vol_status,
            })

    pairs_checked = len(df)
    total_breaks = price_breaks + volume_breaks

    if total_breaks == 0:
        status = "PASS"
    elif total_breaks <= pairs_checked * 0.1:
        status = "WARN"
    else:
        status = "FAIL"

    return {
        "ticker": ticker,
        "pairs_checked": pairs_checked,
        "price_breaks": price_breaks,
        "volume_breaks": volume_breaks,
        "total_breaks": total_breaks,
        "status": status,
        "details": {
            "pairs_checked": pairs_checked,
            "price_breaks": price_breaks,
            "volume_breaks": volume_breaks,
            "price_tolerance": price_tolerance,
            "volume_tolerance": volume_tolerance,
        },
        "break_records": break_records,
    }


def run_all_reconciliation(
    run_date: date,
    lookback_days: int = 30,
    tickers: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Run cross-source reconciliation for all tickers.
    Compares the last `lookback_days` of data.
    """
    settings = get_settings()
    tickers = tickers or settings.ingestion.tickers_list
    price_tol = settings.quality.recon_price_tolerance_pct
    vol_tol = settings.quality.recon_volume_tolerance_pct

    start_date = run_date - timedelta(days=lookback_days)
    end_date = run_date

    logger.info(
        "Running cross-source reconciliation",
        tickers=tickers,
        start=str(start_date),
        end=str(end_date),
    )

    qc_results = []
    all_break_records = []

    for ticker in tickers:
        try:
            result = reconcile_ticker(
                ticker, start_date, end_date,
                price_tolerance=price_tol,
                volume_tolerance=vol_tol,
            )

            qc_results.append({
                "check_name": f"recon_{ticker}",
                "target_table": "equities",
                "target_column": "close_price",
                "status": result["status"],
                "severity": "HIGH",
                "records_checked": result["pairs_checked"] * 2,
                "records_failed": result.get("total_breaks", 0),
                "failure_rate": (
                    result.get("total_breaks", 0) /
                    max(result["pairs_checked"] * 2, 1)
                ),
                "details": result["details"],
            })

            all_break_records.extend(result["break_records"])

            logger.info(
                f"Recon {ticker}: {result['status']} "
                f"(price_breaks={result['price_breaks']}, "
                f"vol_breaks={result['volume_breaks']})"
            )

        except Exception as e:
            logger.error(f"Reconciliation failed for {ticker}: {e}")
            qc_results.append({
                "check_name": f"recon_{ticker}",
                "target_table": "equities",
                "status": "FAIL",
                "severity": "HIGH",
                "records_checked": 0,
                "records_failed": 0,
                "failure_rate": 0.0,
                "details": {"error": str(e)},
            })

    # Persist
    _save_qc_results(run_date, qc_results)
    _save_recon_records(all_break_records)

    total_breaks = sum(r.get("records_failed", 0) for r in qc_results)
    logger.info(f"Reconciliation complete: {total_breaks} total breaks across {len(tickers)} tickers")

    return qc_results


def _save_qc_results(run_date: date, results: List[Dict]):
    """Persist recon QC results."""
    with get_session() as session:
        for r in results:
            qc = QCResult(
                run_date=run_date,
                check_category="recon",
                check_name=r["check_name"],
                target_table=r["target_table"],
                target_column=r.get("target_column"),
                status=r["status"],
                severity=r["severity"],
                details=r.get("details"),
                records_checked=r.get("records_checked", 0),
                records_failed=r.get("records_failed", 0),
                failure_rate=r.get("failure_rate", 0.0),
            )
            session.add(qc)


def _save_recon_records(break_records: List[Dict]):
    """Persist individual reconciliation records."""
    if not break_records:
        return

    with get_session() as session:
        for rec in break_records:
            recon = ReconResult(
                recon_date=rec["recon_date"],
                ticker=rec["ticker"],
                field_name=rec["field_name"],
                source_a=rec["source_a"],
                source_b=rec["source_b"],
                value_a=rec["value_a"],
                value_b=rec["value_b"],
                abs_diff=rec["abs_diff"],
                pct_diff=rec["pct_diff"],
                tolerance_pct=rec["tolerance_pct"],
                status=rec["status"],
            )
            session.add(recon)

    logger.info(f"Saved {len(break_records)} reconciliation records")