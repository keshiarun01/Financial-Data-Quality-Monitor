"""
Statistical anomaly detection for financial data.
Implements two methods:
  1. Z-Score: flags values that deviate significantly from the trailing mean
  2. Rolling Window: flags values outside a rolling mean ± Nσ band

Applied to equity prices, volumes, daily returns, and macro indicators.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import text

from src.database.connection import get_engine, get_session
from src.models.schema import QCResult, AnomalyLog
from src.utils.config import get_settings

import structlog

logger = structlog.get_logger(__name__)


# ── Detection targets ────────────────────────────────────────
EQUITY_TARGETS = [
    {"field": "close_price", "table": "equities"},
    {"field": "volume", "table": "equities"},
]

MACRO_TARGETS = [
    {"series_id": "DGS10", "table": "macro_indicators"},
    {"series_id": "DGS2", "table": "macro_indicators"},
    {"series_id": "DTWEXBGS", "table": "macro_indicators"},
    {"series_id": "CPIAUCSL", "table": "macro_indicators"},
    {"series_id": "UNRATE", "table": "macro_indicators"},
]


def _load_equity_series(
    ticker: str, field: str, lookback_days: int
) -> pd.DataFrame:
    """Load a time series for a single equity ticker."""
    engine = get_engine()
    query = text(f"""
        SELECT trade_date, {field}
        FROM equities
        WHERE ticker = :ticker
          AND trade_date >= CURRENT_DATE - :lookback
          AND {field} IS NOT NULL
        ORDER BY trade_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"ticker": ticker, "lookback": lookback_days})
    return df


def _load_macro_series(
    series_id: str, lookback_days: int
) -> pd.DataFrame:
    """Load a time series for a single macro indicator."""
    engine = get_engine()
    query = text("""
        SELECT observation_date as trade_date, value
        FROM macro_indicators
        WHERE series_id = :series_id
          AND observation_date >= CURRENT_DATE - :lookback
          AND value IS NOT NULL
        ORDER BY observation_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"series_id": series_id, "lookback": lookback_days})
    return df


# ============================================================
# Z-SCORE ANOMALY DETECTION
# ============================================================

def detect_zscore_anomalies(
    series: pd.Series,
    dates: pd.Series,
    lookback: int = 60,
    warn_threshold: float = 2.0,
    fail_threshold: float = 3.0,
) -> List[Dict]:
    """
    Detect anomalies using z-score against trailing statistics.

    For each point, computes z = (x - μ) / σ using the preceding
    `lookback` observations as the baseline.

    Returns list of anomaly dicts for points exceeding thresholds.
    """
    anomalies = []
    values = series.values.astype(float)

    for i in range(lookback, len(values)):
        window = values[max(0, i - lookback):i]
        if len(window) < 10:  # Need minimum data for meaningful stats
            continue

        mu = np.mean(window)
        sigma = np.std(window, ddof=1)

        if sigma == 0 or np.isnan(sigma):
            continue

        z = (values[i] - mu) / sigma

        if abs(z) >= warn_threshold:
            anomalies.append({
                "date": dates.iloc[i],
                "observed": float(values[i]),
                "expected": float(mu),
                "deviation": float(round(z, 4)),
                "threshold": fail_threshold if abs(z) >= fail_threshold else warn_threshold,
                "status": "FAIL" if abs(z) >= fail_threshold else "WARN",
            })

    return anomalies


# ============================================================
# ROLLING WINDOW ANOMALY DETECTION
# ============================================================

def detect_rolling_anomalies(
    series: pd.Series,
    dates: pd.Series,
    window: int = 20,
    num_std: float = 2.0,
) -> List[Dict]:
    """
    Detect anomalies using rolling mean ± N standard deviations.

    Points outside the band [rolling_mean - N*σ, rolling_mean + N*σ]
    are flagged.
    """
    anomalies = []
    values = series.astype(float)

    rolling_mean = values.rolling(window=window, min_periods=10).mean()
    rolling_std = values.rolling(window=window, min_periods=10).std()

    upper = rolling_mean + num_std * rolling_std
    lower = rolling_mean - num_std * rolling_std

    for i in range(window, len(values)):
        if pd.isna(rolling_mean.iloc[i]) or pd.isna(rolling_std.iloc[i]):
            continue
        if rolling_std.iloc[i] == 0:
            continue

        val = values.iloc[i]
        if val > upper.iloc[i] or val < lower.iloc[i]:
            deviation = (val - rolling_mean.iloc[i]) / rolling_std.iloc[i]
            anomalies.append({
                "date": dates.iloc[i],
                "observed": float(val),
                "expected": float(rolling_mean.iloc[i]),
                "deviation": float(round(deviation, 4)),
                "threshold": float(num_std),
                "upper_band": float(upper.iloc[i]),
                "lower_band": float(lower.iloc[i]),
                "status": "FAIL" if abs(deviation) > num_std * 1.5 else "WARN",
            })

    return anomalies


# ============================================================
# VOLUME SPIKE DETECTION
# ============================================================

def detect_volume_spikes(
    volumes: pd.Series,
    dates: pd.Series,
    window: int = 20,
    spike_multiplier: float = 3.0,
) -> List[Dict]:
    """
    Detect abnormal volume spikes.
    Flags days where volume > spike_multiplier × 20-day average.
    """
    anomalies = []
    vol = volumes.astype(float)
    rolling_avg = vol.rolling(window=window, min_periods=10).mean()

    for i in range(window, len(vol)):
        if pd.isna(rolling_avg.iloc[i]) or rolling_avg.iloc[i] == 0:
            continue

        ratio = vol.iloc[i] / rolling_avg.iloc[i]
        if ratio >= spike_multiplier:
            anomalies.append({
                "date": dates.iloc[i],
                "observed": float(vol.iloc[i]),
                "expected": float(rolling_avg.iloc[i]),
                "deviation": float(round(ratio, 4)),
                "threshold": float(spike_multiplier),
                "status": "FAIL" if ratio >= spike_multiplier * 1.5 else "WARN",
            })

    return anomalies


# ============================================================
# MAIN RUNNERS
# ============================================================

def run_equity_anomaly_detection(
    run_date: date,
    tickers: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Run all anomaly detection methods across all equity tickers.
    Detects anomalies in close_price (z-score + rolling) and
    volume (rolling + spike detection).
    """
    settings = get_settings()
    tickers = tickers or settings.ingestion.tickers_list
    lookback = settings.quality.anomaly_lookback_days
    z_warn = settings.quality.zscore_warn_threshold
    z_fail = settings.quality.zscore_fail_threshold
    roll_window = settings.quality.rolling_window_days
    roll_std = settings.quality.rolling_std_multiplier

    all_results = []
    all_anomaly_logs = []

    for ticker in tickers:
        # ── Close price anomalies ────────────────────────
        df = _load_equity_series(ticker, "close_price", lookback * 2)
        if len(df) < 30:
            logger.warning(f"Insufficient data for {ticker} close_price anomaly detection")
            continue

        # Z-score on close price
        z_anomalies = detect_zscore_anomalies(
            df["close_price"], df["trade_date"],
            lookback=lookback, warn_threshold=z_warn, fail_threshold=z_fail,
        )
        for a in z_anomalies:
            all_anomaly_logs.append({
                **a, "method": "z_score", "ticker": ticker,
                "field": "close_price", "table": "equities",
            })

        # Rolling window on close price
        r_anomalies = detect_rolling_anomalies(
            df["close_price"], df["trade_date"],
            window=roll_window, num_std=roll_std,
        )
        for a in r_anomalies:
            all_anomaly_logs.append({
                **a, "method": "rolling_window", "ticker": ticker,
                "field": "close_price", "table": "equities",
            })

        # ── Volume anomalies ─────────────────────────────
        vdf = _load_equity_series(ticker, "volume", lookback * 2)
        if len(vdf) >= 30:
            v_spikes = detect_volume_spikes(
                vdf["volume"], vdf["trade_date"],
                window=roll_window,
            )
            for a in v_spikes:
                all_anomaly_logs.append({
                    **a, "method": "volume_spike", "ticker": ticker,
                    "field": "volume", "table": "equities",
                })

    # Build summary QC results
    total_checks = len(tickers) * 3  # z-score, rolling, volume per ticker
    anomaly_count = len(all_anomaly_logs)
    fail_count = sum(1 for a in all_anomaly_logs if a["status"] == "FAIL")

    all_results.append({
        "check_name": "anomaly_zscore_equities",
        "target_table": "equities",
        "status": "FAIL" if fail_count > 0 else "WARN" if anomaly_count > 0 else "PASS",
        "severity": "HIGH",
        "records_checked": total_checks,
        "records_failed": anomaly_count,
        "failure_rate": anomaly_count / max(total_checks, 1),
        "details": {
            "total_anomalies": anomaly_count,
            "fail_anomalies": fail_count,
            "tickers_checked": tickers,
            "methods": ["z_score", "rolling_window", "volume_spike"],
        },
    })

    # Persist
    _save_qc_results(run_date, "anomaly", all_results)
    _save_anomaly_logs(all_anomaly_logs)

    logger.info(
        f"Equity anomaly detection complete: {anomaly_count} anomalies found "
        f"({fail_count} FAIL) across {len(tickers)} tickers"
    )

    return all_results


def run_macro_anomaly_detection(
    run_date: date,
) -> List[Dict]:
    """
    Run anomaly detection on macro indicator series.
    """
    settings = get_settings()
    lookback = settings.quality.anomaly_lookback_days * 3  # Wider window for monthly
    z_warn = settings.quality.zscore_warn_threshold
    z_fail = settings.quality.zscore_fail_threshold

    all_results = []
    all_anomaly_logs = []

    for target in MACRO_TARGETS:
        series_id = target["series_id"]
        df = _load_macro_series(series_id, lookback * 2)

        if len(df) < 15:
            logger.warning(f"Insufficient data for {series_id} anomaly detection")
            continue

        z_anomalies = detect_zscore_anomalies(
            df["value"], df["trade_date"],
            lookback=lookback, warn_threshold=z_warn, fail_threshold=z_fail,
        )
        for a in z_anomalies:
            all_anomaly_logs.append({
                **a, "method": "z_score", "ticker": series_id,
                "field": "value", "table": "macro_indicators",
            })

    anomaly_count = len(all_anomaly_logs)
    fail_count = sum(1 for a in all_anomaly_logs if a["status"] == "FAIL")

    all_results.append({
        "check_name": "anomaly_zscore_macro",
        "target_table": "macro_indicators",
        "status": "FAIL" if fail_count > 0 else "WARN" if anomaly_count > 0 else "PASS",
        "severity": "HIGH",
        "records_checked": len(MACRO_TARGETS),
        "records_failed": anomaly_count,
        "failure_rate": anomaly_count / max(len(MACRO_TARGETS), 1),
        "details": {
            "total_anomalies": anomaly_count,
            "fail_anomalies": fail_count,
            "series_checked": [t["series_id"] for t in MACRO_TARGETS],
        },
    })

    _save_qc_results(run_date, "anomaly", all_results)
    _save_anomaly_logs(all_anomaly_logs)

    logger.info(f"Macro anomaly detection complete: {anomaly_count} anomalies found")
    return all_results


# ── Persistence helpers ──────────────────────────────────────

def _save_qc_results(run_date: date, category: str, results: List[Dict]):
    """Persist QC summary results."""
    with get_session() as session:
        for r in results:
            qc = QCResult(
                run_date=run_date,
                check_category=category,
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


def _save_anomaly_logs(anomaly_logs: List[Dict]):
    """Persist detailed anomaly logs."""
    if not anomaly_logs:
        return

    with get_session() as session:
        for a in anomaly_logs:
            log = AnomalyLog(
                detection_method=a["method"],
                target_table=a["table"],
                ticker_or_series=a["ticker"],
                observation_date=a["date"],
                field_name=a["field"],
                observed_value=a["observed"],
                expected_value=a["expected"],
                deviation=a["deviation"],
                threshold=a["threshold"],
            )
            session.add(log)

    logger.info(f"Saved {len(anomaly_logs)} anomaly log entries")