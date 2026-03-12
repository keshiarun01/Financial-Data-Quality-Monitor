"""
Page 2: Anomaly Explorer
Interactive deep-dive into detected anomalies with price charts,
z-score overlays, and rolling window band visualization.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import text

from src.database.connection import get_engine
from src.utils.config import get_settings
from streamlit_app.components.charts import price_with_anomalies, COLORS
from streamlit_app.components.scorecard_card import render_status_badge

st.set_page_config(page_title="Anomaly Explorer", page_icon="🔍", layout="wide")

st.markdown("""
<h1 style="font-size: 28px; font-weight: 700;">
    🔍 Anomaly Explorer
</h1>
<p class="muted">Deep-dive into statistical anomalies across equities and macro indicators</p>
""", unsafe_allow_html=True)

engine = get_engine()
settings = get_settings()

# ── Filters ──────────────────────────────────────────────────
col_f1, col_f2, col_f3 = st.columns([1, 1, 1])

with col_f1:
    data_type = st.selectbox("Data Type", ["Equities", "Macro Indicators"])

with col_f2:
    if data_type == "Equities":
        selected = st.selectbox("Ticker", settings.ingestion.tickers_list)
    else:
        selected = st.selectbox("Series", settings.ingestion.fred_daily_list +
                                settings.ingestion.fred_monthly_list)

with col_f3:
    lookback = st.slider("Lookback (days)", 30, 365, 90)

start_date = date.today() - timedelta(days=lookback)
end_date = date.today()

# ── Load data ────────────────────────────────────────────────
try:
    with engine.connect() as conn:
        if data_type == "Equities":
            price_df = pd.read_sql(text("""
                SELECT trade_date, close_price, volume
                FROM equities
                WHERE ticker = :ticker
                  AND trade_date >= :start
                  AND trade_date <= :end
                ORDER BY trade_date
            """), conn, params={"ticker": selected, "start": start_date, "end": end_date})

            anomaly_df = pd.read_sql(text("""
                SELECT observation_date, observed_value, expected_value,
                       deviation, threshold, detection_method,
                       field_name,
                       CASE WHEN ABS(deviation) >= :fail_thresh THEN 'FAIL' ELSE 'WARN' END as status
                FROM anomaly_log
                WHERE ticker_or_series = :ticker
                  AND observation_date >= :start
                  AND observation_date <= :end
                  AND field_name = 'close_price'
                ORDER BY observation_date
            """), conn, params={
                "ticker": selected, "start": start_date, "end": end_date,
                "fail_thresh": settings.quality.zscore_fail_threshold,
            })

            vol_anomaly_df = pd.read_sql(text("""
                SELECT observation_date, observed_value, expected_value,
                       deviation, detection_method,
                       CASE WHEN ABS(deviation) >= 4.5 THEN 'FAIL' ELSE 'WARN' END as status
                FROM anomaly_log
                WHERE ticker_or_series = :ticker
                  AND observation_date >= :start
                  AND field_name = 'volume'
                ORDER BY observation_date
            """), conn, params={"ticker": selected, "start": start_date, "end": end_date})

        else:
            price_df = pd.read_sql(text("""
                SELECT observation_date as trade_date, value as close_price
                FROM macro_indicators
                WHERE series_id = :series
                  AND observation_date >= :start
                  AND observation_date <= :end
                ORDER BY observation_date
            """), conn, params={"series": selected, "start": start_date, "end": end_date})

            anomaly_df = pd.read_sql(text("""
                SELECT observation_date, observed_value, expected_value,
                       deviation, threshold, detection_method,
                       CASE WHEN ABS(deviation) >= :fail_thresh THEN 'FAIL' ELSE 'WARN' END as status
                FROM anomaly_log
                WHERE ticker_or_series = :series
                  AND observation_date >= :start
                ORDER BY observation_date
            """), conn, params={
                "series": selected, "start": start_date, "end": end_date,
                "fail_thresh": settings.quality.zscore_fail_threshold,
            })
            vol_anomaly_df = pd.DataFrame()

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# ── Summary metrics ──────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)

with m1:
    st.metric("Data Points", f"{len(price_df):,}")
with m2:
    st.metric("Anomalies Detected", f"{len(anomaly_df)}")
with m3:
    fail_count = len(anomaly_df[anomaly_df["status"] == "FAIL"]) if not anomaly_df.empty else 0
    st.metric("Critical (FAIL)", f"{fail_count}")
with m4:
    if not price_df.empty and data_type == "Equities":
        latest = price_df.iloc[-1]["close_price"]
        st.metric("Latest Close", f"${latest:,.2f}")
    elif not price_df.empty:
        latest = price_df.iloc[-1]["close_price"]
        st.metric("Latest Value", f"{latest:,.4f}")
    else:
        st.metric("Latest", "N/A")

# ── Price chart with anomaly markers ─────────────────────────
st.markdown("---")

if not price_df.empty:
    label = selected if data_type == "Equities" else selected
    fig = price_with_anomalies(price_df, anomaly_df, label)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning(f"No data found for {selected}")

# ── Volume anomalies (equities only) ────────────────────────
if data_type == "Equities" and not vol_anomaly_df.empty:
    st.markdown("##### Volume Spike Detection")

    import plotly.graph_objects as go
    from streamlit_app.components.charts import apply_theme

    vfig = go.Figure()
    vfig.add_trace(go.Bar(
        x=price_df["trade_date"], y=price_df["volume"],
        name="Volume",
        marker_color=COLORS["blue"], opacity=0.4,
    ))

    if not vol_anomaly_df.empty:
        vfig.add_trace(go.Scatter(
            x=vol_anomaly_df["observation_date"],
            y=vol_anomaly_df["observed_value"],
            mode="markers", name="Volume Spike",
            marker=dict(color=COLORS["red"], size=10, symbol="triangle-up"),
        ))

    vfig.update_layout(title=f"{selected} — Volume & Spikes", height=300)
    st.plotly_chart(apply_theme(vfig), use_container_width=True)

# ── Anomaly detail table ────────────────────────────────────
st.markdown("---")
st.markdown("##### Anomaly Log")

if not anomaly_df.empty:
    display_cols = ["observation_date", "observed_value", "expected_value",
                    "deviation", "detection_method", "status"]
    available_cols = [c for c in display_cols if c in anomaly_df.columns]
    display = anomaly_df[available_cols].copy()

    display = display.sort_values("observation_date", ascending=False)

    # Format
    for col in ["observed_value", "expected_value", "deviation"]:
        if col in display.columns:
            display[col] = display[col].apply(
                lambda x: f"{x:,.4f}" if pd.notna(x) else "—"
            )

    def color_status(val):
        colors = {"PASS": "#2EA043", "WARN": "#D29922", "FAIL": "#F85149"}
        return f"color: {colors.get(val, '#FAFAFA')}"

    styled = display.style.applymap(color_status, subset=["status"])
    st.dataframe(styled, use_container_width=True, height=400)
else:
    st.success(f"No anomalies detected for {selected} in the selected period.")