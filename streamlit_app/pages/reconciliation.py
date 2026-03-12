"""
Page 3: Cross-Source Reconciliation Dashboard
Compares Yahoo Finance vs Alpha Vantage with heatmaps,
side-by-side source comparison, and break history.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date, timedelta
from sqlalchemy import text

from src.database.connection import get_engine
from src.utils.config import get_settings
from streamlit_app.components.charts import recon_heatmap, apply_theme, COLORS

st.set_page_config(page_title="Reconciliation", page_icon="⚖️", layout="wide")

st.markdown("""
<h1 style="font-size: 28px; font-weight: 700;">
    ⚖️ Cross-Source Reconciliation
</h1>
<p class="muted">Yahoo Finance vs Alpha Vantage — price and volume comparison</p>
""", unsafe_allow_html=True)

engine = get_engine()
settings = get_settings()

# ── Filters ──────────────────────────────────────────────────
col_f1, col_f2 = st.columns([1, 1])

with col_f1:
    lookback = st.slider("Lookback (days)", 7, 90, 30)

with col_f2:
    field_filter = st.selectbox("Field", ["close_price", "volume", "All"])

start_date = date.today() - timedelta(days=lookback)

# ── Load recon data ──────────────────────────────────────────
try:
    with engine.connect() as conn:
        filter_clause = ""
        if field_filter != "All":
            filter_clause = f"AND field_name = '{field_filter}'"

        recon_df = pd.read_sql(text(f"""
            SELECT recon_date, ticker, field_name, source_a, source_b,
                   value_a, value_b, abs_diff, pct_diff, tolerance_pct, status
            FROM recon_results
            WHERE recon_date >= :start_date
            {filter_clause}
            ORDER BY recon_date DESC, ticker
        """), conn, params={"start_date": start_date})

        # QC-level recon results
        qc_recon = pd.read_sql(text("""
            SELECT check_name, status, severity, records_checked,
                   records_failed, failure_rate, details
            FROM qc_results
            WHERE check_category = 'recon'
              AND run_date = (SELECT MAX(run_date) FROM qc_results WHERE check_category = 'recon')
            ORDER BY check_name
        """), conn)

except Exception as e:
    st.error(f"Error loading reconciliation data: {e}")
    st.stop()

# ── Summary metrics ──────────────────────────────────────────
if not recon_df.empty:
    total_records = len(recon_df)
    matches = len(recon_df[recon_df["status"] == "MATCH"])
    breaks = len(recon_df[recon_df["status"] == "BREAK"])
    match_rate = matches / total_records * 100 if total_records > 0 else 0

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Comparisons", f"{total_records:,}")
    with m2:
        st.metric("Matches", f"{matches:,}")
    with m3:
        st.metric("Breaks", f"{breaks:,}")
    with m4:
        delta_color = "normal" if match_rate >= 95 else "inverse"
        st.metric("Match Rate", f"{match_rate:.1f}%")
else:
    st.warning("No reconciliation data found. Run the quality suite first.")
    st.stop()

# ── Per-ticker QC status ────────────────────────────────────
st.markdown("---")
st.markdown("##### Per-Ticker Reconciliation Status")

if not qc_recon.empty:
    ticker_cols = st.columns(min(len(qc_recon), 8))
    for i, (_, row) in enumerate(qc_recon.iterrows()):
        with ticker_cols[i % len(ticker_cols)]:
            ticker = row["check_name"].replace("recon_", "")
            color = {"PASS": COLORS["green"], "WARN": COLORS["yellow"],
                     "FAIL": COLORS["red"]}.get(row["status"], COLORS["text_muted"])
            emoji = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(row["status"], "❓")

            st.markdown(f"""
            <div style="background: #1B1F2A; border-radius: 8px; padding: 12px;
                        text-align: center; border: 1px solid #30363D;">
                <div style="font-weight: 700; color: #FAFAFA;">{ticker}</div>
                <div style="font-size: 20px; margin: 4px 0;">{emoji}</div>
                <div style="font-size: 12px; color: {color};">{row['status']}</div>
            </div>
            """, unsafe_allow_html=True)

# ── Heatmap ──────────────────────────────────────────────────
st.markdown("---")
st.markdown("##### Price Difference Heatmap")

price_recon = recon_df[recon_df["field_name"] == "close_price"] if field_filter == "All" else recon_df
if not price_recon.empty:
    fig = recon_heatmap(price_recon)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No price reconciliation data to display.")

# ── Side-by-side comparison ──────────────────────────────────
st.markdown("---")
st.markdown("##### Source Comparison by Ticker")

selected_ticker = st.selectbox("Select Ticker", settings.ingestion.tickers_list)

try:
    with engine.connect() as conn:
        comparison_df = pd.read_sql(text("""
            SELECT
                e.trade_date,
                e.close_price as yahoo_close,
                r.close_price as av_close,
                ABS(e.close_price - r.close_price) as abs_diff,
                CASE WHEN e.close_price != 0
                     THEN ABS(e.close_price - r.close_price) / e.close_price * 100
                     ELSE 0
                END as pct_diff
            FROM equities e
            INNER JOIN equities_recon r
                ON e.ticker = r.ticker AND e.trade_date = r.trade_date
            WHERE e.ticker = :ticker
              AND e.trade_date >= :start_date
              AND e.source = 'yahoo_finance'
              AND r.source = 'alpha_vantage'
            ORDER BY e.trade_date
        """), conn, params={"ticker": selected_ticker, "start_date": start_date})

except Exception as e:
    st.error(f"Error: {e}")
    comparison_df = pd.DataFrame()

if not comparison_df.empty:
    c1, c2 = st.columns([2, 1])

    with c1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=comparison_df["trade_date"], y=comparison_df["yahoo_close"],
            mode="lines", name="Yahoo Finance",
            line=dict(color=COLORS["blue"], width=2),
        ))
        fig.add_trace(go.Scatter(
            x=comparison_df["trade_date"], y=comparison_df["av_close"],
            mode="lines", name="Alpha Vantage",
            line=dict(color=COLORS["purple"], width=2, dash="dot"),
        ))
        fig.update_layout(
            title=f"{selected_ticker} — Close Price Comparison",
            height=350, yaxis_title="Price ($)",
        )
        st.plotly_chart(apply_theme(fig), use_container_width=True)

    with c2:
        st.markdown("**Difference Statistics**")
        st.markdown(f"""
        | Metric | Value |
        |--------|-------|
        | Mean Diff | ${comparison_df['abs_diff'].mean():.4f} |
        | Max Diff | ${comparison_df['abs_diff'].max():.4f} |
        | Mean % Diff | {comparison_df['pct_diff'].mean():.4f}% |
        | Max % Diff | {comparison_df['pct_diff'].max():.4f}% |
        | Data Points | {len(comparison_df)} |
        """)
else:
    st.info(f"No matching data for {selected_ticker}.")

# ── Break history table ──────────────────────────────────────
st.markdown("---")
st.markdown("##### Break History")

breaks_df = recon_df[recon_df["status"] == "BREAK"].copy()
if not breaks_df.empty:
    breaks_df = breaks_df.sort_values("recon_date", ascending=False)
    display = breaks_df[["recon_date", "ticker", "field_name",
                          "value_a", "value_b", "pct_diff"]].copy()
    display["pct_diff"] = display["pct_diff"].apply(lambda x: f"{x*100:.4f}%")
    display.columns = ["Date", "Ticker", "Field", "Yahoo", "Alpha Vantage", "% Diff"]
    st.dataframe(display, use_container_width=True, height=300)
else:
    st.success("No reconciliation breaks found in the selected period.")