"""
Financial Data Quality Monitor — Streamlit Dashboard
Main entry point. Configures the app and renders the landing page.

Run locally:
    streamlit run streamlit_app/app.py

Via Docker:
    (handled by Dockerfile.streamlit entrypoint)
"""

import sys
import os

# Ensure src/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st

# ── Page config (must be first Streamlit call) ───────────────
st.set_page_config(
    page_title="Financial Data Quality Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    /* Dark theme overrides */
    .stApp {
        background-color: #0E1117;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #161B22;
        border-right: 1px solid #21262D;
    }

    /* Metric cards */
    [data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 700;
    }

    /* Table styling */
    .stDataFrame {
        border: 1px solid #21262D;
        border-radius: 8px;
    }

    /* Header styling */
    h1, h2, h3 {
        color: #FAFAFA !important;
    }

    /* Muted text */
    .muted {
        color: #8B949E;
        font-size: 13px;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Tabs */
    .stTabs [data-baseweb="tab"] {
        color: #8B949E;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        color: #58A6FF;
        border-bottom-color: #58A6FF;
    }
</style>
""", unsafe_allow_html=True)


# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 16px 0;">
        <div style="font-size: 32px;">📊</div>
        <div style="font-size: 18px; font-weight: 700; color: #FAFAFA;
                    margin-top: 4px;">FDQM</div>
        <div style="font-size: 11px; color: #8B949E; letter-spacing: 1.5px;
                    text-transform: uppercase;">Financial Data Quality Monitor</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""
    <div style="color: #8B949E; font-size: 12px; padding: 0 8px;">
        <b>Data Sources</b><br>
        • Yahoo Finance (Equities)<br>
        • Alpha Vantage (Recon)<br>
        • FRED (Macro Indicators)<br><br>
        <b>Quality Checks</b><br>
        • Null & Missing Values<br>
        • Stale Data Detection<br>
        • Z-Score Anomalies<br>
        • Rolling Window Bands<br>
        • Cross-Source Recon<br>
        • Holiday-Aware Gaps<br>
        • Great Expectations
    </div>
    """, unsafe_allow_html=True)


# ── Main content ─────────────────────────────────────────────
st.markdown("""
<div style="text-align: center; padding: 40px 0 20px 0;">
    <h1 style="font-size: 36px; font-weight: 800; margin-bottom: 8px;">
        📊 Financial Data Quality Monitor
    </h1>
    <p style="color: #8B949E; font-size: 16px; max-width: 700px; margin: 0 auto;">
        Real-time data quality monitoring across equities, macro indicators,
        and cross-source reconciliation. Navigate to specific dashboards
        using the sidebar.
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("")

# Quick status overview on landing page
col1, col2, col3, col4 = st.columns(4)

try:
    from src.database.connection import get_engine
    from sqlalchemy import text

    engine = get_engine()

    with engine.connect() as conn:
        # Latest QC run stats
        qc_stats = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'PASS') as passed,
                COUNT(*) FILTER (WHERE status = 'WARN') as warned,
                COUNT(*) FILTER (WHERE status = 'FAIL') as failed,
                MAX(run_date) as latest_run
            FROM qc_results
            WHERE run_date = (SELECT MAX(run_date) FROM qc_results)
        """)).fetchone()

        eq_count = conn.execute(text(
            "SELECT COUNT(*) FROM equities"
        )).scalar()

        macro_count = conn.execute(text(
            "SELECT COUNT(*) FROM macro_indicators"
        )).scalar()

        anomaly_count = conn.execute(text("""
            SELECT COUNT(*) FROM anomaly_log
            WHERE observation_date >= CURRENT_DATE - 30
        """)).scalar()

    with col1:
        st.metric("Total QC Checks", f"{qc_stats[0]}" if qc_stats else "0")
    with col2:
        st.metric("Equity Records", f"{eq_count:,}" if eq_count else "0")
    with col3:
        st.metric("Macro Records", f"{macro_count:,}" if macro_count else "0")
    with col4:
        st.metric("Recent Anomalies", f"{anomaly_count}" if anomaly_count else "0")

    if qc_stats and qc_stats[0] > 0:
        total = qc_stats[0]
        score = (qc_stats[1] * 1.0 + qc_stats[2] * 0.5) / total * 100

        st.markdown("")
        st.info(
            f"📅 Latest QC run: **{qc_stats[4]}** — "
            f"**{qc_stats[1]}** passed, **{qc_stats[2]}** warnings, "
            f"**{qc_stats[3]}** failures "
            f"(Health Score: **{score:.1f}%**)"
        )
    else:
        st.warning(
            "No QC results found yet. Run the quality suite first:\n\n"
            "```python\nfrom src.quality.quality_engine import run_full_quality_suite\n"
            "run_full_quality_suite()\n```"
        )

except Exception as e:
    with col1:
        st.metric("Status", "Offline")
    st.error(f"Cannot connect to database: {e}")

st.markdown("""
---
<div style="text-align: center; color: #8B949E; font-size: 12px; padding: 16px;">
    Built with Apache Airflow · PostgreSQL · Great Expectations · Streamlit
</div>
""", unsafe_allow_html=True)