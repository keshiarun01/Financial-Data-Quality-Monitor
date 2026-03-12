"""
Page 1: Data Health Scorecard
Shows RED/YELLOW/GREEN status per data category with
per-check detail table and 30-day health trend.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import text

from src.database.connection import get_engine
from streamlit_app.components.scorecard_card import (
    render_overall_score, render_health_card,
)
from streamlit_app.components.charts import (
    health_trend_chart, qc_summary_bar, COLORS,
)

st.set_page_config(page_title="Health Scorecard", page_icon="🏥", layout="wide")

st.markdown("""
<h1 style="font-size: 28px; font-weight: 700;">
    🏥 Data Health Scorecard
</h1>
<p class="muted">Real-time quality status across all data categories</p>
""", unsafe_allow_html=True)

engine = get_engine()

# ── Load latest QC results ───────────────────────────────────
try:
    with engine.connect() as conn:
        latest_date = conn.execute(text(
            "SELECT MAX(run_date) FROM qc_results"
        )).scalar()

        if not latest_date:
            st.warning("No QC results found. Run the quality suite first.")
            st.stop()

        qc_df = pd.read_sql(text("""
            SELECT check_name, check_category, status, severity,
                   records_checked, records_failed, failure_rate, details
            FROM qc_results
            WHERE run_date = :run_date
            ORDER BY
                CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
                check_category
        """), conn, params={"run_date": latest_date})

except Exception as e:
    st.error(f"Error loading QC results: {e}")
    st.stop()

st.markdown(f"<p class='muted'>Latest run: {latest_date}</p>", unsafe_allow_html=True)

# ── Compute scores per category ──────────────────────────────
STATUS_SCORES = {"PASS": 1.0, "WARN": 0.5, "FAIL": 0.0}

categories = {}
for _, row in qc_df.iterrows():
    cat = row["check_category"]
    if cat not in categories:
        categories[cat] = {"pass": 0, "warn": 0, "fail": 0, "total": 0}
    categories[cat]["total"] += 1
    categories[cat][row["status"].lower()] += 1

for cat in categories:
    c = categories[cat]
    c["score"] = (c["pass"] * 1.0 + c["warn"] * 0.5) / max(c["total"], 1) * 100
    if c["score"] >= 90:
        c["color"] = "GREEN"
    elif c["score"] >= 70:
        c["color"] = "YELLOW"
    else:
        c["color"] = "RED"

# Overall score
total = len(qc_df)
overall_score = qc_df["status"].map(STATUS_SCORES).mean() * 100
overall_color = "GREEN" if overall_score >= 90 else "YELLOW" if overall_score >= 70 else "RED"

# ── Render overall banner ────────────────────────────────────
render_overall_score(overall_score, overall_color, total)

# ── Category cards ───────────────────────────────────────────
# Map category names to display names
DISPLAY_NAMES = {
    "null_check": "Null Checks",
    "stale": "Freshness",
    "anomaly": "Anomalies",
    "recon": "Reconciliation",
    "gap": "Gap Detection",
    "great_expectations": "GE Validation",
}

cat_list = list(categories.items())
cols = st.columns(min(len(cat_list), 6))

for i, (cat, info) in enumerate(cat_list):
    with cols[i % len(cols)]:
        render_health_card(
            title=DISPLAY_NAMES.get(cat, cat),
            score=info["score"],
            color=info["color"],
            pass_count=info["pass"],
            warn_count=info["warn"],
            fail_count=info["fail"],
        )

st.markdown("<br>", unsafe_allow_html=True)

# ── Summary bar chart ────────────────────────────────────────
col_chart, col_table = st.columns([1.2, 1])

with col_chart:
    summary_data = []
    for _, row in qc_df.iterrows():
        summary_data.append({
            "check_category": DISPLAY_NAMES.get(row["check_category"], row["check_category"]),
            "status": row["status"],
            "count": 1,
        })
    summary_df = pd.DataFrame(summary_data)

    if not summary_df.empty:
        fig = qc_summary_bar(summary_df)
        st.plotly_chart(fig, use_container_width=True)

with col_table:
    st.markdown("##### Check Results Detail")

    display_df = qc_df[["check_name", "check_category", "status", "severity",
                         "records_checked", "records_failed"]].copy()
    display_df["check_category"] = display_df["check_category"].map(
        lambda x: DISPLAY_NAMES.get(x, x)
    )

    # Color-code status column
    def color_status(val):
        colors = {"PASS": "#2EA043", "WARN": "#D29922", "FAIL": "#F85149"}
        return f"color: {colors.get(val, '#FAFAFA')}"

    styled = display_df.style.applymap(color_status, subset=["status"])
    st.dataframe(styled, use_container_width=True, height=400)

# ── 30-Day Health Trend ──────────────────────────────────────
st.markdown("---")
st.markdown("##### 30-Day Health Trend")

try:
    with engine.connect() as conn:
        trend_df = pd.read_sql(text("""
            SELECT
                run_date,
                AVG(CASE status
                    WHEN 'PASS' THEN 100.0
                    WHEN 'WARN' THEN 50.0
                    ELSE 0.0
                END) as score
            FROM qc_results
            WHERE run_date >= CURRENT_DATE - 30
            GROUP BY run_date
            ORDER BY run_date
        """), conn)

    if not trend_df.empty:
        trend_df["color"] = trend_df["score"].apply(
            lambda s: "GREEN" if s >= 90 else "YELLOW" if s >= 70 else "RED"
        )
        fig = health_trend_chart(trend_df)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough historical data for trend chart yet.")

except Exception as e:
    st.error(f"Error loading trend data: {e}")

# ── GE Validation Results ────────────────────────────────────
st.markdown("---")
st.markdown("##### Great Expectations Validation")

try:
    with engine.connect() as conn:
        ge_df = pd.read_sql(text("""
            SELECT suite_name, success, evaluated_expectations,
                   successful_expectations, unsuccessful_expectations,
                   run_timestamp
            FROM ge_validation_results
            WHERE run_date = :run_date
            ORDER BY suite_name
        """), conn, params={"run_date": latest_date})

    if not ge_df.empty:
        ge_cols = st.columns(len(ge_df))
        for i, (_, row) in enumerate(ge_df.iterrows()):
            with ge_cols[i]:
                status_emoji = "✅" if row["success"] else "❌"
                rate = (row["successful_expectations"] /
                        max(row["evaluated_expectations"], 1) * 100)
                st.markdown(f"""
                <div style="background: #1B1F2A; border-radius: 8px; padding: 16px;
                            border: 1px solid #30363D;">
                    <div style="font-size: 13px; color: #8B949E;">
                        {row['suite_name'].replace('_', ' ').title()}</div>
                    <div style="font-size: 24px; margin: 4px 0;">
                        {status_emoji} {rate:.0f}%</div>
                    <div style="font-size: 11px; color: #8B949E;">
                        {row['successful_expectations']}/{row['evaluated_expectations']} passed</div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("No Great Expectations results for this date.")

except Exception as e:
    st.warning(f"Could not load GE results: {e}")