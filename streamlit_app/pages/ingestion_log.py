"""
Page 4: Ingestion Log & Pipeline History
Shows pipeline run history, records ingested per source,
table row counts, and error logs.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date, timedelta
from sqlalchemy import text

from src.database.connection import get_engine
from streamlit_app.components.charts import ingestion_timeline, apply_theme, COLORS

st.set_page_config(page_title="Ingestion Log", page_icon="📥", layout="wide")

st.markdown("""
<h1 style="font-size: 28px; font-weight: 700;">
    📥 Ingestion Log & Pipeline History
</h1>
<p class="muted">Track data pipeline runs, record counts, and errors</p>
""", unsafe_allow_html=True)

engine = get_engine()

# ── Table row counts ─────────────────────────────────────────
st.markdown("##### Current Database Status")

try:
    with engine.connect() as conn:
        tables = ["equities", "equities_recon", "macro_indicators",
                  "qc_results", "anomaly_log", "recon_results",
                  "market_holidays", "ingestion_metadata"]
        counts = {}
        for t in tables:
            try:
                counts[t] = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            except Exception:
                counts[t] = 0

    cols = st.columns(4)
    display_names = {
        "equities": ("📈", "Equities"),
        "equities_recon": ("🔄", "Recon Data"),
        "macro_indicators": ("📊", "Macro"),
        "ingestion_metadata": ("📋", "Ingestion Logs"),
    }

    for i, (table, (emoji, name)) in enumerate(display_names.items()):
        with cols[i]:
            st.metric(f"{emoji} {name}", f"{counts.get(table, 0):,}")

    cols2 = st.columns(4)
    display_names2 = {
        "qc_results": ("✅", "QC Results"),
        "anomaly_log": ("🔍", "Anomaly Logs"),
        "recon_results": ("⚖️", "Recon Results"),
        "market_holidays": ("📅", "Holidays"),
    }
    for i, (table, (emoji, name)) in enumerate(display_names2.items()):
        with cols2[i]:
            st.metric(f"{emoji} {name}", f"{counts.get(table, 0):,}")

except Exception as e:
    st.error(f"Error loading table counts: {e}")

# ── Ingestion history ────────────────────────────────────────
st.markdown("---")
st.markdown("##### Ingestion Run History")

try:
    with engine.connect() as conn:
        ingestion_df = pd.read_sql(text("""
            SELECT run_date, source, target_table, status,
                   records_fetched, records_inserted,
                   duration_seconds, error_message,
                   started_at, completed_at
            FROM ingestion_metadata
            ORDER BY started_at DESC
            LIMIT 100
        """), conn)

except Exception as e:
    st.error(f"Error loading ingestion history: {e}")
    ingestion_df = pd.DataFrame()

if not ingestion_df.empty:
    # Summary metrics
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        total_runs = len(ingestion_df)
        st.metric("Total Runs", f"{total_runs}")
    with m2:
        success_runs = len(ingestion_df[ingestion_df["status"] == "SUCCESS"])
        st.metric("Successful", f"{success_runs}")
    with m3:
        failed_runs = len(ingestion_df[ingestion_df["status"] == "FAILED"])
        st.metric("Failed", f"{failed_runs}")
    with m4:
        avg_duration = ingestion_df["duration_seconds"].mean()
        st.metric("Avg Duration", f"{avg_duration:.1f}s" if pd.notna(avg_duration) else "N/A")

    # Timeline chart
    timeline_data = ingestion_df.groupby(["run_date", "source"]).agg(
        records_inserted=("records_inserted", "sum")
    ).reset_index()

    if not timeline_data.empty:
        fig = ingestion_timeline(timeline_data)
        st.plotly_chart(fig, use_container_width=True)

    # Status breakdown by source
    st.markdown("##### Status by Source")

    source_status = ingestion_df.groupby(["source", "status"]).size().reset_index(name="count")

    if not source_status.empty:
        fig = go.Figure()

        for status in ["SUCCESS", "PARTIAL", "FAILED"]:
            s_data = source_status[source_status["status"] == status]
            if not s_data.empty:
                color = {"SUCCESS": COLORS["green"], "PARTIAL": COLORS["yellow"],
                         "FAILED": COLORS["red"]}.get(status, COLORS["text_muted"])
                fig.add_trace(go.Bar(
                    x=s_data["source"], y=s_data["count"],
                    name=status, marker_color=color,
                ))

        fig.update_layout(barmode="stack", height=300,
                          title="Ingestion Status by Source")
        st.plotly_chart(apply_theme(fig), use_container_width=True)

    # Detail table
    st.markdown("##### Run Details")

    display = ingestion_df[[
        "started_at", "source", "target_table", "status",
        "records_fetched", "records_inserted", "duration_seconds",
    ]].copy()

    display["started_at"] = pd.to_datetime(display["started_at"]).dt.strftime("%Y-%m-%d %H:%M")
    display["duration_seconds"] = display["duration_seconds"].apply(
        lambda x: f"{x:.1f}s" if pd.notna(x) else "—"
    )

    def color_status(val):
        colors = {"SUCCESS": "#2EA043", "PARTIAL": "#D29922", "FAILED": "#F85149"}
        return f"color: {colors.get(val, '#FAFAFA')}"

    styled = display.style.applymap(color_status, subset=["status"])
    st.dataframe(styled, use_container_width=True, height=400)

    # Error log
    errors = ingestion_df[ingestion_df["status"] == "FAILED"]
    if not errors.empty:
        st.markdown("---")
        st.markdown("##### Error Log")

        for _, row in errors.iterrows():
            with st.expander(
                f"❌ {row['source']} → {row['target_table']} "
                f"({row['started_at']})"
            ):
                st.code(row["error_message"] or "No error message captured", language="text")

else:
    st.info("No ingestion history found. Run the data ingestion pipeline first.")

# ── Data source registry ─────────────────────────────────────
st.markdown("---")
st.markdown("##### Data Source Registry")

try:
    with engine.connect() as conn:
        sources_df = pd.read_sql(text("""
            SELECT source_name, source_type, base_url,
                   rate_limit, is_active, notes
            FROM data_source_registry
            ORDER BY source_name
        """), conn)

    if not sources_df.empty:
        for _, row in sources_df.iterrows():
            status = "🟢 Active" if row["is_active"] else "🔴 Inactive"
            st.markdown(f"""
            <div style="background: #1B1F2A; border-radius: 8px; padding: 14px;
                        border: 1px solid #30363D; margin-bottom: 8px;">
                <div style="display: flex; justify-content: space-between;">
                    <span style="font-weight: 700; color: #FAFAFA;">
                        {row['source_name']}</span>
                    <span style="font-size: 12px;">{status}</span>
                </div>
                <div style="font-size: 12px; color: #8B949E; margin-top: 4px;">
                    {row['source_type']} · {row['rate_limit']} · {row['notes'] or ''}
                </div>
            </div>
            """, unsafe_allow_html=True)

except Exception as e:
    st.warning(f"Could not load source registry: {e}")