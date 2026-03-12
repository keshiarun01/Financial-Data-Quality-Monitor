"""
Plotly chart helpers for the Financial Data Quality Monitor dashboard.
Provides consistent styling and reusable chart components.
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from typing import List, Optional


# ── Global theme ─────────────────────────────────────────────

COLORS = {
    "bg": "#0E1117",
    "card_bg": "#1B1F2A",
    "text": "#FAFAFA",
    "text_muted": "#8B949E",
    "green": "#2EA043",
    "yellow": "#D29922",
    "red": "#F85149",
    "blue": "#58A6FF",
    "purple": "#BC8CFF",
    "cyan": "#39D2C0",
    "grid": "#21262D",
    "border": "#30363D",
}

STATUS_COLORS = {
    "PASS": COLORS["green"],
    "WARN": COLORS["yellow"],
    "FAIL": COLORS["red"],
    "MATCH": COLORS["green"],
    "BREAK": COLORS["red"],
    "GREEN": COLORS["green"],
    "YELLOW": COLORS["yellow"],
    "RED": COLORS["red"],
}

LAYOUT_DEFAULTS = dict(
    paper_bgcolor=COLORS["bg"],
    plot_bgcolor=COLORS["bg"],
    font=dict(family="Inter, sans-serif", color=COLORS["text"], size=12),
    margin=dict(l=50, r=30, t=50, b=40),
    xaxis=dict(gridcolor=COLORS["grid"], zerolinecolor=COLORS["grid"]),
    yaxis=dict(gridcolor=COLORS["grid"], zerolinecolor=COLORS["grid"]),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
)


def apply_theme(fig: go.Figure) -> go.Figure:
    """Apply consistent dark theme to any Plotly figure."""
    fig.update_layout(**LAYOUT_DEFAULTS)
    return fig


# ── Health Trend Chart ───────────────────────────────────────

def health_trend_chart(df: pd.DataFrame) -> go.Figure:
    """
    Line chart showing daily health score over time.
    Expects columns: run_date, score, color
    """
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["run_date"],
        y=df["score"],
        mode="lines+markers",
        line=dict(color=COLORS["cyan"], width=2.5),
        marker=dict(size=6),
        name="Health Score",
        hovertemplate="%{x|%b %d}<br>Score: %{y:.1f}%<extra></extra>",
    ))

    # Add threshold bands
    fig.add_hline(y=90, line_dash="dash", line_color=COLORS["green"],
                  opacity=0.4, annotation_text="GREEN ≥90%",
                  annotation_position="top left")
    fig.add_hline(y=70, line_dash="dash", line_color=COLORS["yellow"],
                  opacity=0.4, annotation_text="YELLOW ≥70%",
                  annotation_position="bottom left")

    fig.update_layout(
        title="30-Day Data Health Trend",
        xaxis_title="Date",
        yaxis_title="Health Score (%)",
        yaxis=dict(range=[0, 105]),
        height=350,
    )
    return apply_theme(fig)


# ── QC Results Bar Chart ─────────────────────────────────────

def qc_summary_bar(df: pd.DataFrame) -> go.Figure:
    """
    Stacked bar chart of PASS/WARN/FAIL per check category.
    Expects columns: check_category, status, count
    """
    categories = df["check_category"].unique()
    pass_vals, warn_vals, fail_vals = [], [], []

    for cat in categories:
        cat_df = df[df["check_category"] == cat]
        pass_vals.append(cat_df[cat_df["status"] == "PASS"]["count"].sum())
        warn_vals.append(cat_df[cat_df["status"] == "WARN"]["count"].sum())
        fail_vals.append(cat_df[cat_df["status"] == "FAIL"]["count"].sum())

    fig = go.Figure()
    fig.add_trace(go.Bar(name="PASS", x=categories, y=pass_vals,
                         marker_color=COLORS["green"]))
    fig.add_trace(go.Bar(name="WARN", x=categories, y=warn_vals,
                         marker_color=COLORS["yellow"]))
    fig.add_trace(go.Bar(name="FAIL", x=categories, y=fail_vals,
                         marker_color=COLORS["red"]))

    fig.update_layout(
        barmode="stack",
        title="QC Results by Category",
        xaxis_title="Check Category",
        yaxis_title="Number of Checks",
        height=350,
    )
    return apply_theme(fig)


# ── Anomaly Price Chart ──────────────────────────────────────

def price_with_anomalies(
    price_df: pd.DataFrame,
    anomaly_df: pd.DataFrame,
    ticker: str,
    show_bands: bool = True,
) -> go.Figure:
    """
    Price line chart with anomaly markers and optional Bollinger-style bands.

    price_df: columns [trade_date, close_price]
    anomaly_df: columns [observation_date, observed_value, deviation, status]
    """
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.08,
    )

    # Price line
    fig.add_trace(go.Scatter(
        x=price_df["trade_date"], y=price_df["close_price"],
        mode="lines", name="Close Price",
        line=dict(color=COLORS["blue"], width=1.5),
        hovertemplate="%{x|%b %d, %Y}<br>$%{y:.2f}<extra></extra>",
    ), row=1, col=1)

    # Rolling bands
    if show_bands and len(price_df) >= 20:
        prices = price_df["close_price"].astype(float)
        rm = prices.rolling(20).mean()
        rs = prices.rolling(20).std()

        fig.add_trace(go.Scatter(
            x=price_df["trade_date"], y=rm + 2 * rs,
            mode="lines", name="Upper Band",
            line=dict(color=COLORS["text_muted"], width=0.8, dash="dot"),
            showlegend=False,
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=price_df["trade_date"], y=rm - 2 * rs,
            mode="lines", name="Lower Band",
            line=dict(color=COLORS["text_muted"], width=0.8, dash="dot"),
            fill="tonexty", fillcolor="rgba(88, 166, 255, 0.05)",
            showlegend=False,
        ), row=1, col=1)

    # Anomaly markers
    if not anomaly_df.empty:
        warn_df = anomaly_df[anomaly_df["status"] == "WARN"]
        fail_df = anomaly_df[anomaly_df["status"] == "FAIL"]

        if not warn_df.empty:
            fig.add_trace(go.Scatter(
                x=warn_df["observation_date"],
                y=warn_df["observed_value"],
                mode="markers", name="Warning",
                marker=dict(color=COLORS["yellow"], size=10, symbol="diamond"),
            ), row=1, col=1)

        if not fail_df.empty:
            fig.add_trace(go.Scatter(
                x=fail_df["observation_date"],
                y=fail_df["observed_value"],
                mode="markers", name="Anomaly",
                marker=dict(color=COLORS["red"], size=12, symbol="x"),
            ), row=1, col=1)

    # Z-score subplot
    if not anomaly_df.empty and "deviation" in anomaly_df.columns:
        fig.add_trace(go.Bar(
            x=anomaly_df["observation_date"],
            y=anomaly_df["deviation"],
            name="Z-Score",
            marker_color=[
                COLORS["red"] if abs(z) >= 3 else COLORS["yellow"]
                for z in anomaly_df["deviation"]
            ],
        ), row=2, col=1)

        fig.add_hline(y=3, line_dash="dash", line_color=COLORS["red"],
                      opacity=0.5, row=2, col=1)
        fig.add_hline(y=-3, line_dash="dash", line_color=COLORS["red"],
                      opacity=0.5, row=2, col=1)

    fig.update_layout(
        title=f"{ticker} — Price & Anomaly Detection",
        height=550,
    )
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Z-Score", row=2, col=1)

    return apply_theme(fig)


# ── Reconciliation Heatmap ───────────────────────────────────

def recon_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Heatmap showing percentage differences across tickers × dates.
    Expects columns: recon_date, ticker, pct_diff
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No reconciliation data available",
                           xref="paper", yref="paper", x=0.5, y=0.5,
                           showarrow=False, font=dict(size=16, color=COLORS["text_muted"]))
        return apply_theme(fig)

    pivot = df.pivot_table(
        index="ticker", columns="recon_date", values="pct_diff",
        aggfunc="mean"
    ).fillna(0)

    # Format dates for display
    pivot.columns = [str(c) for c in pivot.columns]

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values * 100,
        x=pivot.columns,
        y=pivot.index,
        colorscale=[
            [0, COLORS["green"]],
            [0.5, COLORS["yellow"]],
            [1.0, COLORS["red"]],
        ],
        zmin=0, zmax=2,
        text=[[f"{v*100:.3f}%" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        hovertemplate="Ticker: %{y}<br>Date: %{x}<br>Diff: %{text}<extra></extra>",
        colorbar=dict(title="% Diff"),
    ))

    fig.update_layout(
        title="Cross-Source Price Reconciliation (% Difference)",
        xaxis_title="Date",
        yaxis_title="Ticker",
        height=400,
    )
    return apply_theme(fig)


# ── Ingestion Timeline ───────────────────────────────────────

def ingestion_timeline(df: pd.DataFrame) -> go.Figure:
    """
    Timeline showing records ingested per source over time.
    Expects columns: run_date, source, records_inserted
    """
    fig = go.Figure()

    color_map = {
        "yahoo_finance": COLORS["blue"],
        "alpha_vantage": COLORS["purple"],
        "fred": COLORS["cyan"],
    }

    for source in df["source"].unique():
        sdf = df[df["source"] == source].sort_values("run_date")
        fig.add_trace(go.Scatter(
            x=sdf["run_date"],
            y=sdf["records_inserted"],
            mode="lines+markers",
            name=source,
            line=dict(color=color_map.get(source, COLORS["text_muted"]), width=2),
            marker=dict(size=5),
        ))

    fig.update_layout(
        title="Records Ingested by Source",
        xaxis_title="Date",
        yaxis_title="Records Inserted",
        height=350,
    )
    return apply_theme(fig)