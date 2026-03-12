"""
Reusable scorecard card components for the Streamlit dashboard.
Renders styled metric cards with RED/YELLOW/GREEN status indicators.
"""

import streamlit as st


# ── Color mappings ───────────────────────────────────────────

COLOR_MAP = {
    "GREEN": {"bg": "rgba(46, 160, 67, 0.15)", "border": "#2EA043", "emoji": "🟢", "text": "#2EA043"},
    "YELLOW": {"bg": "rgba(210, 153, 34, 0.15)", "border": "#D29922", "emoji": "🟡", "text": "#D29922"},
    "RED": {"bg": "rgba(248, 81, 73, 0.15)", "border": "#F85149", "emoji": "🔴", "text": "#F85149"},
}

STATUS_MAP = {
    "PASS": {"bg": "rgba(46, 160, 67, 0.2)", "text": "#2EA043"},
    "WARN": {"bg": "rgba(210, 153, 34, 0.2)", "text": "#D29922"},
    "FAIL": {"bg": "rgba(248, 81, 73, 0.2)", "text": "#F85149"},
}


def render_health_card(
    title: str,
    score: float,
    color: str,
    pass_count: int = 0,
    warn_count: int = 0,
    fail_count: int = 0,
):
    """
    Render a single health scorecard card.

    Args:
        title: Category name (e.g., "Equities", "Macro", "Recon")
        score: Percentage score (0-100)
        color: "GREEN", "YELLOW", or "RED"
        pass_count: Number of passed checks
        warn_count: Number of warning checks
        fail_count: Number of failed checks
    """
    colors = COLOR_MAP.get(color, COLOR_MAP["RED"])

    st.markdown(f"""
    <div style="
        background: {colors['bg']};
        border: 1px solid {colors['border']};
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        height: 180px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    ">
        <div style="font-size: 14px; color: #8B949E; text-transform: uppercase;
                    letter-spacing: 1.5px; margin-bottom: 4px;">{title}</div>
        <div style="font-size: 36px; margin: 2px 0;">{colors['emoji']}</div>
        <div style="font-size: 28px; font-weight: 700; color: {colors['text']};
                    margin: 2px 0;">{score:.1f}%</div>
        <div style="font-size: 12px; color: #8B949E; margin-top: 6px;">
            <span style="color: #2EA043;">✓{pass_count}</span> &nbsp;
            <span style="color: #D29922;">⚠{warn_count}</span> &nbsp;
            <span style="color: #F85149;">✗{fail_count}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_overall_score(score: float, color: str, total_checks: int):
    """Render the large overall health score banner."""
    colors = COLOR_MAP.get(color, COLOR_MAP["RED"])

    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {colors['bg']}, rgba(27, 31, 42, 0.8));
        border: 2px solid {colors['border']};
        border-radius: 16px;
        padding: 30px;
        text-align: center;
        margin-bottom: 24px;
    ">
        <div style="font-size: 13px; color: #8B949E; text-transform: uppercase;
                    letter-spacing: 2px;">Overall Data Health</div>
        <div style="font-size: 56px; font-weight: 800; color: {colors['text']};
                    margin: 8px 0;">{score:.1f}%</div>
        <div style="font-size: 18px; color: {colors['text']};">
            {colors['emoji']} {color}
        </div>
        <div style="font-size: 12px; color: #8B949E; margin-top: 8px;">
            {total_checks} checks executed
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_status_badge(status: str) -> str:
    """Return HTML for an inline status badge."""
    colors = STATUS_MAP.get(status, STATUS_MAP["FAIL"])
    return (
        f'<span style="background: {colors["bg"]}; color: {colors["text"]}; '
        f'padding: 2px 10px; border-radius: 12px; font-size: 12px; '
        f'font-weight: 600;">{status}</span>'
    )


def render_metric_row(label: str, value: str, status: str = ""):
    """Render a single metric row with optional status badge."""
    badge = render_status_badge(status) if status else ""
    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center;
                padding: 8px 0; border-bottom: 1px solid #21262D;">
        <span style="color: #8B949E; font-size: 13px;">{label}</span>
        <span style="color: #FAFAFA; font-size: 14px; font-weight: 500;">
            {value} {badge}
        </span>
    </div>
    """, unsafe_allow_html=True)