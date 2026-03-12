"""
Alerting module for the Financial Data Quality Monitor.
Sends notifications when critical data quality failures are detected.

Supports:
  - Console/log alerts (always on)
  - Email alerts via SMTP (configurable)
  - Structured alert payloads for future Slack/PagerDuty integration
"""

import smtplib
from datetime import date, datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional

from src.utils.config import get_settings

import structlog

logger = structlog.get_logger(__name__)


# ── Alert severity levels ────────────────────────────────────

ALERT_LEVELS = {
    "CRITICAL": {"emoji": "🔴", "priority": 1, "notify": True},
    "HIGH":     {"emoji": "🟠", "priority": 2, "notify": True},
    "MEDIUM":   {"emoji": "🟡", "priority": 3, "notify": False},
    "LOW":      {"emoji": "🟢", "priority": 4, "notify": False},
}


def build_alert_payload(
    run_date: date,
    health_score: Dict,
    failed_checks: List[Dict],
    total_checks: int,
) -> Dict:
    """
    Build a structured alert payload from quality check results.

    Args:
        run_date: Date of the QC run
        health_score: Dict with score, color, categories
        failed_checks: List of checks with status FAIL
        total_checks: Total number of checks executed

    Returns:
        Structured alert dict ready for any notification channel
    """
    critical_failures = [
        c for c in failed_checks
        if c.get("severity") in ("CRITICAL", "HIGH")
    ]

    score = health_score.get("score", 0)
    color = health_score.get("color", "RED")

    # Determine alert level
    if score < 50 or len(critical_failures) >= 3:
        level = "CRITICAL"
    elif score < 70 or len(critical_failures) >= 1:
        level = "HIGH"
    elif score < 90:
        level = "MEDIUM"
    else:
        level = "LOW"

    payload = {
        "timestamp": datetime.now().isoformat(),
        "run_date": str(run_date),
        "level": level,
        "health_score": score,
        "health_color": color,
        "total_checks": total_checks,
        "total_failures": len(failed_checks),
        "critical_failures": len(critical_failures),
        "failed_checks": [
            {
                "name": c.get("check_name", "unknown"),
                "category": c.get("check_category", "unknown"),
                "severity": c.get("severity", "UNKNOWN"),
                "details": _summarize_details(c.get("details")),
            }
            for c in failed_checks[:10]  # Cap at 10 to avoid giant payloads
        ],
        "category_summary": health_score.get("categories", {}),
    }

    return payload


def _summarize_details(details) -> str:
    """Extract a one-line summary from a check's details dict."""
    if not details:
        return ""
    if isinstance(details, dict):
        if "message" in details:
            return details["message"]
        if "error" in details:
            return f"Error: {details['error'][:100]}"
        if "lag_days" in details:
            return f"Data is {details['lag_days']} days stale"
        if "null_count" in details:
            return f"{details['null_count']} null values found"
        if "total_anomalies" in details:
            return f"{details['total_anomalies']} anomalies detected"
        if "missing_trading_days" in details:
            return f"{details['missing_trading_days']} missing trading days"
    return str(details)[:100]


# ── Console / Log Alert ──────────────────────────────────────

def send_log_alert(payload: Dict) -> None:
    """
    Always-on alert: logs the alert payload to structured logs.
    This is the fallback when email is not configured.
    """
    level = payload["level"]
    meta = ALERT_LEVELS.get(level, ALERT_LEVELS["HIGH"])

    border = "=" * 60
    logger.warning(
        f"\n{border}\n"
        f"{meta['emoji']} DATA QUALITY ALERT — {level}\n"
        f"{border}\n"
        f"Date:             {payload['run_date']}\n"
        f"Health Score:     {payload['health_score']:.1f}% ({payload['health_color']})\n"
        f"Total Checks:     {payload['total_checks']}\n"
        f"Failed Checks:    {payload['total_failures']}\n"
        f"Critical Failures:{payload['critical_failures']}\n"
        f"{border}",
        alert_level=level,
        health_score=payload["health_score"],
    )

    if payload["failed_checks"]:
        logger.warning("Failed check details:")
        for check in payload["failed_checks"]:
            logger.warning(
                f"  {meta['emoji']} [{check['severity']}] "
                f"{check['name']} ({check['category']})"
                f"{': ' + check['details'] if check['details'] else ''}"
            )


# ── Email Alert ──────────────────────────────────────────────

def _build_email_html(payload: Dict) -> str:
    """Build an HTML email body from the alert payload."""
    level = payload["level"]
    color_map = {"CRITICAL": "#F85149", "HIGH": "#D29922", "MEDIUM": "#D29922", "LOW": "#2EA043"}
    score_color = {"GREEN": "#2EA043", "YELLOW": "#D29922", "RED": "#F85149"}

    # Build failed checks rows
    check_rows = ""
    for c in payload["failed_checks"]:
        sev_color = color_map.get(c["severity"], "#8B949E")
        check_rows += f"""
        <tr>
            <td style="padding: 8px; border-bottom: 1px solid #30363D;">{c['name']}</td>
            <td style="padding: 8px; border-bottom: 1px solid #30363D;">{c['category']}</td>
            <td style="padding: 8px; border-bottom: 1px solid #30363D;
                       color: {sev_color}; font-weight: 600;">{c['severity']}</td>
            <td style="padding: 8px; border-bottom: 1px solid #30363D;
                       color: #8B949E; font-size: 12px;">{c['details']}</td>
        </tr>
        """

    # Build category summary
    cat_rows = ""
    for cat, info in payload.get("category_summary", {}).items():
        cat_color = score_color.get(info.get("color", "RED"), "#8B949E")
        cat_rows += f"""
        <tr>
            <td style="padding: 6px 8px; border-bottom: 1px solid #30363D;">{cat}</td>
            <td style="padding: 6px 8px; border-bottom: 1px solid #30363D;
                       color: {cat_color}; font-weight: 600;">{info.get('score', 'N/A')}%</td>
            <td style="padding: 6px 8px; border-bottom: 1px solid #30363D;">
                ✓{info['pass']} ⚠{info['warn']} ✗{info['fail']}
            </td>
        </tr>
        """

    html = f"""
    <html>
    <body style="background: #0E1117; color: #FAFAFA; font-family: 'Inter', Arial, sans-serif;
                 padding: 24px; max-width: 700px; margin: 0 auto;">
        <div style="background: {color_map.get(level, '#D29922')}20;
                    border: 2px solid {color_map.get(level, '#D29922')};
                    border-radius: 12px; padding: 24px; text-align: center;
                    margin-bottom: 24px;">
            <div style="font-size: 12px; color: #8B949E; text-transform: uppercase;
                        letter-spacing: 2px;">Financial Data Quality Alert</div>
            <div style="font-size: 42px; font-weight: 800;
                        color: {color_map.get(level, '#D29922')};
                        margin: 8px 0;">{level}</div>
            <div style="font-size: 14px; color: #8B949E;">
                {payload['run_date']} · Health Score:
                <span style="color: {score_color.get(payload['health_color'], '#8B949E')};
                             font-weight: 700;">
                    {payload['health_score']:.1f}% ({payload['health_color']})
                </span>
            </div>
        </div>

        <div style="background: #161B22; border-radius: 8px; padding: 16px;
                    margin-bottom: 16px;">
            <div style="display: flex; justify-content: space-around; text-align: center;">
                <div>
                    <div style="font-size: 24px; font-weight: 700;">{payload['total_checks']}</div>
                    <div style="font-size: 11px; color: #8B949E;">Total Checks</div>
                </div>
                <div>
                    <div style="font-size: 24px; font-weight: 700;
                                color: #F85149;">{payload['total_failures']}</div>
                    <div style="font-size: 11px; color: #8B949E;">Failures</div>
                </div>
                <div>
                    <div style="font-size: 24px; font-weight: 700;
                                color: #F85149;">{payload['critical_failures']}</div>
                    <div style="font-size: 11px; color: #8B949E;">Critical</div>
                </div>
            </div>
        </div>

        {"<h3 style='color: #FAFAFA; font-size: 16px;'>Category Summary</h3>"
         if cat_rows else ""}
        {"<table style='width: 100%; border-collapse: collapse; font-size: 13px;'>"
         "<tr style='color: #8B949E; text-align: left;'>"
         "<th style='padding: 6px 8px;'>Category</th>"
         "<th style='padding: 6px 8px;'>Score</th>"
         "<th style='padding: 6px 8px;'>Results</th>"
         "</tr>" + cat_rows + "</table>" if cat_rows else ""}

        <h3 style="color: #FAFAFA; font-size: 16px; margin-top: 24px;">Failed Checks</h3>
        <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
            <tr style="color: #8B949E; text-align: left;">
                <th style="padding: 8px;">Check</th>
                <th style="padding: 8px;">Category</th>
                <th style="padding: 8px;">Severity</th>
                <th style="padding: 8px;">Details</th>
            </tr>
            {check_rows}
        </table>

        <div style="margin-top: 24px; padding-top: 16px; border-top: 1px solid #30363D;
                    text-align: center; font-size: 11px; color: #8B949E;">
            Financial Data Quality Monitor · Generated {payload['timestamp'][:19]}
        </div>
    </body>
    </html>
    """
    return html


def send_email_alert(payload: Dict) -> bool:
    """
    Send an HTML email alert via SMTP.

    Returns True if sent successfully, False otherwise.
    """
    settings = get_settings()

    if not settings.alerts.enable_email_alerts:
        logger.info("Email alerts disabled — skipping")
        return False

    if not settings.alerts.alert_email_to:
        logger.warning("No alert email recipient configured")
        return False

    level = payload["level"]
    subject = (
        f"[FDQM {level}] Data Health {payload['health_score']:.1f}% "
        f"({payload['health_color']}) — {payload['run_date']}"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = settings.alerts.smtp_user or "fdqm@localhost"
    msg["To"] = settings.alerts.alert_email_to

    # Plain text fallback
    text_body = (
        f"FDQM Alert: {level}\n"
        f"Date: {payload['run_date']}\n"
        f"Health Score: {payload['health_score']:.1f}% ({payload['health_color']})\n"
        f"Total Checks: {payload['total_checks']}\n"
        f"Failures: {payload['total_failures']}\n"
        f"Critical: {payload['critical_failures']}\n"
    )
    msg.attach(MIMEText(text_body, "plain"))

    # HTML body
    html_body = _build_email_html(payload)
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(settings.alerts.smtp_host, settings.alerts.smtp_port) as server:
            server.starttls()
            if settings.alerts.smtp_user and settings.alerts.smtp_password:
                server.login(settings.alerts.smtp_user, settings.alerts.smtp_password)
            server.send_message(msg)

        logger.info(
            "Email alert sent",
            to=settings.alerts.alert_email_to,
            level=level,
            subject=subject,
        )
        return True

    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")
        return False


# ── Main alert dispatcher ────────────────────────────────────

def dispatch_alerts(
    run_date: date,
    health_score: Dict,
    failed_checks: List[Dict],
    total_checks: int,
) -> Dict:
    """
    Main entry point for alerting. Builds the alert payload and
    dispatches to all configured channels.

    Always sends to log. Sends email only if:
      - Email alerts are enabled in config
      - Alert level is CRITICAL or HIGH

    Returns:
        Dict with alert payload and dispatch results
    """
    payload = build_alert_payload(
        run_date=run_date,
        health_score=health_score,
        failed_checks=failed_checks,
        total_checks=total_checks,
    )

    level = payload["level"]
    should_notify = ALERT_LEVELS.get(level, {}).get("notify", False)

    # Always log
    send_log_alert(payload)

    # Email if configured and severity warrants it
    email_sent = False
    if should_notify:
        email_sent = send_email_alert(payload)

    result = {
        "alert_level": level,
        "log_sent": True,
        "email_sent": email_sent,
        "should_notify": should_notify,
        "payload": payload,
    }

    logger.info(
        "Alert dispatch complete",
        level=level,
        email_sent=email_sent,
        total_failures=payload["total_failures"],
    )

    return result