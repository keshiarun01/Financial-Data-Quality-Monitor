"""
Quality Engine — orchestrates all data quality checks.
Single entry point that runs null checks, stale detection,
anomaly detection, cross-source reconciliation, and gap detection.
"""

from datetime import date, datetime
from typing import Dict, List, Optional

from src.quality.null_checks import run_all_null_checks
from src.quality.stale_detection import run_all_stale_checks
from src.quality.anomaly_detection import (
    run_equity_anomaly_detection,
    run_macro_anomaly_detection,
)
from src.quality.cross_source_recon import run_all_reconciliation
from src.quality.gap_detection import run_all_gap_detection
from src.quality.ge_validation import run_all_ge_validations

import structlog

logger = structlog.get_logger(__name__)


def compute_health_score(results: List[Dict]) -> Dict:
    """
    Compute overall data health score from QC results.

    Scoring:
        PASS = 1.0, WARN = 0.5, FAIL = 0.0
        Score = weighted average (CRITICAL checks weighted 3x)

    Returns:
        Dict with overall score, status color, and per-category breakdown
    """
    if not results:
        return {"score": 0.0, "color": "RED", "categories": {}}

    severity_weights = {
        "CRITICAL": 3.0,
        "HIGH": 2.0,
        "MEDIUM": 1.0,
        "LOW": 0.5,
    }
    status_scores = {"PASS": 1.0, "WARN": 0.5, "FAIL": 0.0}

    total_weight = 0.0
    weighted_score = 0.0
    categories = {}

    for r in results:
        weight = severity_weights.get(r.get("severity", "MEDIUM"), 1.0)
        score = status_scores.get(r.get("status", "FAIL"), 0.0)

        total_weight += weight
        weighted_score += score * weight

        # Per-category tracking
        cat = r.get("check_category", r.get("check_name", "unknown").split("_")[0])
        if cat not in categories:
            categories[cat] = {"pass": 0, "warn": 0, "fail": 0, "total": 0}
        categories[cat]["total"] += 1
        categories[cat][r.get("status", "FAIL").lower()] += 1

    overall_score = weighted_score / total_weight if total_weight > 0 else 0.0

    # Determine color: GREEN >= 0.9, YELLOW >= 0.7, RED < 0.7
    if overall_score >= 0.9:
        color = "GREEN"
    elif overall_score >= 0.7:
        color = "YELLOW"
    else:
        color = "RED"

    # Per-category scores
    for cat, counts in categories.items():
        cat_total = counts["total"]
        if cat_total > 0:
            cat_score = (
                counts["pass"] * 1.0 +
                counts["warn"] * 0.5 +
                counts["fail"] * 0.0
            ) / cat_total
            categories[cat]["score"] = round(cat_score * 100, 1)
            if cat_score >= 0.9:
                categories[cat]["color"] = "GREEN"
            elif cat_score >= 0.7:
                categories[cat]["color"] = "YELLOW"
            else:
                categories[cat]["color"] = "RED"

    return {
        "score": round(overall_score * 100, 1),
        "color": color,
        "categories": categories,
    }


def run_full_quality_suite(
    run_date: Optional[date] = None,
) -> Dict:
    """
    Execute the complete quality control suite.

    Runs all checks in order:
        1. Null checks
        2. Stale data detection
        3. Anomaly detection (equities + macro)
        4. Cross-source reconciliation
        5. Holiday-aware gap detection

    Returns:
        Dict with all results and overall health score
    """
    if run_date is None:
        run_date = date.today()

    started_at = datetime.now()
    all_results = []

    logger.info(f"Starting full quality suite for {run_date}")

    # ── 0. Great Expectations Validation ─────────────────
    logger.info("=" * 50)
    logger.info("Phase 0: Great Expectations Validation")
    ge_results = []
    try:
        ge_results = run_all_ge_validations(run_date)
        for r in ge_results:
            all_results.append({
                "check_name": f"ge_{r['suite_name']}",
                "check_category": "great_expectations",
                "target_table": r["suite_name"].replace("_suite", ""),
                "status": "PASS" if r["success"] else "FAIL",
                "severity": "HIGH",
                "records_checked": r["evaluated"],
                "records_failed": r["unsuccessful"],
                "failure_rate": r["unsuccessful"] / max(r["evaluated"], 1),
                "details": {
                    "success_rate": r["success_rate"],
                    "failed_expectations": r["failed_expectations"],
                },
            })
    except Exception as e:
        logger.error(f"Great Expectations validation failed: {e}")

    # ── 1. Null Checks ───────────────────────────────────
    logger.info("=" * 50)
    logger.info("Phase 1: Null Checks")
    try:
        null_results = run_all_null_checks(run_date)
        for r in null_results:
            r["check_category"] = "null_check"
        all_results.extend(null_results)
    except Exception as e:
        logger.error(f"Null checks failed: {e}")

    # ── 2. Stale Detection ───────────────────────────────
    logger.info("=" * 50)
    logger.info("Phase 2: Stale Data Detection")
    try:
        stale_results = run_all_stale_checks(run_date)
        for r in stale_results:
            r["check_category"] = "stale"
        all_results.extend(stale_results)
    except Exception as e:
        logger.error(f"Stale checks failed: {e}")

    # ── 3. Anomaly Detection ─────────────────────────────
    logger.info("=" * 50)
    logger.info("Phase 3: Anomaly Detection")
    try:
        eq_anomaly_results = run_equity_anomaly_detection(run_date)
        for r in eq_anomaly_results:
            r["check_category"] = "anomaly"
        all_results.extend(eq_anomaly_results)
    except Exception as e:
        logger.error(f"Equity anomaly detection failed: {e}")

    try:
        macro_anomaly_results = run_macro_anomaly_detection(run_date)
        for r in macro_anomaly_results:
            r["check_category"] = "anomaly"
        all_results.extend(macro_anomaly_results)
    except Exception as e:
        logger.error(f"Macro anomaly detection failed: {e}")

    # ── 4. Cross-Source Reconciliation ────────────────────
    logger.info("=" * 50)
    logger.info("Phase 4: Cross-Source Reconciliation")
    try:
        recon_results = run_all_reconciliation(run_date)
        for r in recon_results:
            r["check_category"] = "recon"
        all_results.extend(recon_results)
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")

    # ── 5. Gap Detection ─────────────────────────────────
    logger.info("=" * 50)
    logger.info("Phase 5: Holiday-Aware Gap Detection")
    try:
        gap_results = run_all_gap_detection(run_date)
        for r in gap_results:
            r["check_category"] = "gap"
        all_results.extend(gap_results)
    except Exception as e:
        logger.error(f"Gap detection failed: {e}")

    # ── Compute health score ─────────────────────────────
    health = compute_health_score(all_results)
    duration = (datetime.now() - started_at).total_seconds()

    logger.info("=" * 50)
    logger.info(
        f"Quality suite complete: score={health['score']}% "
        f"({health['color']}) in {duration:.1f}s"
    )
    logger.info(f"Total checks: {len(all_results)}")

    return {
        "run_date": str(run_date),
        "health_score": health,
        "total_checks": len(all_results),
        "results_summary": {
            "pass": sum(1 for r in all_results if r.get("status") == "PASS"),
            "warn": sum(1 for r in all_results if r.get("status") == "WARN"),
            "fail": sum(1 for r in all_results if r.get("status") == "FAIL"),
        },
        "duration_seconds": round(duration, 2),
        "results": all_results,
    }