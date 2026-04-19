"""
strands_agents/tools/holiday_tools.py
======================================
Strands @tool for US holiday calendar lookups.
Used by the validation agent to factor in holiday volume suppressions.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any

from strands import tool

HOLIDAY_CALENDAR_PATH = os.environ.get("HOLIDAY_CALENDAR_PATH",
                                        "config/holiday_calendar.json")

_calendar_cache: dict | None = None


def _load_calendar() -> dict:
    global _calendar_cache
    if _calendar_cache is not None:
        return _calendar_cache
    path = HOLIDAY_CALENDAR_PATH
    if path.startswith("s3://"):
        import boto3
        bucket, key = path[5:].split("/", 1)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        _calendar_cache = json.loads(body)
    else:
        with open(path) as fh:
            _calendar_cache = json.load(fh)
    return _calendar_cache


@tool
def get_holiday_context(date_str: str) -> str:
    """
    Check if a given date is a US holiday and return volume adjustment context.

    The agent uses this BEFORE classifying a ROW_COUNT or volume-related failure.
    If the date is a holiday, low-volume failures may be FALSE_POSITIVE.

    Args:
        date_str: Date in YYYY-MM-DD format.

    Returns:
        JSON with is_holiday, holiday_name, volume_multiplier,
        post_holiday_surge_multiplier, zscore_threshold, and a text summary.
    """
    try:
        cal  = _load_calendar()
        year = date_str[:4]

        entry = (
            cal.get(f"explicit_dates_{year}", {}).get(date_str)
            or cal.get(f"explicit_dates_{int(year) - 1}", {}).get(date_str)
        )

        if not entry:
            thresholds = cal.get("zscore_thresholds", {})
            normal_z   = thresholds.get("normal", {}).get("alert_zscore", 3.0)
            return json.dumps({
                "is_holiday":                    False,
                "date":                          date_str,
                "volume_multiplier":             1.0,
                "post_holiday_surge_multiplier": 1.0,
                "zscore_threshold":              normal_z,
                "summary": f"{date_str} is not a holiday. Use standard Z-score threshold ({normal_z}σ).",
            })

        holiday_name = entry.get("holiday", "").replace("_", " ").title()
        multiplier   = entry.get("multiplier", 1.0)
        surge_next   = entry.get("surge_next", 1.0)
        context_key  = entry.get("zscore_context", "holiday_low_volume")
        thresholds   = cal.get("zscore_thresholds", {})
        zscore_thr   = thresholds.get(context_key, {}).get("alert_zscore", 99.0)

        # Check if tomorrow is a post-holiday surge day
        tomorrow = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        tom_year = tomorrow[:4]
        tom_entry = cal.get(f"explicit_dates_{tom_year}", {}).get(tomorrow)
        is_pre_surge = tom_entry is not None

        summary = (
            f"{date_str} is *{holiday_name}*. "
            f"Expected volume: {multiplier:.0%} of normal. "
            f"Z-score alerting threshold: {zscore_thr}σ (effectively suppressed if 99+). "
            f"ROW_COUNT and volume failures on this date should be classified FALSE_POSITIVE "
            f"unless actual count is below {multiplier * 0.5:.0%} of normal."
        )
        if surge_next > 1.0:
            summary += f" Next-day surge expected: {surge_next:.0%} of normal."

        return json.dumps({
            "is_holiday":                    True,
            "date":                          date_str,
            "holiday_name":                  holiday_name,
            "volume_multiplier":             multiplier,
            "post_holiday_surge_multiplier": surge_next,
            "zscore_threshold":              zscore_thr,
            "is_pre_surge_day":              is_pre_surge,
            "summary":                       summary,
        })

    except FileNotFoundError:
        return json.dumps({
            "is_holiday":        False,
            "date":              date_str,
            "error":             "Holiday calendar not found",
            "zscore_threshold":  3.0,
            "summary":           "Holiday calendar unavailable — use default thresholds.",
        })
    except Exception as exc:
        return json.dumps({"error": str(exc), "date": date_str})


@tool
def list_upcoming_holidays(days_ahead: int = 30) -> str:
    """
    List upcoming US holidays within the next N days.

    Args:
        days_ahead: Number of days to look ahead (default 30).

    Returns:
        JSON list of upcoming holiday entries.
    """
    cal      = _load_calendar()
    today    = datetime.utcnow().date()
    upcoming = []

    for offset in range(days_ahead):
        d = (today + timedelta(days=offset)).strftime("%Y-%m-%d")
        year = d[:4]
        entry = cal.get(f"explicit_dates_{year}", {}).get(d)
        if entry:
            upcoming.append({"date": d, **entry})

    return json.dumps({"upcoming_holidays": upcoming, "count": len(upcoming)})
