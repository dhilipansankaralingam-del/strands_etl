"""
Holiday tools — public-holiday context for interpreting volume anomalies.

A validation rule that flags "fewer rows than expected" on a bank holiday is
almost certainly a FALSE_POSITIVE.  These tools give the validation agent that
calendar awareness.

Tools
-----
get_holiday_context     Check whether a specific date (or today) is a holiday
                        or falls within N days of one.
list_upcoming_holidays  List upcoming public holidays within a given window.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from typing import Optional

from strands import tool

try:
    import holidays as _holidays_lib  # type: ignore

    _HOLIDAYS_AVAILABLE = True
except ImportError:
    _HOLIDAYS_AVAILABLE = False

DEFAULT_COUNTRY: str = os.environ.get("HOLIDAY_COUNTRY", "US")
DEFAULT_SUBDIV: Optional[str] = os.environ.get("HOLIDAY_SUBDIV")  # e.g. "NY"
PROXIMITY_DAYS: int = int(os.environ.get("HOLIDAY_PROXIMITY_DAYS", "2"))


def _holiday_calendar(country: str, subdiv: Optional[str] = None, year: int = 0):
    """Return a holidays object for the given country / subdivision / year."""
    if not _HOLIDAYS_AVAILABLE:
        return {}
    yr = year or date.today().year
    kwargs = {"years": [yr, yr + 1]}
    if subdiv:
        kwargs["subdiv"] = subdiv
    try:
        return _holidays_lib.country_holidays(country, **kwargs)
    except (KeyError, NotImplementedError):
        return {}


def _parse_date(date_str: Optional[str]) -> date:
    """Parse an ISO-8601 date or datetime string; fall back to today."""
    if not date_str:
        return date.today()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str[:19], fmt).date()
        except ValueError:
            continue
    return date.today()


# ---------------------------------------------------------------------------
# Public tools
# ---------------------------------------------------------------------------


@tool
def get_holiday_context(
    date_str: Optional[str] = None,
    country: str = DEFAULT_COUNTRY,
    proximity_days: int = PROXIMITY_DAYS,
) -> str:
    """
    Check whether a given date (or today) is a public holiday or falls within
    *proximity_days* of one.  Use this before classifying a volume-based FAIL
    to determine whether lower-than-expected row counts are holiday-driven.

    Parameters
    ----------
    date_str       : ISO-8601 date or datetime string (e.g. '2024-12-25' or
                     '2024-12-25T08:00:00').  Defaults to today if omitted.
    country        : ISO-3166-1 alpha-2 country code (default: US).
    proximity_days : How many days before/after a holiday to flag as 'near'
                     (default: 2).

    Returns
    -------
    JSON string with keys:
        is_holiday        bool   – True if the date itself is a holiday.
        holiday_name      str    – Name of the holiday (empty string if none).
        is_near_holiday   bool   – True if within proximity_days of a holiday.
        nearby_holidays   list   – [{date, name, days_away}] within the window.
        recommendation    str    – Plain-English note for the classification agent.
    """
    target = _parse_date(date_str)
    cal = _holiday_calendar(country, DEFAULT_SUBDIV, target.year)

    is_holiday = target in cal
    holiday_name = cal.get(target, "")

    nearby: list[dict] = []
    for offset in range(-proximity_days, proximity_days + 1):
        if offset == 0:
            continue
        candidate = target + timedelta(days=offset)
        if candidate in cal:
            nearby.append(
                {
                    "date": candidate.isoformat(),
                    "name": cal[candidate],
                    "days_away": offset,
                }
            )

    is_near = bool(is_holiday or nearby)

    if is_holiday:
        recommendation = (
            f"Date {target} IS the holiday '{holiday_name}'.  Volume-based "
            "validation failures are very likely FALSE_POSITIVE."
        )
    elif nearby:
        names = ", ".join(h["name"] for h in nearby)
        recommendation = (
            f"Date {target} is within {proximity_days} days of: {names}.  "
            "Consider FALSE_POSITIVE for row-count or threshold rules."
        )
    else:
        recommendation = (
            f"Date {target} has no nearby public holidays ({country}).  "
            "Volume anomalies are not holiday-driven."
        )

    return json.dumps(
        {
            "is_holiday": is_holiday,
            "holiday_name": str(holiday_name),
            "is_near_holiday": is_near,
            "nearby_holidays": nearby,
            "recommendation": recommendation,
        }
    )


@tool
def list_upcoming_holidays(
    days_ahead: int = 14,
    country: str = DEFAULT_COUNTRY,
) -> str:
    """
    List public holidays in the next *days_ahead* calendar days.

    Useful for proactively flagging validation rules that may produce
    FALSE_POSITIVE failures around upcoming holidays.

    Parameters
    ----------
    days_ahead : Number of days to look ahead (default: 14).
    country    : ISO-3166-1 alpha-2 country code (default: US).

    Returns
    -------
    JSON string with keys:
        country          str   – Country code used.
        window_start     str   – Today's ISO date.
        window_end       str   – End of window ISO date.
        holidays         list  – [{date, name, days_from_today}].
        count            int   – Number of holidays found.
    """
    today = date.today()
    window_end = today + timedelta(days=int(days_ahead))
    cal = _holiday_calendar(country, DEFAULT_SUBDIV, today.year)

    found: list[dict] = []
    cursor = today
    while cursor <= window_end:
        if cursor in cal:
            found.append(
                {
                    "date": cursor.isoformat(),
                    "name": cal[cursor],
                    "days_from_today": (cursor - today).days,
                }
            )
        cursor += timedelta(days=1)

    return json.dumps(
        {
            "country": country,
            "window_start": today.isoformat(),
            "window_end": window_end.isoformat(),
            "holidays": found,
            "count": len(found),
        }
    )
