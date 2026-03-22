"""Formatter module: human-friendly number, time, and duration formatting."""

from __future__ import annotations

from datetime import datetime, timezone


def humanize_number(n: int | None) -> str:
    """Format a number into a human-readable short form.

    Examples:
        None  → "—"
        0     → "0"
        999   → "999"
        1234  → "1.2K"
        1234567   → "1.2M"
        1234567890 → "1.2B"

    Args:
        n: The number to format, or None.

    Returns:
        A human-readable string representation.
    """
    if n is None:
        return "—"
    if abs(n) < 1000:
        return str(n)
    for threshold, suffix in [(1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")]:
        if abs(n) >= threshold:
            value = n / threshold
            return f"{value:.1f}{suffix}"
    return str(n)


def relative_time(iso_str: str | None) -> str:
    """Convert an ISO 8601 UTC timestamp to a relative time string.

    Examples:
        "2024-01-01T00:00:00Z" → "3d ago" (if now is 3 days later)
        None → "—"

    Args:
        iso_str: An ISO 8601 datetime string, or None.

    Returns:
        A relative time string like "2h ago" or "3d ago".
    """
    if not iso_str:
        return "—"
    try:
        # Handle both "Z" suffix and "+00:00" style
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        diff = now - dt
        total_seconds = int(diff.total_seconds())

        if total_seconds < 0:
            return "just now"
        if total_seconds < 60:
            return f"{total_seconds}s ago"
        minutes = total_seconds // 60
        if minutes < 60:
            return f"{minutes}m ago"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h ago"
        days = hours // 24
        if days < 30:
            return f"{days}d ago"
        months = days // 30
        if months < 12:
            return f"{months}mo ago"
        years = days // 365
        return f"{years}y ago"
    except (ValueError, TypeError):
        return "—"


def format_duration(seconds: int | None) -> str:
    """Format a duration in seconds to a human-readable string.

    Examples:
        None → "—"
        62   → "1:02"
        3661 → "1:01:01"
        0    → "0:00"

    Args:
        seconds: Duration in seconds, or None.

    Returns:
        Formatted duration string.
    """
    if seconds is None:
        return "—"
    if seconds < 0:
        return "—"

    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60

    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"
