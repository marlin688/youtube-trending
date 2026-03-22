"""Aggregator module: transforms raw API data into clean, deduplicated records."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_ISO8601_DURATION_RE = re.compile(
    r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$"
)


def parse_duration(iso_duration: str | None) -> int | None:
    """Parse an ISO 8601 duration string (e.g. 'PT1H2M30S') into total seconds.

    Returns None if the input is None or cannot be parsed.
    """
    if not iso_duration:
        return None
    m = _ISO8601_DURATION_RE.match(iso_duration)
    if not m:
        logger.warning("Unparseable duration: %s", iso_duration)
        return None
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def _safe_int(value: Any) -> int | None:
    """Convert a value to int, returning None if missing or invalid."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def aggregate(
    raw_items: list[dict[str, Any]],
    category_id: str,
    category_name: str,
    region: str,
) -> list[dict[str, Any]]:
    """Transform raw YouTube API items into standardized records.

    Deduplicates by video_id within this batch.
    """
    seen: set[str] = set()
    records: list[dict[str, Any]] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for item in raw_items:
        video_id = item.get("id", "")
        if not video_id or video_id in seen:
            continue
        seen.add(video_id)

        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})

        tags = snippet.get("tags", [])
        thumbnails = snippet.get("thumbnails", {})
        thumbnail_url = (
            thumbnails.get("high", {}).get("url")
            or thumbnails.get("medium", {}).get("url")
            or thumbnails.get("default", {}).get("url", "")
        )

        records.append({
            "video_id": video_id,
            "title": snippet.get("title", ""),
            "channel_id": snippet.get("channelId", ""),
            "channel_name": snippet.get("channelTitle", ""),
            "category_id": category_id,
            "category_name": category_name,
            "published_at": snippet.get("publishedAt", ""),
            "view_count": _safe_int(stats.get("viewCount")),
            "like_count": _safe_int(stats.get("likeCount")),
            "comment_count": _safe_int(stats.get("commentCount")),
            "duration_seconds": parse_duration(content.get("duration")),
            "thumbnail_url": thumbnail_url,
            "tags": json.dumps(tags, ensure_ascii=False),
            "fetched_at": fetched_at,
            "region": region,
        })

    return records


def deduplicate(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate records by video_id across all categories, keeping first occurrence."""
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for rec in records:
        vid = rec["video_id"]
        if vid not in seen:
            seen.add(vid)
            result.append(rec)
    return result


def filter_records(
    records: list[dict[str, Any]],
    min_duration_seconds: int = 0,
    max_video_age_days: int = 0,
    min_view_count: int = 0,
) -> list[dict[str, Any]]:
    """Filter records by duration, age, and minimum views.

    Duration and view count filters apply to all records.
    Age filter only applies to custom categories (category_id starting with "custom_").

    Args:
        records: List of record dicts.
        min_duration_seconds: Exclude videos shorter than this (0 = no filter).
        max_video_age_days: Exclude videos published more than N days ago (0 = no filter).
        min_view_count: Exclude videos with fewer views (0 = no filter).
    """
    result: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for rec in records:
        # Filter by duration
        if min_duration_seconds > 0:
            dur = rec.get("duration_seconds")
            if dur is not None and dur < min_duration_seconds:
                continue

        # Filter by minimum view count
        if min_view_count > 0:
            views = rec.get("view_count")
            if views is not None and views < min_view_count:
                continue

        # Filter by age (custom categories only, respects per-record _max_age_days)
        cat_id = str(rec.get("category_id", ""))
        if cat_id.startswith("custom_"):
            rec_max_age = rec.get("_max_age_days", max_video_age_days)
            if rec_max_age > 0:
                pub = rec.get("published_at", "")
                if pub:
                    try:
                        dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                        age_days = (now - dt).total_seconds() / 86400
                        if age_days > rec_max_age:
                            continue
                    except (ValueError, TypeError):
                        pass

        result.append(rec)

    filtered = len(records) - len(result)
    if filtered:
        logger.info("Filtered out %d records (min_duration=%ds, max_age=%dd, min_views=%d)",
                     filtered, min_duration_seconds, max_video_age_days, min_view_count)
    return result
