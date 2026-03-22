"""Fetcher module: YouTube Data API v3 calls with caching and retry logic."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import Config

logger = logging.getLogger(__name__)


class QuotaExceededError(Exception):
    """Raised when the YouTube API quota is exhausted."""


def _is_quota_exceeded(error: HttpError) -> bool:
    """Check if an HttpError is specifically a quota exceeded error."""
    try:
        detail = json.loads(error.content.decode("utf-8"))
        for err in detail.get("error", {}).get("errors", []):
            if err.get("reason") == "quotaExceeded":
                return True
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        pass
    return False


def _should_retry(error: HttpError) -> bool:
    """Determine if an HttpError warrants a retry."""
    status = error.resp.status
    if status == 403 and _is_quota_exceeded(error):
        return False
    return status in (403, 429) or 500 <= status < 600


def _retry_call(func: Any, max_retries: int = 3) -> Any:
    """Execute an API call with exponential back-off retry logic.

    Raises QuotaExceededError immediately on quota errors.
    Returns None after exhausting retries for transient errors.
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except HttpError as e:
            if e.resp.status == 403 and _is_quota_exceeded(e):
                logger.error("YouTube API quota exceeded.")
                raise QuotaExceededError("YouTube API quota exceeded") from e
            if not _should_retry(e) or attempt == max_retries:
                logger.error("API error (attempt %d/%d): %s", attempt + 1, max_retries + 1, e)
                return None
            wait = 2**attempt  # 1, 2, 4
            logger.warning("Retryable error (attempt %d/%d), waiting %ds: %s",
                           attempt + 1, max_retries + 1, wait, e)
            time.sleep(wait)
        except Exception as e:
            if attempt == max_retries:
                logger.error("Network/other error (attempt %d/%d): %s",
                             attempt + 1, max_retries + 1, e)
                return None
            wait = 2**attempt
            logger.warning("Transient error (attempt %d/%d), waiting %ds: %s",
                           attempt + 1, max_retries + 1, wait, e)
            time.sleep(wait)
    return None


def _build_youtube(api_key: str) -> Any:
    """Build a YouTube API client, respecting http_proxy/https_proxy env vars."""
    proxy_url = os.environ.get("https_proxy") or os.environ.get("http_proxy") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    if proxy_url:
        try:
            import socks
            parsed = urlparse(proxy_url)
            proxy_info = httplib2.ProxyInfo(
                proxy_type=socks.PROXY_TYPE_HTTP,
                proxy_host=parsed.hostname or "127.0.0.1",
                proxy_port=parsed.port or 7890,
            )
            http = httplib2.Http(proxy_info=proxy_info)
            return build("youtube", "v3", developerKey=api_key, http=http)
        except ImportError:
            logger.warning("pysocks not installed, proxy env vars ignored. Install with: pip install pysocks")
    return build("youtube", "v3", developerKey=api_key)


def fetch_categories(config: Config, youtube: Any = None) -> list[dict[str, str]]:
    """Fetch assignable video categories, with file-based caching.

    Returns a list of dicts with 'id' and 'title' keys.
    """
    cache_path = Path(config.cache_dir) / "categories.json"

    # Check cache
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            fetched_at = datetime.fromisoformat(data["fetched_at"])
            age_days = (datetime.now(timezone.utc) - fetched_at).days
            if age_days < config.categories_cache_ttl_days:
                logger.info("Using cached categories (age: %d days)", age_days)
                return data["categories"]
            logger.info("Categories cache expired (age: %d days)", age_days)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning("Invalid categories cache, refetching: %s", e)

    # Fetch from API
    if youtube is None:
        youtube = _build_youtube(config.youtube_api_key)

    response = _retry_call(
        lambda: youtube.videoCategories().list(
            regionCode=config.region_code,
            part="snippet",
        ).execute()
    )

    if response is None:
        logger.error("Failed to fetch categories")
        return []

    categories = [
        {"id": item["id"], "title": item["snippet"]["title"]}
        for item in response.get("items", [])
        if item.get("snippet", {}).get("assignable", False)
    ]

    # Write cache
    cache_data = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "categories": categories,
    }
    cache_path.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Cached %d assignable categories", len(categories))

    return categories


def fetch_trending_videos(
    config: Config,
    category_id: str,
    youtube: Any = None,
) -> list[dict[str, Any]]:
    """Fetch trending videos for a specific category.

    Returns raw API response items, or an empty list on failure.
    Raises QuotaExceededError if the API quota is exhausted.
    """
    if youtube is None:
        youtube = _build_youtube(config.youtube_api_key)

    response = _retry_call(
        lambda: youtube.videos().list(
            chart="mostPopular",
            videoCategoryId=category_id,
            regionCode=config.region_code,
            part="snippet,statistics,contentDetails",
            maxResults=config.max_results_per_category,
        ).execute()
    )

    if response is None:
        return []

    items = response.get("items", [])
    logger.info("Fetched %d videos for category %s", len(items), category_id)
    return items
