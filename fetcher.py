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


def parse_custom_categories(raw: str) -> list[dict[str, Any]]:
    """Parse CUSTOM_CATEGORIES config string into structured list.

    Format: "Name1:kw1|kw2|kw3;Name2:kw4|kw5"
    Append @days,order to override search params, e.g. "AI Course:kw1|kw2@90,relevance"

    Returns: [{"name": "Name1", "keywords": "kw1|kw2", "search_days": 7, "order": "viewCount"}, ...]
    """
    if not raw.strip():
        return []
    result = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if ":" not in entry:
            continue
        name, rest = entry.split(":", 1)
        name = name.strip()

        # Parse optional @days,order suffix
        search_days = 7
        order = "viewCount"
        if "@" in rest:
            keywords_part, params = rest.rsplit("@", 1)
            parts = params.split(",")
            if parts[0].strip().isdigit():
                search_days = int(parts[0].strip())
            if len(parts) > 1 and parts[1].strip():
                order = parts[1].strip()
        else:
            keywords_part = rest

        keywords = keywords_part.strip()
        if name and keywords:
            result.append({
                "name": name,
                "keywords": keywords,
                "search_days": search_days,
                "order": order,
            })
    return result


def fetch_custom_category_videos(
    config: Config,
    keywords: str,
    youtube: Any = None,
    search_days: int = 7,
    order: str = "viewCount",
) -> list[dict[str, Any]]:
    """Fetch videos by keyword search, then retrieve full details.

    Uses search.list to find video IDs, then videos.list for full data.
    Returns raw API video items, or an empty list on failure.
    Raises QuotaExceededError if the API quota is exhausted.

    Args:
        config: Application configuration.
        keywords: Pipe-separated keywords, e.g. "AI|ChatGPT|LLM".
        youtube: Optional pre-built YouTube API client.
        search_days: How many days back to search.
        order: Search result ordering ("viewCount" or "relevance").
    """
    if youtube is None:
        youtube = _build_youtube(config.youtube_api_key)

    # Split keywords into chunks to stay under YouTube API query length limit
    kw_list = [kw.strip() for kw in keywords.split("|") if kw.strip()]
    max_query_len = 120
    chunks: list[list[str]] = []
    current_chunk: list[str] = []
    current_len = 0
    for kw in kw_list:
        added_len = len(kw) + (len(" OR ") if current_chunk else 0)
        if current_chunk and current_len + added_len > max_query_len:
            chunks.append(current_chunk)
            current_chunk = [kw]
            current_len = len(kw)
        else:
            current_chunk.append(kw)
            current_len += added_len
    if current_chunk:
        chunks.append(current_chunk)

    # Search for videos matching keywords (multiple queries if needed)
    video_ids: list[str] = []
    for chunk in chunks:
        query = " OR ".join(chunk)
        search_response = _retry_call(
            lambda query=query: youtube.search().list(
                q=query,
                type="video",
                order=order,
                relevanceLanguage="en",
                publishedAfter=_recent_date_iso(search_days),
                part="id",
                maxResults=config.max_results_per_category,
            ).execute()
        )
        if search_response:
            video_ids.extend(
                item["id"]["videoId"]
                for item in search_response.get("items", [])
                if item.get("id", {}).get("videoId")
            )

    if not video_ids:
        return []
    # Deduplicate while preserving order
    video_ids = list(dict.fromkeys(video_ids))

    # Fetch full video details in batches of 50
    all_items: list[dict[str, Any]] = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        detail_response = _retry_call(
            lambda batch=batch: youtube.videos().list(
                id=",".join(batch),
                part="snippet,statistics,contentDetails",
            ).execute()
        )
        if detail_response:
            all_items.extend(detail_response.get("items", []))

    # Title/tags relevance validation: keep only videos whose title or tags
    # contain at least one search keyword (case-insensitive).
    # For multi-word keywords like "Jensen Huang interview", we check if ALL
    # individual words appear in the combined text (not necessarily adjacent).
    # This filters false positives like Spanish "llama" (=call) or French "mistral".
    validated_items = []
    kw_lower = [kw.strip().lower() for kw in kw_list]
    for item in all_items:
        title = item.get("snippet", {}).get("title", "").lower()
        tags = " ".join(t.lower() for t in item.get("snippet", {}).get("tags", []))
        desc = item.get("snippet", {}).get("description", "").lower()[:500]
        combined = f"{title} {tags} {desc}"
        matched = False
        for kw in kw_lower:
            words = kw.split()
            if len(words) <= 1:
                # Single word/phrase: direct substring match
                if kw in combined:
                    matched = True
                    break
            else:
                # Multi-word: all words must appear somewhere in text
                if all(w in combined for w in words):
                    matched = True
                    break
        if matched:
            validated_items.append(item)

    dropped = len(all_items) - len(validated_items)
    full_query = " OR ".join(kw_list)
    if dropped:
        logger.info("Title relevance filter: dropped %d/%d videos", dropped, len(all_items))
    if len(chunks) > 1:
        logger.info("Fetched %d videos in %d query chunks for: %s", len(validated_items), len(chunks), full_query)
    else:
        logger.info("Fetched %d videos for custom keywords: %s", len(validated_items), full_query)
    return validated_items


def parse_monitor_channels(raw: str) -> list[dict[str, str]]:
    """Parse MONITOR_CHANNELS config string into structured list.

    Format: "Label1:channel_id1,Label2:channel_id2"
    Channel IDs can be either UCxxxx channel IDs or @handle format.

    Returns: [{"name": "Label1", "channel_id": "UCxxxx"}, ...]
    """
    if not raw.strip():
        return []
    result = []
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        name, channel_id = entry.split(":", 1)
        name = name.strip()
        channel_id = channel_id.strip()
        if name and channel_id:
            result.append({"name": name, "channel_id": channel_id})
    return result


def fetch_channel_latest_videos(
    config: Config,
    channel_id: str,
    youtube: Any = None,
    max_results: int = 5,
    max_age_days: int = 7,
) -> list[dict[str, Any]]:
    """Fetch latest videos from a specific channel.

    Uses the channel's uploads playlist (UC -> UU) to get recent video IDs,
    then fetches full details. Filters by max_age_days.

    Returns raw API video items, or an empty list on failure.
    Raises QuotaExceededError if the API quota is exhausted.
    """
    if youtube is None:
        youtube = _build_youtube(config.youtube_api_key)

    # If channel_id starts with @, resolve to UC channel ID first
    if channel_id.startswith("@"):
        resolve_response = _retry_call(
            lambda: youtube.channels().list(
                forHandle=channel_id,
                part="id",
            ).execute()
        )
        if not resolve_response or not resolve_response.get("items"):
            logger.warning("Could not resolve handle %s to channel ID", channel_id)
            return []
        channel_id = resolve_response["items"][0]["id"]

    # Convert channel ID to uploads playlist ID (UC -> UU)
    if channel_id.startswith("UC"):
        uploads_playlist = "UU" + channel_id[2:]
    else:
        logger.warning("Unexpected channel ID format: %s", channel_id)
        return []

    # Fetch latest items from uploads playlist
    playlist_response = _retry_call(
        lambda: youtube.playlistItems().list(
            playlistId=uploads_playlist,
            part="contentDetails",
            maxResults=max_results,
        ).execute()
    )

    if not playlist_response:
        return []

    # Extract video IDs and filter by publish date
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    video_ids = []
    for item in playlist_response.get("items", []):
        pub_str = item.get("contentDetails", {}).get("videoPublishedAt", "")
        if pub_str:
            try:
                pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue
            except (ValueError, TypeError):
                pass
        vid = item.get("contentDetails", {}).get("videoId")
        if vid:
            video_ids.append(vid)

    if not video_ids:
        return []

    # Fetch full video details
    detail_response = _retry_call(
        lambda: youtube.videos().list(
            id=",".join(video_ids),
            part="snippet,statistics,contentDetails",
        ).execute()
    )

    if not detail_response:
        return []

    items = detail_response.get("items", [])
    logger.info("Fetched %d recent videos from channel %s", len(items), channel_id)
    return items


def _recent_date_iso(days: int = 3) -> str:
    """Return ISO 8601 datetime for N days ago (search time window)."""
    from datetime import timedelta
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%dT00:00:00Z")
