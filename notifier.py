"""Notifier module: optional Telegram notification for run summaries."""

from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error
from typing import Any

from config import Config

logger = logging.getLogger(__name__)


def send_summary(
    config: Config,
    total: int,
    category_counts: dict[str, int],
    errors: list[str],
    date_str: str,
) -> None:
    """Send a run summary via Telegram if configured.

    Silently skips if Telegram credentials are not set.
    Logs but does not raise on send failure.

    Args:
        config: Application configuration.
        total: Total number of videos fetched.
        category_counts: Mapping of category name to video count.
        errors: List of error messages encountered.
        date_str: Date string for the report.
    """
    if not config.telegram_bot_token or not config.telegram_chat_id:
        logger.debug("Telegram not configured, skipping notification")
        return

    lines = [
        f"📊 YouTube Trending Report — {date_str}",
        f"Total videos: {total}",
        "",
        "Categories:",
    ]
    for cat, count in sorted(category_counts.items()):
        lines.append(f"  • {cat}: {count}")

    if errors:
        lines.append("")
        lines.append(f"⚠️ Errors ({len(errors)}):")
        for err in errors[:5]:
            lines.append(f"  • {err}")
        if len(errors) > 5:
            lines.append(f"  ... and {len(errors) - 5} more")

    message = "\n".join(lines)

    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
    payload = json.dumps({
        "chat_id": config.telegram_chat_id,
        "text": message,
        "parse_mode": "HTML",
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                logger.info("Telegram notification sent successfully")
            else:
                logger.warning("Telegram API returned status %d", resp.status)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
        logger.warning("Failed to send Telegram notification: %s", e)
