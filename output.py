"""Output module: CSV and Markdown report generation."""

from __future__ import annotations

import csv
import shutil
import logging
from pathlib import Path
from typing import Any

from formatter import humanize_number, relative_time, format_duration

logger = logging.getLogger(__name__)

CATEGORY_ZH: dict[str, str] = {
    "Film & Animation": "电影和动画",
    "Autos & Vehicles": "汽车和交通",
    "Music": "音乐",
    "Pets & Animals": "宠物和动物",
    "Sports": "体育",
    "Travel & Events": "旅行和活动",
    "Gaming": "游戏",
    "People & Blogs": "人物和博客",
    "Comedy": "喜剧",
    "Entertainment": "娱乐",
    "News & Politics": "新闻和政治",
    "Howto & Style": "技巧和时尚",
    "Education": "教育",
    "Science & Technology": "科学和技术",
    "Nonprofits & Activism": "非营利和行动主义",
    "AI": "人工智能",
    "LLM": "大模型",
    "AI News": "AI 资讯",
    "Tech": "技术开发",
    "AI Course": "AI 高校课程",
}

FIELD_ORDER = [
    "video_id", "title", "channel_id", "channel_name",
    "category_id", "category_name", "published_at",
    "view_count", "like_count", "comment_count",
    "duration_seconds", "thumbnail_url", "tags",
    "fetched_at", "region",
]


def write_csv(records: list[dict[str, Any]], filepath: str | Path) -> None:
    """Write records to a CSV file with BOM encoding for Excel compatibility.

    Args:
        records: List of record dicts to write.
        filepath: Destination file path.
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_ORDER, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    logger.info("Wrote %d records to %s", len(records), filepath)


def write_markdown(
    records: list[dict[str, Any]],
    filepath: str | Path,
    display_timezone: str = "UTC",
    priority_categories: list[str] | None = None,
) -> None:
    """Write a Markdown report grouped by category with formatted tables.

    Args:
        records: List of record dicts.
        filepath: Destination file path.
        display_timezone: Timezone name for display (informational).
        priority_categories: Category names to display first, in order.
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Group by category
    groups: dict[str, list[dict[str, Any]]] = {}
    for rec in records:
        cat = rec.get("category_name", "Unknown")
        groups.setdefault(cat, []).append(rec)

    lines: list[str] = []
    lines.append(f"# YouTube 热门视频报告")
    lines.append("")
    lines.append(f"**视频总数:** {len(records)}  ")
    lines.append(f"**时区:** {display_timezone}")
    lines.append("")

    # Order: priority categories first (in order), then rest sorted alphabetically
    priority = priority_categories or []
    ordered = [c for c in priority if c in groups]
    ordered += sorted(c for c in groups if c not in priority)

    for category_name in ordered:
        cat_records = groups[category_name]
        # Sort by view_count descending, None last
        cat_records.sort(key=lambda r: (r.get("view_count") is None, -(r.get("view_count") or 0)))

        zh_name = CATEGORY_ZH.get(category_name, category_name)
        lines.append(f"## {zh_name} ({category_name})")
        lines.append("")
        lines.append("| # | 标题 | 频道 | 播放量 | 点赞 | 时长 | 发布时间 | 链接 |")
        lines.append("|---|------|------|--------|------|------|----------|------|")

        for i, rec in enumerate(cat_records, 1):
            title = _escape_md(rec.get("title", "—"))
            channel = _escape_md(rec.get("channel_name", "—"))
            views = humanize_number(rec.get("view_count"))
            likes = humanize_number(rec.get("like_count"))
            duration = format_duration(rec.get("duration_seconds"))
            published = relative_time(rec.get("published_at"))
            video_id = rec.get("video_id", "")
            url = f"https://www.youtube.com/watch?v={video_id}" if video_id else "—"

            lines.append(
                f"| {i} | {title} | {channel} | {views} | {likes} | {duration} | {published} | [观看]({url}) |"
            )

        lines.append("")

    filepath.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Wrote Markdown report to %s", filepath)


def update_latest(snapshot_path: str | Path, output_dir: str | Path) -> None:
    """Copy the snapshot CSV to latest.csv in the output directory.

    Args:
        snapshot_path: Source CSV file path.
        output_dir: Output directory where latest.csv will be placed.
    """
    dest = Path(output_dir) / "latest.csv"
    shutil.copy2(str(snapshot_path), str(dest))
    logger.info("Updated %s", dest)


def _escape_md(text: str) -> str:
    """Escape pipe characters in Markdown table cells."""
    return text.replace("|", "\\|")
