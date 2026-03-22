"""Main entry point: orchestrates the full YouTube trending data pipeline."""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from config import load_config
from fetcher import (
    fetch_categories, fetch_trending_videos, fetch_custom_category_videos,
    parse_custom_categories, QuotaExceededError,
)
from aggregator import aggregate, deduplicate, filter_records
from output import write_csv, write_markdown, update_latest
from cleaner import clean_old_files
from notifier import send_summary


def setup_logging(log_dir: str) -> None:
    """Configure logging to both stdout and a daily log file."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = Path(log_dir) / f"run-{date_str}.log"

    fmt = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(log_file), encoding="utf-8"),
    ]

    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)


def main() -> None:
    """Run the full pipeline."""
    start = time.monotonic()

    # 1. Load config
    config = load_config()

    # 2. Init logging
    setup_logging(config.log_dir)
    logger = logging.getLogger(__name__)
    logger.info("=== YouTube Trending Pipeline started ===")

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    errors: list[str] = []
    all_records: list[dict] = []
    skipped_categories: list[str] = []
    quota_hit = False

    # 3. Fetch categories
    categories = fetch_categories(config)
    if not categories:
        logger.error("No categories available, aborting")
        sys.exit(1)

    logger.info("Available categories: %s", ", ".join(f"{c['id']}={c['title']}" for c in categories))

    # 4. Determine target categories
    if config.categories:
        wanted = {cid.strip() for cid in config.categories.split(",") if cid.strip()}
        target = [c for c in categories if c["id"] in wanted]
        if not target:
            logger.warning("None of the configured categories %s are assignable", wanted)
            target = categories
    else:
        target = categories

    logger.info("Target categories (%d): %s", len(target), ", ".join(c["title"] for c in target))

    # 5. Fetch trending videos per category
    for cat in target:
        if quota_hit:
            skipped_categories.append(cat["title"])
            continue

        try:
            items = fetch_trending_videos(config, cat["id"])
        except QuotaExceededError:
            logger.error("Quota exceeded at category %s, stopping fetches", cat["title"])
            errors.append(f"Quota exceeded at category {cat['title']}")
            quota_hit = True
            skipped_categories.append(cat["title"])
            continue

        if not items:
            logger.warning("No videos for category %s (%s)", cat["id"], cat["title"])
            skipped_categories.append(cat["title"])
            continue

        # 6. Aggregate
        records = aggregate(items, cat["id"], cat["title"], config.region_code)
        all_records.extend(records)

    # 5b. Fetch custom categories (keyword-based search)
    custom_cats = parse_custom_categories(config.custom_categories)
    for ccat in custom_cats:
        if quota_hit:
            skipped_categories.append(ccat["name"])
            continue

        logger.info("Fetching custom category: %s", ccat["name"])
        try:
            items = fetch_custom_category_videos(config, ccat["keywords"])
        except QuotaExceededError:
            logger.error("Quota exceeded at custom category %s, stopping", ccat["name"])
            errors.append(f"Quota exceeded at custom category {ccat['name']}")
            quota_hit = True
            skipped_categories.append(ccat["name"])
            continue

        if not items:
            logger.warning("No videos for custom category %s", ccat["name"])
            skipped_categories.append(ccat["name"])
            continue

        records = aggregate(items, f"custom_{ccat['name']}", ccat["name"], config.region_code)
        all_records.extend(records)

    # Deduplicate across categories
    all_records = deduplicate(all_records)
    logger.info("Total records after dedup: %d", len(all_records))

    # 6b. Filter: remove Shorts and old videos
    all_records = filter_records(
        all_records,
        min_duration_seconds=config.min_duration_seconds,
        max_video_age_days=config.max_video_age_days,
    )
    logger.info("Total records after filtering: %d", len(all_records))

    # 7. Output CSV + Markdown
    snapshot_path = Path(config.output_dir) / "snapshots" / f"{date_str}.csv"
    report_path = Path(config.output_dir) / "reports" / f"{date_str}.md"

    # Custom categories come first in report, in their defined order
    priority_categories = [c["name"] for c in custom_cats]

    if all_records:
        write_csv(all_records, snapshot_path)
        write_markdown(all_records, report_path, config.display_timezone, priority_categories)

        # 8. Update latest.csv
        update_latest(snapshot_path, config.output_dir)
    else:
        logger.warning("No records to write")

    # 9. Clean old files
    clean_old_files(config.output_dir, config.retention_days)

    # 10. Health check: core categories
    core_ids = {"10", "20", "24"}
    found_core = {r["category_id"] for r in all_records if r["category_id"] in core_ids}
    missing_core = core_ids - found_core
    if missing_core:
        logger.warning("Health check: missing data for core categories: %s", missing_core)
        errors.append(f"Missing core categories: {missing_core}")

    # 11. Notify
    category_counts = {}
    for rec in all_records:
        cat = rec["category_name"]
        category_counts[cat] = category_counts.get(cat, 0) + 1

    send_summary(config, len(all_records), category_counts, errors, date_str)

    # 12. Summary log
    elapsed = time.monotonic() - start
    logger.info("=== Pipeline complete ===")
    logger.info("Total records: %d", len(all_records))
    logger.info("Skipped categories: %s", skipped_categories or "none")
    logger.info("Errors: %d", len(errors))
    logger.info("Elapsed: %.1fs", elapsed)


if __name__ == "__main__":
    main()
