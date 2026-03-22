"""Cleaner module: removes expired snapshot and report files."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def clean_old_files(output_dir: str | Path, retention_days: int) -> int:
    """Delete files older than retention_days from snapshots/ and reports/.

    Files are identified by a YYYY-MM-DD date in their filename.

    Args:
        output_dir: Base output directory containing snapshots/ and reports/.
        retention_days: Number of days to retain. 0 means skip cleaning.

    Returns:
        Number of files deleted.
    """
    if retention_days <= 0:
        logger.info("Retention days is %d, skipping cleanup", retention_days)
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_date = cutoff.date()
    deleted = 0

    output_path = Path(output_dir)
    for subdir in ["snapshots", "reports"]:
        folder = output_path / subdir
        if not folder.is_dir():
            continue

        for f in folder.iterdir():
            if not f.is_file():
                continue
            m = _DATE_RE.search(f.name)
            if not m:
                continue
            try:
                file_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
            except ValueError:
                continue

            if file_date < cutoff_date:
                f.unlink()
                logger.info("Deleted expired file: %s", f)
                deleted += 1

    logger.info("Cleanup complete: %d files deleted", deleted)
    return deleted
