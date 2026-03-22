"""Tests for the formatter module."""

import unittest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta

from formatter import humanize_number, relative_time, format_duration


class TestHumanizeNumber(unittest.TestCase):

    def test_none(self):
        self.assertEqual(humanize_number(None), "—")

    def test_zero(self):
        self.assertEqual(humanize_number(0), "0")

    def test_small(self):
        self.assertEqual(humanize_number(999), "999")

    def test_thousands(self):
        self.assertEqual(humanize_number(1234), "1.2K")

    def test_millions(self):
        self.assertEqual(humanize_number(1234567), "1.2M")

    def test_billions(self):
        self.assertEqual(humanize_number(1234567890), "1.2B")

    def test_exact_thousand(self):
        self.assertEqual(humanize_number(1000), "1.0K")

    def test_negative(self):
        self.assertEqual(humanize_number(-1500), "-1.5K")

    def test_large_billion(self):
        self.assertEqual(humanize_number(9_999_999_999), "10.0B")


class TestRelativeTime(unittest.TestCase):

    def test_none(self):
        self.assertEqual(relative_time(None), "—")

    def test_empty_string(self):
        self.assertEqual(relative_time(""), "—")

    def test_invalid(self):
        self.assertEqual(relative_time("not-a-date"), "—")

    def test_seconds_ago(self):
        now = datetime.now(timezone.utc)
        t = (now - timedelta(seconds=30)).isoformat()
        self.assertEqual(relative_time(t), "30s ago")

    def test_minutes_ago(self):
        now = datetime.now(timezone.utc)
        t = (now - timedelta(minutes=45)).isoformat()
        self.assertEqual(relative_time(t), "45m ago")

    def test_hours_ago(self):
        now = datetime.now(timezone.utc)
        t = (now - timedelta(hours=5)).isoformat()
        self.assertEqual(relative_time(t), "5h ago")

    def test_days_ago(self):
        now = datetime.now(timezone.utc)
        t = (now - timedelta(days=3)).isoformat()
        self.assertEqual(relative_time(t), "3d ago")

    def test_z_suffix(self):
        now = datetime.now(timezone.utc)
        t = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertEqual(relative_time(t), "2h ago")


class TestFormatDuration(unittest.TestCase):

    def test_none(self):
        self.assertEqual(format_duration(None), "—")

    def test_negative(self):
        self.assertEqual(format_duration(-1), "—")

    def test_zero(self):
        self.assertEqual(format_duration(0), "0:00")

    def test_seconds_only(self):
        self.assertEqual(format_duration(45), "0:45")

    def test_minutes_and_seconds(self):
        self.assertEqual(format_duration(62), "1:02")

    def test_hours(self):
        self.assertEqual(format_duration(3661), "1:01:01")

    def test_exact_hour(self):
        self.assertEqual(format_duration(3600), "1:00:00")

    def test_large(self):
        self.assertEqual(format_duration(36000), "10:00:00")


if __name__ == "__main__":
    unittest.main()
