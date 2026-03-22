"""Tests for the output module."""

import csv
import tempfile
import unittest
from pathlib import Path

from output import write_csv, write_markdown, update_latest


class TestWriteCSV(unittest.TestCase):

    def _make_record(self, **overrides):
        rec = {
            "video_id": "v1", "title": "Test Video", "channel_id": "ch1",
            "channel_name": "Channel", "category_id": "10", "category_name": "Music",
            "published_at": "2024-01-01T00:00:00Z", "view_count": 1000,
            "like_count": 100, "comment_count": 10, "duration_seconds": 120,
            "thumbnail_url": "http://img.example.com/1.jpg",
            "tags": '["a","b"]', "fetched_at": "2024-01-01T12:00:00Z", "region": "US",
        }
        rec.update(overrides)
        return rec

    def test_basic_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.csv"
            write_csv([self._make_record()], path)

            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["video_id"], "v1")

    def test_title_with_comma(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.csv"
            write_csv([self._make_record(title="Hello, World")], path)

            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            self.assertEqual(rows[0]["title"], "Hello, World")

    def test_title_with_quotes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.csv"
            write_csv([self._make_record(title='He said "hello"')], path)

            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            self.assertEqual(rows[0]["title"], 'He said "hello"')

    def test_title_with_newline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.csv"
            write_csv([self._make_record(title="Line1\nLine2")], path)

            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            self.assertEqual(rows[0]["title"], "Line1\nLine2")

    def test_title_with_emoji(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.csv"
            write_csv([self._make_record(title="🎵 Music Video 🎶")], path)

            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            self.assertEqual(rows[0]["title"], "🎵 Music Video 🎶")

    def test_bom_present(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.csv"
            write_csv([self._make_record()], path)

            raw = path.read_bytes()
            self.assertTrue(raw.startswith(b"\xef\xbb\xbf"))


class TestWriteMarkdown(unittest.TestCase):

    def test_generates_report(self):
        records = [
            {
                "video_id": "v1", "title": "Test", "channel_name": "Ch",
                "category_name": "Music", "view_count": 1000, "like_count": 100,
                "duration_seconds": 60, "published_at": "2024-01-01T00:00:00Z",
            }
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.md"
            write_markdown(records, path)
            content = path.read_text(encoding="utf-8")
            self.assertIn("音乐 (Music)", content)
            self.assertIn("Test", content)
            self.assertIn("1.0K", content)


class TestUpdateLatest(unittest.TestCase):

    def test_copies_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "snapshot.csv"
            src.write_text("a,b\n1,2\n")
            update_latest(src, tmpdir)
            latest = Path(tmpdir) / "latest.csv"
            self.assertTrue(latest.exists())
            self.assertEqual(latest.read_text(), "a,b\n1,2\n")


if __name__ == "__main__":
    unittest.main()
