"""Tests for the aggregator module."""

import json
import unittest

from aggregator import parse_duration, aggregate, deduplicate


class TestParseDuration(unittest.TestCase):

    def test_none(self):
        self.assertIsNone(parse_duration(None))

    def test_empty(self):
        self.assertIsNone(parse_duration(""))

    def test_hours_only(self):
        self.assertEqual(parse_duration("PT1H"), 3600)

    def test_minutes_seconds(self):
        self.assertEqual(parse_duration("PT2M30S"), 150)

    def test_seconds_only(self):
        self.assertEqual(parse_duration("PT30S"), 30)

    def test_full(self):
        self.assertEqual(parse_duration("PT1H2M30S"), 3750)

    def test_invalid_format(self):
        self.assertIsNone(parse_duration("invalid"))

    def test_missing_pt_prefix(self):
        self.assertIsNone(parse_duration("1H2M30S"))

    def test_zero(self):
        # PT0S is technically valid
        self.assertEqual(parse_duration("PT0S"), 0)


class TestAggregate(unittest.TestCase):

    def _make_item(self, video_id="v1", title="Test", tags=None, duration="PT1M",
                   view_count="1000", like_count=None, comment_count=None):
        item = {
            "id": video_id,
            "snippet": {
                "title": title,
                "channelId": "ch1",
                "channelTitle": "Channel",
                "publishedAt": "2024-01-01T00:00:00Z",
                "thumbnails": {"default": {"url": "http://img.example.com/1.jpg"}},
            },
            "statistics": {"viewCount": view_count},
            "contentDetails": {"duration": duration},
        }
        if tags is not None:
            item["snippet"]["tags"] = tags
        if like_count is not None:
            item["statistics"]["likeCount"] = like_count
        if comment_count is not None:
            item["statistics"]["commentCount"] = comment_count
        return item

    def test_basic_aggregate(self):
        items = [self._make_item()]
        result = aggregate(items, "10", "Music", "US")
        self.assertEqual(len(result), 1)
        rec = result[0]
        self.assertEqual(rec["video_id"], "v1")
        self.assertEqual(rec["category_name"], "Music")
        self.assertEqual(rec["view_count"], 1000)
        self.assertEqual(rec["duration_seconds"], 60)
        self.assertEqual(rec["region"], "US")

    def test_missing_stats(self):
        items = [self._make_item(like_count=None, comment_count=None)]
        result = aggregate(items, "10", "Music", "US")
        self.assertIsNone(result[0]["like_count"])
        self.assertIsNone(result[0]["comment_count"])

    def test_tags_serialization(self):
        items = [self._make_item(tags=["music", "pop", "中文"])]
        result = aggregate(items, "10", "Music", "US")
        tags = json.loads(result[0]["tags"])
        self.assertEqual(tags, ["music", "pop", "中文"])

    def test_no_tags(self):
        items = [self._make_item(tags=None)]
        result = aggregate(items, "10", "Music", "US")
        self.assertEqual(result[0]["tags"], "[]")

    def test_dedup_within_batch(self):
        items = [self._make_item(video_id="v1"), self._make_item(video_id="v1")]
        result = aggregate(items, "10", "Music", "US")
        self.assertEqual(len(result), 1)

    def test_missing_fields_no_crash(self):
        """Aggregate should handle items with missing nested fields."""
        items = [{"id": "v2"}]  # Minimal item
        result = aggregate(items, "10", "Music", "US")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "")


class TestDeduplicate(unittest.TestCase):

    def test_no_dupes(self):
        records = [{"video_id": "a"}, {"video_id": "b"}]
        self.assertEqual(len(deduplicate(records)), 2)

    def test_with_dupes(self):
        records = [{"video_id": "a", "x": 1}, {"video_id": "a", "x": 2}, {"video_id": "b", "x": 3}]
        result = deduplicate(records)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["x"], 1)  # First occurrence kept


if __name__ == "__main__":
    unittest.main()
