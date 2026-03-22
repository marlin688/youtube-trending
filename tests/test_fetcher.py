"""Tests for the fetcher module."""

import json
import unittest
from unittest.mock import MagicMock, patch

from httplib2 import Response

from googleapiclient.errors import HttpError

from fetcher import (
    QuotaExceededError,
    _is_quota_exceeded,
    _should_retry,
    fetch_categories,
    fetch_trending_videos,
)
from config import Config


def _make_http_error(status: int, reason: str = "other") -> HttpError:
    """Create an HttpError with a given status and error reason."""
    resp = Response({"status": status})
    content = json.dumps({
        "error": {"errors": [{"reason": reason}], "code": status}
    }).encode("utf-8")
    return HttpError(resp, content)


class TestIsQuotaExceeded(unittest.TestCase):

    def test_quota_exceeded(self):
        err = _make_http_error(403, "quotaExceeded")
        self.assertTrue(_is_quota_exceeded(err))

    def test_other_403(self):
        err = _make_http_error(403, "forbidden")
        self.assertFalse(_is_quota_exceeded(err))


class TestShouldRetry(unittest.TestCase):

    def test_quota_no_retry(self):
        err = _make_http_error(403, "quotaExceeded")
        self.assertFalse(_should_retry(err))

    def test_429_retry(self):
        err = _make_http_error(429, "rateLimitExceeded")
        self.assertTrue(_should_retry(err))

    def test_500_retry(self):
        err = _make_http_error(500, "backendError")
        self.assertTrue(_should_retry(err))

    def test_403_other_retry(self):
        err = _make_http_error(403, "forbidden")
        self.assertTrue(_should_retry(err))

    def test_404_no_retry(self):
        err = _make_http_error(404, "notFound")
        self.assertFalse(_should_retry(err))


class TestFetchTrendingVideos(unittest.TestCase):

    def _make_config(self) -> Config:
        """Create a Config without triggering __post_init__ validation."""
        cfg = object.__new__(Config)
        cfg.youtube_api_key = "fake-key"
        cfg.region_code = "US"
        cfg.categories = ""
        cfg.max_results_per_category = 50
        cfg.output_dir = "./data"
        cfg.log_dir = "./logs"
        cfg.cache_dir = "./cache"
        cfg.categories_cache_ttl_days = 7
        cfg.retention_days = 90
        cfg.display_timezone = "UTC"
        cfg.telegram_bot_token = ""
        cfg.telegram_chat_id = ""
        return cfg

    @patch("fetcher.time.sleep")
    def test_quota_exceeded_raises(self, mock_sleep):
        config = self._make_config()
        mock_youtube = MagicMock()
        mock_youtube.videos().list().execute.side_effect = _make_http_error(403, "quotaExceeded")

        with self.assertRaises(QuotaExceededError):
            fetch_trending_videos(config, "10", youtube=mock_youtube)

    @patch("fetcher.time.sleep")
    def test_retries_on_500(self, mock_sleep):
        config = self._make_config()
        mock_youtube = MagicMock()
        mock_execute = mock_youtube.videos().list().execute
        mock_execute.side_effect = [
            _make_http_error(500, "backendError"),
            _make_http_error(500, "backendError"),
            {"items": [{"id": "v1"}]},
        ]

        result = fetch_trending_videos(config, "10", youtube=mock_youtube)
        self.assertEqual(len(result), 1)
        self.assertEqual(mock_execute.call_count, 3)

    @patch("fetcher.time.sleep")
    def test_returns_empty_after_max_retries(self, mock_sleep):
        config = self._make_config()
        mock_youtube = MagicMock()
        mock_youtube.videos().list().execute.side_effect = _make_http_error(500, "backendError")

        result = fetch_trending_videos(config, "10", youtube=mock_youtube)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
