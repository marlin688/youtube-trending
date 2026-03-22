"""Configuration module: loads settings from .env into a typed dataclass."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class Config:
    """Application configuration loaded from environment variables."""

    youtube_api_key: str = ""
    region_code: str = "US"
    categories: str = ""
    max_results_per_category: int = 50
    output_dir: str = "./data"
    log_dir: str = "./logs"
    cache_dir: str = "./cache"
    categories_cache_ttl_days: int = 7
    retention_days: int = 90
    display_timezone: str = "UTC"
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    custom_categories: str = "LLM:LLM|GPT|Claude|Gemini|Llama|大模型|大语言模型|ChatGPT|OpenAI;AI:artificial intelligence|AI|ChatGPT|LLM|机器学习|AI发展|AI趋势|AGI|AI安全|AI监管|人工智能发展|人工智能未来|AI revolution|future of AI|AI regulation"
    min_duration_seconds: int = 181
    max_video_age_days: int = 7
    min_view_count: int = 1000

    def __post_init__(self) -> None:
        if not self.youtube_api_key:
            print("ERROR: YOUTUBE_API_KEY is required but not set.", file=sys.stderr)
            sys.exit(1)

        # Create required directories
        for d in [
            Path(self.output_dir) / "snapshots",
            Path(self.output_dir) / "reports",
            Path(self.log_dir),
            Path(self.cache_dir),
        ]:
            d.mkdir(parents=True, exist_ok=True)


def load_config(env_path: str | None = None) -> Config:
    """Load configuration from .env file and environment variables."""
    load_dotenv(env_path or ".env")

    return Config(
        youtube_api_key=os.getenv("YOUTUBE_API_KEY", ""),
        region_code=os.getenv("REGION_CODE", "US"),
        categories=os.getenv("CATEGORIES", ""),
        max_results_per_category=int(os.getenv("MAX_RESULTS_PER_CATEGORY", "50")),
        output_dir=os.getenv("OUTPUT_DIR", "./data"),
        log_dir=os.getenv("LOG_DIR", "./logs"),
        cache_dir=os.getenv("CACHE_DIR", "./cache"),
        categories_cache_ttl_days=int(os.getenv("CATEGORIES_CACHE_TTL_DAYS", "7")),
        retention_days=int(os.getenv("RETENTION_DAYS", "90")),
        display_timezone=os.getenv("DISPLAY_TIMEZONE", "UTC"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        custom_categories=os.getenv("CUSTOM_CATEGORIES", "LLM:LLM|GPT|Claude|Gemini|Llama|大模型|大语言模型|ChatGPT|OpenAI;AI:artificial intelligence|AI|ChatGPT|LLM|机器学习|AI发展|AI趋势|AGI|AI安全|AI监管|人工智能发展|人工智能未来|AI revolution|future of AI|AI regulation"),
        min_duration_seconds=int(os.getenv("MIN_DURATION_SECONDS", "181")),
        max_video_age_days=int(os.getenv("MAX_VIDEO_AGE_DAYS", "7")),
        min_view_count=int(os.getenv("MIN_VIEW_COUNT", "1000")),
    )
