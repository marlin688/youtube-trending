# YouTube Trending

每日自动抓取 YouTube 各类别热门视频，生成 CSV 数据快照和中文 Markdown 报告。

## 功能特性

- 自动获取 YouTube 15 个可分配类别的热门视频（每类最多 50 条）
- Markdown 报告：中文类别名、中文表头、视频直链、播放量/点赞人性化显示
- CSV 快照：UTF-8 BOM 编码，兼容 Excel 直接打开
- 类别列表本地缓存，减少 API 配额消耗
- API 调用失败自动重试（指数退避），配额耗尽立即终止并保存已有数据
- 自动清理过期快照和报告
- 可选 Telegram 通知（未配置时静默跳过）
- 自动检测 `http_proxy`/`https_proxy` 环境变量，支持代理访问

## 快速开始

```bash
git clone <repo-url> && cd youtube-trending
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# 编辑 .env，填入 YOUTUBE_API_KEY
python main.py
```

> 如果在国内使用，需要配置代理：
> ```bash
> export https_proxy=http://127.0.0.1:7890
> python main.py
> ```

## 输出示例

运行后在 `data/` 目录下生成：

```
data/
├── snapshots/2026-03-22.csv   # 当日原始数据
├── reports/2026-03-22.md      # 当日中文报告
└── latest.csv                 # 最新快照副本
```

Markdown 报告效果：

| # | 标题 | 频道 | 播放量 | 点赞 | 时长 | 发布时间 | 链接 |
|---|------|------|--------|------|------|----------|------|
| 1 | Example Video | Channel | 21.5M | 1.4M | 1:13 | 11d ago | [观看](https://www.youtube.com/watch?v=xxx) |

## 配置项

所有配置通过 `.env` 文件管理：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `YOUTUBE_API_KEY` | *(必填)* | YouTube Data API v3 密钥 |
| `REGION_CODE` | `US` | 地区代码（ISO 3166-1 alpha-2） |
| `CATEGORIES` | *(空)* | 指定类别 ID（逗号分隔），留空则抓取所有可用类别 |
| `MAX_RESULTS_PER_CATEGORY` | `50` | 每个类别最大视频数（API 上限 50） |
| `OUTPUT_DIR` | `./data` | 输出目录 |
| `LOG_DIR` | `./logs` | 日志目录 |
| `CACHE_DIR` | `./cache` | 缓存目录 |
| `CATEGORIES_CACHE_TTL_DAYS` | `7` | 类别列表缓存有效期（天） |
| `RETENTION_DAYS` | `90` | 自动清理超过 N 天的文件（0 = 不清理） |
| `DISPLAY_TIMEZONE` | `UTC` | 报告中显示的时区 |
| `TELEGRAM_BOT_TOKEN` | *(空)* | Telegram Bot Token（可选） |
| `TELEGRAM_CHAT_ID` | *(空)* | Telegram Chat ID（可选） |

## 执行流程

```
1. 加载 .env 配置
2. 初始化日志（stdout + logs/run-{date}.log）
3. 获取类别列表（优先读缓存）
4. 确定目标类别
5. 逐类别拉取热门视频
   - 配额耗尽 → 立即停止，已有数据仍输出
   - 其他错误 → 重试 3 次后跳过该类别
6. 数据聚合、清洗、去重
7. 输出 CSV + Markdown 报告
8. 更新 latest.csv
9. 清理过期文件
10. 健康检查（音乐/游戏/娱乐是否有数据）
11. 发送 Telegram 通知（如已配置）
12. 日志收尾（耗时、总条数、跳过的类别）
```

## 定时运行

### cron (Linux/macOS)

```cron
0 6 * * * cd /path/to/youtube-trending && /path/to/.venv/bin/python main.py
```

### launchd (macOS)

创建 `~/Library/LaunchAgents/com.youtube-trending.plist`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.youtube-trending</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/.venv/bin/python</string>
        <string>/path/to/youtube-trending/main.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/youtube-trending</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
</dict>
</plist>
```

## 项目结构

```
youtube-trending/
├── main.py          # 入口 - 串联完整流程
├── config.py        # .env → dataclass 配置加载
├── fetcher.py       # YouTube API 调用、缓存、重试、代理
├── aggregator.py    # 数据转换、清洗、去重
├── formatter.py     # 数字/时间/时长人性化格式
├── output.py        # CSV + Markdown 报告生成
├── cleaner.py       # 过期文件清理
├── notifier.py      # Telegram 通知（可选）
├── tests/           # 单元测试（60 个用例）
├── .env.example     # 配置模板
├── requirements.txt # Python 依赖
└── README.md
```

## 依赖

- Python 3.9+
- `google-api-python-client` — YouTube Data API v3
- `python-dotenv` — 环境变量加载
- `pysocks` — 代理支持（国内使用需要）
- 其余全部使用 Python 标准库
