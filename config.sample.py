# Sample configuration file
# Rename this file to config.py and update with your credentials

import os

# Replace with your own Telegram API credentials
API_ID = 123456  # Get from https://my.telegram.org
API_HASH = "your_api_hash_here"  # Get from https://my.telegram.org
BOT_TOKEN = "your_bot_token_here"  # Get from BotFather

# Download directories
DOWNLOAD_DIR = "/path/to/downloads/"  # Update this path
TEMP_DIR = os.path.join(DOWNLOAD_DIR, "temp")
ZIP_PART_SIZE = int(1.9 * 1024**3)  # ~1.9GB (slightly under Telegram's limit)
MAX_FILE_SIZE = 2 * 1024**3  # 2GB (Telegram's limit)

# Admin user IDs (users who can access admin features)
ADMIN_USER_IDS = [12345678]  # Replace with your Telegram user ID

# Rate limits
MAX_CONCURRENT_DOWNLOADS = 3
MAX_CONCURRENT_TORRENTS = 2
MAX_CONCURRENT_ZIPS = 2
MAX_TASKS_PER_USER = 5
MAX_DISK_USAGE_PERCENT = 90
CHECK_DISK_INTERVAL = 300  # seconds

# Database paths
DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "torrent_bot_db")
DB_PATH = os.path.join(DB_DIR, "tasks.db")
USER_STATS_DB_PATH = os.path.join(DB_DIR, "user_stats.db")

RATE_LIMIT_CONFIG = {
    "hourly_tasks": 10,
    "daily_tasks": 30,
    "daily_data_limit": 20 * 1024**3,  # 20GB
    "weekly_data_limit": 100 * 1024**3,  # 100GB
    "cooldown_minutes": 5
}

# Session path
SESSION_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sessions")
SESSION_PATH = os.path.join(SESSION_DIR, "Zakulika_AIO_Downloader")

# Cookies file path (for YouTube, etc.)
COOKIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "youtube_cookies.txt")
