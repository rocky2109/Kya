import os

API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# Use Heroku's ephemeral filesystem
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/tmp/downloads/")
TEMP_DIR = os.path.join(DOWNLOAD_DIR, "temp")

# Ensure directories exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
ZIP_PART_SIZE = int(1.9 * 1024**3)  # ~1.9GB (slightly under Telegram's limit)
MAX_FILE_SIZE = 2 * 1024**3  # 2GB (Telegram's limit)

# Admin user IDs (users who can access admin features)
ADMIN_USER_IDS = [int(x) for x in os.environ.get("ADMIN_USER_IDS", "884479561").split(",") if x.strip()]

# Rate limits - configurable via environment variables
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "3"))
MAX_CONCURRENT_TORRENTS = int(os.environ.get("MAX_CONCURRENT_TORRENTS", "2"))
MAX_CONCURRENT_ZIPS = int(os.environ.get("MAX_CONCURRENT_ZIPS", "2"))
MAX_TASKS_PER_USER = int(os.environ.get("MAX_TASKS_PER_USER", "5"))
MAX_DISK_USAGE_PERCENT = int(os.environ.get("MAX_DISK_USAGE_PERCENT", "90"))
CHECK_DISK_INTERVAL = int(os.environ.get("CHECK_DISK_INTERVAL", "300"))  # seconds

# Database paths - use /tmp for Heroku (ephemeral storage)
DB_DIR = os.path.join("/tmp", "torrent_bot_db")
DB_PATH = os.path.join(DB_DIR, "tasks.db")
USER_STATS_DB_PATH = os.path.join(DB_DIR, "user_stats.db")

# Ensure database directory exists
os.makedirs(DB_DIR, exist_ok=True)

# Rate limit configuration - configurable via environment variables
RATE_LIMIT_CONFIG = {
    "hourly_tasks": int(os.environ.get("HOURLY_TASKS_LIMIT", "10")),
    "daily_tasks": int(os.environ.get("DAILY_TASKS_LIMIT", "30")),
    "daily_data_limit": int(os.environ.get("DAILY_DATA_LIMIT_GB", "20")) * 1024**3,  # Convert GB to bytes
    "weekly_data_limit": int(os.environ.get("WEEKLY_DATA_LIMIT_GB", "100")) * 1024**3,  # Convert GB to bytes
    "cooldown_minutes": int(os.environ.get("COOLDOWN_MINUTES", "5"))
}

# Session path - use /tmp for Heroku
SESSION_DIR = os.path.join("/tmp", "sessions")
SESSION_PATH = os.path.join(SESSION_DIR, "Zakulika_AIO_Downloader")

# Ensure session directory exists
os.makedirs(SESSION_DIR, exist_ok=True)

# Cookies file path (for YouTube, etc.) - use /tmp for Heroku
COOKIES_FILE = os.path.join("/tmp", "youtube_cookies.txt")
