import os
import logging
from telethon.sync import TelegramClient
from telethon.sessions import MemorySession

from config import API_ID, API_HASH, BOT_TOKEN, SESSION_DIR, DOWNLOAD_DIR, COOKIES_FILE
from video_downloader import VideoDownloader

logger = logging.getLogger(__name__)

# Initialize client without connecting
client = TelegramClient(MemorySession(), API_ID, API_HASH)

# Initialize video downloader
effective_cookies_file = COOKIES_FILE
if not os.path.exists(effective_cookies_file):
    logger.warning(f"Cookies file not found at {effective_cookies_file}. Some video downloads might fail.")
    effective_cookies_file = None

try:
    video_downloader = VideoDownloader(DOWNLOAD_DIR, cookies_file=effective_cookies_file)
    logger.info("VideoDownloader initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize VideoDownloader: {e}", exc_info=True)
    video_downloader = None
    logger.warning("Video download features will be unavailable.")

