import os
import json
import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Tuple

from config import USER_STATS_DB_PATH, RATE_LIMIT_CONFIG, ADMIN_USER_IDS

logger = logging.getLogger(__name__)

class UserStats:
    """Track user statistics and enforce rate limits"""

    def __init__(self):
        # Stores general stats like total downloaded, tasks completed/failed
        self.user_stats = {}
        # Stores timestamps of tasks for rate limiting (tasks per hour/day)
        self.task_history = defaultdict(list)
        # Stores timestamps and sizes of data transfers for rate limiting (data per day/week)
        self.data_usage = defaultdict(list)
        self._load_data()

    def _save_data(self):
        """Save all stats data to JSON files with better error handling"""
        try:
            os.makedirs(os.path.dirname(USER_STATS_DB_PATH), exist_ok=True)

            # --- Save general user stats ---
            stats_temp_path = USER_STATS_DB_PATH + '.json.tmp'
            with open(stats_temp_path, 'w') as f:
                # Ensure keys are strings for JSON compatibility
                json.dump({str(k): v for k, v in self.user_stats.items()}, f, indent=4)
            os.replace(stats_temp_path, USER_STATS_DB_PATH + '.json')
            logger.debug("Saved general user stats.")

            # --- Save rate limit data (task history and data usage) ---
            rates_temp_path = USER_STATS_DB_PATH + '_rates.json.tmp'
            # Combine task history and data usage for saving
            rate_limits_data_to_save = {
                'task_history': {str(k): v for k, v in self.task_history.items()},
                'data_usage': {str(k): v for k, v in self.data_usage.items()}
            }
            with open(rates_temp_path, 'w') as f:
                json.dump(rate_limits_data_to_save, f, indent=4)
            os.replace(rates_temp_path, USER_STATS_DB_PATH + '_rates.json')
            logger.debug("Saved rate limit data (task history and data usage).")

        except Exception as e:
            logger.error(f"Error saving user stats data: {e}")

    def _load_data(self):
        """Load data from JSON files"""
        try:
            # --- Load general user stats ---
            stats_path = USER_STATS_DB_PATH + '.json'
            if os.path.exists(stats_path):
                with open(stats_path, 'r') as f:
                    loaded_stats = json.load(f)
                    # Convert keys back to int if necessary, though string keys are fine
                    self.user_stats = {int(k) if k.isdigit() else k: v for k, v in loaded_stats.items()}
                logger.info(f"Loaded general user stats for {len(self.user_stats)} users.")

            # --- Load rate limit data ---
            rates_path = USER_STATS_DB_PATH + '_rates.json'
            if os.path.exists(rates_path):
                with open(rates_path, 'r') as f:
                    loaded_rates = json.load(f)
                    # Load task history
                    task_history_loaded = loaded_rates.get('task_history', {})
                    self.task_history = defaultdict(list, {int(k): v for k, v in task_history_loaded.items()})
                    # Load data usage
                    data_usage_loaded = loaded_rates.get('data_usage', {})
                    self.data_usage = defaultdict(list, {int(k): v for k, v in data_usage_loaded.items()})
                logger.info(f"Loaded rate limit data for {len(self.task_history)} users.")

            # Clean old entries after loading
            self._clean_old_rate_limits(save_after_clean=False) # Avoid saving immediately after load

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from stats files: {e}. Starting fresh.")
            self.user_stats = {}
            self.task_history = defaultdict(list)
            self.data_usage = defaultdict(list)
        except Exception as e:
            logger.error(f"Error loading user stats data: {e}")

    def _clean_old_rate_limits(self, save_after_clean=True):
        """Remove rate limit data older than the longest period (e.g., 7 days for weekly limit)"""
        # Determine the cutoff based on the longest limit period (default to 7 days)
        longest_period_days = 7 # Assuming weekly is the longest relevant period
        cutoff_time = time.time() - (longest_period_days * 24 * 60 * 60)
        cleaned_tasks = 0
        cleaned_data = 0

        for user_id in list(self.task_history.keys()):
            original_len = len(self.task_history[user_id])
            self.task_history[user_id] = [
                entry for entry in self.task_history[user_id] if entry[0] > cutoff_time # entry is (timestamp, task_type)
            ]
            cleaned_tasks += original_len - len(self.task_history[user_id])
            if not self.task_history[user_id]: # Remove user if list becomes empty
                del self.task_history[user_id]

        for user_id in list(self.data_usage.keys()):
            original_len = len(self.data_usage[user_id])
            self.data_usage[user_id] = [
                entry for entry in self.data_usage[user_id] if entry[0] > cutoff_time # entry is (timestamp, data_size)
            ]
            cleaned_data += original_len - len(self.data_usage[user_id])
            if not self.data_usage[user_id]: # Remove user if list becomes empty
                del self.data_usage[user_id]

        if cleaned_tasks > 0 or cleaned_data > 0:
             logger.info(f"Cleaned {cleaned_tasks} old task history entries and {cleaned_data} old data usage entries.")
             if save_after_clean:
                 self._save_data()

    def update_user_stats(self, user_id, downloaded=0, uploaded=0, completed=False, failed=False):
        """Update general user statistics"""
        # Use int for user_id internally, but handle potential string keys from old saves
        user_id_key = int(user_id)

        if user_id_key not in self.user_stats:
            self.user_stats[user_id_key] = {
                'total_downloaded': 0,
                'total_uploaded': 0,
                'tasks_completed': 0,
                'tasks_failed': 0,
                'last_updated': 0
            }

        stats = self.user_stats[user_id_key]
        stats['total_downloaded'] += downloaded
        stats['total_uploaded'] += uploaded
        if completed:
            stats['tasks_completed'] += 1
        if failed:
            stats['tasks_failed'] += 1
        stats['last_updated'] = time.time()

        # No immediate save here, rely on record_completed_transfer or periodic saves
        # self._save_data()

    def record_task_for_rate_limit(self, user_id, task_type):
        """Record a task start for rate limiting (tasks per hour/day)"""
        timestamp = time.time()
        self.task_history[int(user_id)].append((timestamp, task_type))
        # No immediate save here

    def record_data_transfer_for_rate_limit(self, user_id, data_size):
        """Record data transfer size for rate limiting (data per day/week)"""
        if data_size > 0:
            timestamp = time.time()
            self.data_usage[int(user_id)].append((timestamp, data_size))
            # No immediate save here

    def record_completed_transfer(self, user_id, downloaded_size, uploaded_size=0, task_type=None, success=True):
        """Record a completed file transfer, updating stats and rate limits"""
        user_id_key = int(user_id)
        self.update_user_stats(
            user_id_key,
            downloaded=downloaded_size,
            uploaded=uploaded_size,
            completed=success,
            failed=not success
        )

        # Record data transfer for rate limiting if download occurred
        if downloaded_size > 0:
            self.record_data_transfer_for_rate_limit(user_id_key, downloaded_size)

        # Update last transfer details in general stats
        timestamp = time.time()
        if user_id_key not in self.user_stats: # Should exist after update_user_stats, but check anyway
             self.user_stats[user_id_key] = {}
        self.user_stats[user_id_key]['last_transfer_time'] = timestamp
        self.user_stats[user_id_key]['last_transfer_size'] = downloaded_size
        self.user_stats[user_id_key]['last_transfer_success'] = success

        # Save all data after recording a completed transfer
        self._save_data()

    def check_rate_limit(self, user_id, task_type=None) -> tuple[bool, Optional[str]]:
        """Check if the user is allowed to start a new task based on limits."""
        user_id_key = int(user_id)
        if user_id_key in ADMIN_USER_IDS:
            return True, None # Admins bypass limits

        current_time = time.time()
        one_hour_ago = current_time - (60 * 60)
        one_day_ago = current_time - (24 * 60 * 60)
        one_week_ago = current_time - (7 * 24 * 60 * 60)

        # --- Get relevant history (filter old entries implicitly) ---
        user_task_history = [ts for ts, _ in self.task_history.get(user_id_key, []) if ts > one_week_ago]
        user_data_usage = [(ts, size) for ts, size in self.data_usage.get(user_id_key, []) if ts > one_week_ago]

        # --- Check Task Count Limits ---
        hourly_tasks = sum(1 for ts in user_task_history if ts > one_hour_ago)
        daily_tasks = sum(1 for ts in user_task_history if ts > one_day_ago) # Already filtered for week

        if hourly_tasks >= RATE_LIMIT_CONFIG["hourly_tasks"]:
            # Find the timestamp of the oldest task within the last hour that caused the limit
            relevant_tasks = sorted([ts for ts in user_task_history if ts > one_hour_ago])
            if len(relevant_tasks) >= RATE_LIMIT_CONFIG["hourly_tasks"]:
                 limit_hit_time = relevant_tasks[0] # The first task in the window
                 next_available = limit_hit_time + (60 * 60)
                 wait_seconds = max(1, int(next_available - current_time))
                 wait_mins = (wait_seconds + 59) // 60 # Round up to nearest minute
                 return False, f"Hourly limit ({RATE_LIMIT_CONFIG['hourly_tasks']}) reached. Please try again in {wait_mins} min."
            # Fallback if logic error
            return False, f"Hourly limit ({RATE_LIMIT_CONFIG['hourly_tasks']}) reached. Please try again later."


        if daily_tasks >= RATE_LIMIT_CONFIG.get("daily_tasks", 100): # Use .get for safety
            # Find next day boundary
            now_dt = datetime.now()
            next_day_dt = (now_dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            wait_seconds = max(1, int(next_day_dt.timestamp() - current_time))
            wait_hours = (wait_seconds + 3599) // 3600 # Round up to nearest hour
            return False, f"Daily task limit ({RATE_LIMIT_CONFIG.get('daily_tasks', 100)}) reached. Please try again in {wait_hours} hr."

        # --- Check Data Transfer Limits ---
        daily_data = sum(size for ts, size in user_data_usage if ts > one_day_ago)
        weekly_data = sum(size for ts, size in user_data_usage) # Already filtered for week

        if daily_data >= RATE_LIMIT_CONFIG["daily_data_limit"]:
            now_dt = datetime.now()
            next_day_dt = (now_dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            wait_seconds = max(1, int(next_day_dt.timestamp() - current_time))
            wait_hours = (wait_seconds + 3599) // 3600
            limit_gb = RATE_LIMIT_CONFIG['daily_data_limit'] / (1024**3)
            return False, f"Daily data limit ({limit_gb:.1f} GB) reached. Please try again in {wait_hours} hr."

        if weekly_data >= RATE_LIMIT_CONFIG["weekly_data_limit"]:
            # Find next week boundary (e.g., Monday 00:00)
            now_dt = datetime.now()
            days_until_monday = (7 - now_dt.weekday()) % 7
            if days_until_monday == 0: days_until_monday = 7 # If today is Monday, wait till next Monday
            next_week_dt = (now_dt + timedelta(days=days_until_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
            wait_seconds = max(1, int(next_week_dt.timestamp() - current_time))
            wait_days = (wait_seconds + 86399) // 86400 # Round up to nearest day
            limit_gb = RATE_LIMIT_CONFIG['weekly_data_limit'] / (1024**3)
            return False, f"Weekly data limit ({limit_gb:.1f} GB) reached. Please try again in {wait_days} day(s)."

        # --- Check Cooldown Period ---
        cooldown_seconds = RATE_LIMIT_CONFIG.get("cooldown_minutes", 0) * 60
        if cooldown_seconds > 0:
            last_transfer_time = self.user_stats.get(user_id_key, {}).get('last_transfer_time', 0)
            if current_time - last_transfer_time < cooldown_seconds:
                wait_seconds = max(1, int(last_transfer_time + cooldown_seconds - current_time))
                wait_mins = (wait_seconds + 59) // 60
                return False, f"Please wait {wait_mins} min between transfers (cooldown)."

        # If all checks pass
        # Record the *intention* to start a task for task count limits immediately
        self.record_task_for_rate_limit(user_id_key, task_type or "unknown")
        # Save data after potentially adding a task history entry
        self._save_data()
        return True, None

    def get_user_stats(self, user_id):
        """Get general statistics for a user"""
        user_id_key = int(user_id)
        default_stats = {
            "total_downloaded": 0,
            "total_uploaded": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "last_updated": None,
            "last_transfer_time": None,
            "last_transfer_size": None,
            "last_transfer_success": None
        }
        return self.user_stats.get(user_id_key, default_stats)

    def get_rate_limit_status(self, user_id):
        """Get current rate limit usage and limits for a user"""
        user_id_key = int(user_id)
        current_time = time.time()
        one_hour_ago = current_time - (60 * 60)
        one_day_ago = current_time - (24 * 60 * 60)
        one_week_ago = current_time - (7 * 24 * 60 * 60)

        # Use filtered history
        user_task_history = [ts for ts, _ in self.task_history.get(user_id_key, []) if ts > one_week_ago]
        user_data_usage = [(ts, size) for ts, size in self.data_usage.get(user_id_key, []) if ts > one_week_ago]

        hourly_tasks = sum(1 for ts in user_task_history if ts > one_hour_ago)
        daily_tasks = sum(1 for ts in user_task_history if ts > one_day_ago)
        daily_data = sum(size for ts, size in user_data_usage if ts > one_day_ago)
        weekly_data = sum(size for ts, size in user_data_usage)

        return {
            "hourly_tasks": {
                "used": hourly_tasks,
                "limit": RATE_LIMIT_CONFIG["hourly_tasks"],
                "remaining": max(0, RATE_LIMIT_CONFIG["hourly_tasks"] - hourly_tasks)
            },
            "daily_tasks": {
                "used": daily_tasks,
                "limit": RATE_LIMIT_CONFIG.get("daily_tasks", 100),
                "remaining": max(0, RATE_LIMIT_CONFIG.get("daily_tasks", 100) - daily_tasks)
            },
            "daily_data": {
                "used": daily_data,
                "limit": RATE_LIMIT_CONFIG["daily_data_limit"],
                "remaining": max(0, RATE_LIMIT_CONFIG["daily_data_limit"] - daily_data)
            },
            "weekly_data": {
                "used": weekly_data,
                "limit": RATE_LIMIT_CONFIG["weekly_data_limit"],
                "remaining": max(0, RATE_LIMIT_CONFIG["weekly_data_limit"] - weekly_data)
            }
        }

    def get_system_stats(self):
        """Get system-wide statistics for admin view"""
        current_time = time.time()
        one_day_ago = current_time - (24 * 60 * 60)

        recent_tasks = 0
        for user_id, history in self.task_history.items():
            recent_tasks += sum(1 for ts, _ in history if ts > one_day_ago)

        total_downloaded = 0
        total_uploaded = 0
        total_completed = 0
        total_failed = 0
        user_downloads = {}

        for user_id, stats in self.user_stats.items():
            total_downloaded += stats.get('total_downloaded', 0)
            total_uploaded += stats.get('total_uploaded', 0)
            total_completed += stats.get('tasks_completed', 0)
            total_failed += stats.get('tasks_failed', 0)
            user_downloads[user_id] = stats.get('total_downloaded', 0)

        top_users = sorted(user_downloads.items(), key=lambda item: item[1], reverse=True)[:5]

        return {
            "total_users": len(self.user_stats),
            "total_downloaded": total_downloaded,
            "total_uploaded": total_uploaded,
            "total_completed": total_completed,
            "total_failed": total_failed,
            "recent_tasks_24h": recent_tasks,
            "top_users_by_download": top_users # List of (user_id, bytes_downloaded)
        }

# Class to override database operations with in-memory storage
class InMemoryUserStats(UserStats):
    """UserStats implementation that stores everything in memory (no database)"""

    def _save_data(self):
        """Don't save to disk in this in-memory version"""
        logger.debug("Database save skipped - using in-memory storage (InMemoryUserStats)")
        return

    def _load_data(self):
        """Don't load from disk in this in-memory version"""
        logger.info("Database load skipped - using in-memory storage (InMemoryUserStats)")
        self.user_stats = {}
        self.task_history = defaultdict(list)
        self.data_usage = defaultdict(list)
        return

    def _clean_old_rate_limits(self, save_after_clean=True):
         # Call parent's clean logic but skip the save at the end
         super()._clean_old_rate_limits(save_after_clean=False)
