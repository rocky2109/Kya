import os
import re
import time
import logging
import asyncio
import shutil
import psutil
import gc
import libtorrent as lt # Keep for version check maybe?
from telethon import events, Button
from typing import Optional

# --- Configuration and Core Components ---
import config
from enums import TaskType, TaskStatus
from models import Task
from task_manager import MemoryOnlyTaskManager # Using in-memory version
from user_stats import InMemoryUserStats # Using in-memory version
from bot_client import client, video_downloader # Import initialized client and downloader
from video_downloader import VideoSource # Import VideoSource enum

# --- Utility Functions ---
from utils.formatting import format_size, format_time
from utils.debouncer import message_debouncer
from utils.telegram_helpers import delete_temp_messages # Import specific helpers
from utils.message_tracker import message_tracker # Import our new message tracker
from task_manager import StalledDownloadCleaner # Import StalledDownloadCleaner

# --- Download Handlers ---
from download_handlers.direct_handler import download_with_pysmartdl
from download_handlers.torrent_handler import handle_torrent, handle_magnet, LT_VERSION # Import LT_VERSION if needed
from download_handlers.video_handler import handle_video_queue, download_video_with_quality
from download_handlers.playlist_handler import handle_playlist_queue, download_playlist_with_quality

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Initialize Managers and Cleaners ---
# Using in-memory versions as defined in their respective modules
task_manager = MemoryOnlyTaskManager()
user_stats_manager = InMemoryUserStats()
stalled_download_cleaner = StalledDownloadCleaner(config.DOWNLOAD_DIR)

# --- Helper Functions ---

def get_libtorrent_version_str():
    """Get libtorrent version as a string"""
    try:
        return lt.version
    except Exception:
        return "N/A"

# --- System Monitoring and Cleanup Tasks ---

async def check_disk_space():
    """Monitor disk space and log warnings if low."""
    # This version just logs warnings, doesn't pause queues.
    # Pausing queues based on disk space can be complex and might need
    # coordination with the TaskManager or semaphore adjustments.
    while True:
        try:
            usage = psutil.disk_usage(config.DOWNLOAD_DIR)
            percent_used = usage.percent
            logger.info(f"Disk Usage ({config.DOWNLOAD_DIR}): {percent_used:.1f}% used ({format_size(usage.free)} free)")
            if percent_used > config.MAX_DISK_USAGE_PERCENT:
                logger.critical(f"CRITICAL: Disk usage ({percent_used:.1f}%) exceeds threshold ({config.MAX_DISK_USAGE_PERCENT}%). Manual cleanup may be required.")
                # Add actions here if needed (e.g., notify admin, pause *new* tasks)
            elif percent_used > config.MAX_DISK_USAGE_PERCENT - 5:
                 logger.warning(f"WARNING: Disk usage ({percent_used:.1f}%) is high.")

        except FileNotFoundError:
            logger.error(f"Download directory {config.DOWNLOAD_DIR} not found for disk space check.")
        except Exception as e:
            logger.error(f"Error checking disk space: {e}")

        await asyncio.sleep(config.CHECK_DISK_INTERVAL) # Check periodically

async def periodic_task_cleanup():
    """Periodically clean old tasks from the task manager."""
    while True:
        logger.info("Running periodic task cleanup...")
        try:
            task_manager.clean_old_tasks(older_than_days=7) # Clean tasks older than 7 days
        except Exception as e:
            logger.error(f"Error during periodic task cleanup: {e}")
        await asyncio.sleep(24 * 60 * 60) # Run once a day

async def periodic_stats_cleanup():
    """Periodically clean old rate limit entries from user stats."""
    while True:
        logger.info("Running periodic user stats cleanup...")
        try:
            user_stats_manager._clean_old_rate_limits(save_after_clean=True) # Clean and save
        except Exception as e:
            logger.error(f"Error during periodic stats cleanup: {e}")
        await asyncio.sleep(24 * 60 * 60) # Run once a day


async def run_system_cleanup(older_than_hours=24):
    """Perform a cleanup of temporary files and potentially old downloads."""
    logger.info(f"Starting system cleanup (files older than {older_than_hours} hours)...")
    now = time.time()
    cutoff = now - (older_than_hours * 60 * 60)
    cleaned_count = 0
    cleaned_size = 0

    # Clean TEMP_DIR
    temp_dir = config.TEMP_DIR
    if os.path.exists(temp_dir):
        try:
            for item_name in os.listdir(temp_dir):
                item_path = os.path.join(temp_dir, item_name)
                try:
                    item_mtime = os.path.getmtime(item_path)
                    if item_mtime < cutoff:
                        item_size = 0
                        if os.path.isfile(item_path):
                            item_size = os.path.getsize(item_path)
                            os.remove(item_path)
                            cleaned_size += item_size
                        elif os.path.isdir(item_path):
                            try:
                                # Get directory size before removal
                                for dirpath, _, filenames in os.walk(item_path):
                                    for filename in filenames:
                                        try:
                                            item_size += os.path.getsize(os.path.join(dirpath, filename))
                                        except (OSError, IOError):
                                            continue
                            except Exception as size_e:
                                logger.warning(f"Error calculating size for directory {item_path}: {size_e}")
                            shutil.rmtree(item_path)
                            cleaned_size += item_size
                        logger.info(f"Cleaned old temp item: {item_path} (Size: {format_size(item_size)})")
                        cleaned_count += 1
                except Exception as e:
                    logger.warning(f"Failed to process/clean temp item {item_path}: {e}")
        except Exception as e:
            logger.error(f"Error listing or cleaning temp directory {temp_dir}: {e}")

    logger.info(f"System cleanup finished. Removed {cleaned_count} old temp items (total size: {format_size(cleaned_size)})")


async def periodic_system_cleanup():
    """Run system cleanup periodically."""
    while True:
        await asyncio.sleep(6 * 60 * 60) # Run every 6 hours
        try:
            await run_system_cleanup(older_than_hours=24) # Clean temp items older than 24 hours
        except Exception as e:
            logger.error(f"Error during periodic system cleanup: {e}")


async def memory_monitor():
    """Monitor memory usage and log warnings."""
    threshold_percent = 85.0  # Log warning above 85% memory usage
    gc_threshold = 75.0  # Run garbage collection above 75%
    while True:
        try:
            mem = psutil.virtual_memory()
            usage_percent = mem.percent
            
            if usage_percent > threshold_percent:
                logger.warning(f"High memory usage detected: {usage_percent:.1f}% "
                             f"(Used: {format_size(mem.used)}, Available: {format_size(mem.available)})")
            
            # Run garbage collection if memory usage is above gc_threshold
            if usage_percent > gc_threshold:
                import gc
                logger.info(f"Memory usage {usage_percent:.1f}% above {gc_threshold}%. Running garbage collection...")
                collected = gc.collect()
                logger.info(f"Garbage collection finished. Collected {collected} objects.")

        except Exception as e:
            logger.error(f"Error monitoring memory: {e}")
        
        await asyncio.sleep(300)  # Check every 5 minutes


# --- Queue Handlers (Wrappers) ---
# These wrappers pass the necessary dependencies to the actual handlers

async def handle_direct_download_queue_wrapper():
    while True:
        task = await task_manager.download_queue.get()
        logger.info(f"Dequeued direct download task {task.id} for user {task.chat_id}")
        if task.status == TaskStatus.CANCELLED:
            logger.info(f"Skipping cancelled direct download task {task.id}")
            task_manager.download_queue.task_done()
            continue

        task_done_called = False
        progress_msg = None
        try:
             # Check rate limits before acquiring semaphore
            can_proceed, reason = user_stats_manager.check_rate_limit(task.chat_id, task.type.name)
            if not can_proceed:
                logger.warning(f"Rate limit exceeded for user {task.chat_id}: {reason}")
                await client.send_message(task.chat_id, f"⚠️ Task {task.id} delayed: {reason}")
                task.update_status(TaskStatus.FAILED, f"Rate limit exceeded: {reason}")
                task_manager._save_task(task)
                task_manager.download_queue.task_done()
                task_done_called = True
                continue

            # Apply timeout only to semaphore acquisition, not the entire task execution
            semaphore_acquired = False
            try:
                await asyncio.wait_for(task_manager.download_semaphore.acquire(), timeout=300)  # 5 minute timeout for semaphore acquisition only
                semaphore_acquired = True
            except asyncio.TimeoutError:
                logger.error(f"Task {task.id}: Timeout waiting for download semaphore")
                try:
                    await client.send_message(task.chat_id, f"❌ Task {task.id} failed: Timed out waiting for available download slot")
                except Exception:
                    pass
                task.update_status(TaskStatus.FAILED, "Semaphore acquisition timeout")
                task_manager._save_task(task)
                task_manager.download_queue.task_done()
                task_done_called = True
                continue
                
            try:
                if semaphore_acquired:
                    if task.status == TaskStatus.CANCELLED:  # Re-check after acquiring
                        logger.info(f"Skipping cancelled direct download task {task.id} after semaphore.")
                        task_manager.download_semaphore.release()
                        task_manager.download_queue.task_done()
                        task_done_called = True
                        continue

                    logger.info(f"Starting direct download task {task.id}")
                    task.update_status(TaskStatus.RUNNING)
                    task_manager._save_task(task)

                    # Send the initial message and store its ID
                    try:
                        progress_msg = await client.send_message(
                            task.chat_id,
                            f"⏳ Task {task.id}: Starting direct download..."
                        )
                        task_manager.update_task(task.id, message_id=progress_msg.id)
                        task.add_temp_message(progress_msg.id)
                    except Exception as e:
                        logger.error(f"Task {task.id}: Failed to send initial status message: {e}")
                        task.update_status(TaskStatus.FAILED, "Failed to send status message")
                        task_manager._save_task(task)
                        task_manager.download_semaphore.release()
                        task_manager.download_queue.task_done()
                        task_done_called = True
                        continue

                    try:
                        # Call the actual handler and pass user_stats_manager with no timeout constraint
                        await download_with_pysmartdl(task_manager, client, url=task.data, chat_id=task.chat_id, task=task, user_stats_manager=user_stats_manager)
                        # Status (COMPLETED/FAILED) is set within download_with_pysmartdl
                    finally:
                        # Always release the semaphore, even if an exception occurs
                        task_manager.download_semaphore.release()

            except Exception as e:
                # Handle any other exceptions that might occur during task execution
                logger.error(f"Error during direct download task {task.id} execution: {e}", exc_info=True)
                if task.status not in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    task.update_status(TaskStatus.FAILED, str(e))
                    task_manager._save_task(task)
                try:
                    error_text = f"❌ Task {task.id} failed: {str(e)[:200]}"
                    if progress_msg:
                         await client.edit_message(task.chat_id, progress_msg.id, error_text)
                    else:
                         await client.send_message(task.chat_id, error_text)
                except Exception:
                    pass

        except Exception as e:
            # Handle exceptions outside the semaphore acquisition block
            logger.error(f"Error processing direct download task {task.id}: {e}", exc_info=True)
            if task.status not in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                 task.update_status(TaskStatus.FAILED, str(e))
                 task_manager._save_task(task)
            try:
                if not progress_msg:
                    await client.send_message(task.chat_id, f"❌ Task {task.id} failed: {str(e)[:200]}")
            except Exception: 
                pass
        finally:
            if not task_done_called:
                 task_manager.download_queue.task_done()


async def handle_torrent_queue_wrapper():
    """Wrapper for torrent queue handler that passes required dependencies"""
    while True:
        task = await task_manager.torrent_queue.get()
        logger.info(f"Dequeued torrent task {task.id} for user {task.chat_id}")
        if task.status == TaskStatus.CANCELLED:
            logger.info(f"Skipping cancelled torrent task {task.id}")
            task_manager.torrent_queue.task_done()
            continue

        task_done_called = False
        try:
            can_proceed, reason = user_stats_manager.check_rate_limit(task.chat_id, task.type.name)
            if not can_proceed:
                logger.warning(f"Rate limit exceeded for user {task.chat_id}: {reason}")
                await client.send_message(task.chat_id, f"⚠️ Task {task.id} delayed: {reason}")
                task.update_status(TaskStatus.FAILED, f"Rate limit exceeded: {reason}")
                task_manager._save_task(task)
                task_manager.torrent_queue.task_done()
                task_done_called = True
                continue

            try:
                # Add timeout to semaphore acquisition
                await asyncio.wait_for(task_manager.torrent_semaphore.acquire(), timeout=300)  # 5 minute timeout
                try:
                    if task.status == TaskStatus.CANCELLED:
                        logger.info(f"Skipping cancelled torrent task {task.id} after semaphore.")
                        task_manager.torrent_queue.task_done()
                        task_done_called = True
                        continue

                    logger.info(f"Starting torrent task {task.id}")
                    task.update_status(TaskStatus.RUNNING)
                    task_manager._save_task(task)

                    # Call the actual handler (task.data is the .torrent file path)
                    await handle_torrent(task_manager, client, user_stats_manager, task.data, task.chat_id, task)
                    # Status is set within handle_torrent
                finally:
                    task_manager.torrent_semaphore.release()

            except asyncio.TimeoutError:
                logger.error(f"Task {task.id}: Timeout waiting for torrent semaphore")
                await client.send_message(task.chat_id, f"❌ Task {task.id} failed: Timed out waiting for available download slot")
                task.update_status(TaskStatus.FAILED, "Semaphore acquisition timeout")
                task_manager._save_task(task)
                task_manager.torrent_queue.task_done()
                task_done_called = True
                continue

        except Exception as e:
            logger.error(f"Error processing torrent task {task.id}: {e}", exc_info=True)
            # handle_torrent should set FAILED status internally, but double-check
            if task.status not in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                task.update_status(TaskStatus.FAILED, str(e))
                task_manager._save_task(task)
            # Error message is usually sent by handle_torrent

        finally:
            if not task_done_called:
                task_manager.torrent_queue.task_done()


async def handle_magnet_queue_wrapper():
    while True:
        task = await task_manager.magnet_queue.get()
        logger.info(f"Dequeued magnet task {task.id} for user {task.chat_id}")
        if task.status == TaskStatus.CANCELLED:
            logger.info(f"Skipping cancelled magnet task {task.id}")
            task_manager.magnet_queue.task_done()
            continue

        task_done_called = False
        progress_msg = None
        try:
            can_proceed, reason = user_stats_manager.check_rate_limit(task.chat_id, task.type.name)
            if not can_proceed:
                logger.warning(f"Rate limit exceeded for user {task.chat_id}: {reason}")
                await client.send_message(task.chat_id, f"⚠️ Task {task.id} delayed: {reason}")
                task.update_status(TaskStatus.FAILED, f"Rate limit exceeded: {reason}")
                task_manager._save_task(task)
                task_manager.magnet_queue.task_done()
                task_done_called = True
                continue

            try:
                # Add timeout to semaphore acquisition
                await asyncio.wait_for(task_manager.torrent_semaphore.acquire(), timeout=300)  # 5 minute timeout
                try:
                    if task.status == TaskStatus.CANCELLED:
                        logger.info(f"Skipping cancelled magnet task {task.id} after semaphore.")
                        task_manager.magnet_queue.task_done()
                        task_done_called = True
                        continue

                    logger.info(f"Starting magnet task {task.id}")
                    task.update_status(TaskStatus.RUNNING)
                    task_manager._save_task(task) # Save running status

                    # Send the initial message and store its ID
                    try:
                        progress_msg = await client.send_message(
                            task.chat_id,
                            f"🧲 Task {task.id}: Processing magnet link..."
                        )
                        task_manager.update_task(task.id, message_id=progress_msg.id)
                        task.add_temp_message(progress_msg.id) # For cleanup on failure
                    except Exception as e:
                        logger.error(f"Task {task.id}: Failed to send initial status message: {e}")
                        task.update_status(TaskStatus.FAILED, "Failed to send status message")
                        task_manager._save_task(task)
                        task_manager.magnet_queue.task_done()
                        task_done_called = True
                        continue # Skip to next task

                    # Call the actual handler (task.data is the magnet link)
                    await handle_magnet(task_manager, client, user_stats_manager, task.data, task.chat_id, task)
                    # Status is set within handle_magnet
                finally:
                    task_manager.torrent_semaphore.release()

            except asyncio.TimeoutError:
                logger.error(f"Task {task.id}: Timeout waiting for magnet semaphore")
                # Try sending a message if possible
                try:
                    await client.send_message(task.chat_id, f"❌ Task {task.id} failed: Timed out waiting for available download slot")
                except Exception: pass
                task.update_status(TaskStatus.FAILED, "Semaphore acquisition timeout")
                task_manager._save_task(task)
                task_manager.magnet_queue.task_done()
                task_done_called = True
                continue

        except Exception as e:
            logger.error(f"Error processing magnet task {task.id}: {e}", exc_info=True)
            if task.status not in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                task.update_status(TaskStatus.FAILED, str(e))
                task_manager._save_task(task)
            try:
                # Try editing the existing message if it exists, otherwise send new
                error_text = f"❌ Task {task.id} failed: {str(e)[:200]}"
                if progress_msg:
                     await client.edit_message(task.chat_id, progress_msg.id, error_text)
                else:
                     await client.send_message(task.chat_id, error_text)
            except Exception:
                pass
        finally:
            if not task_done_called:
                task_manager.magnet_queue.task_done()

# Wrapper for video queue
async def handle_video_queue_wrapper():
    """Wrapper for video queue handler that passes required dependencies"""
    await handle_video_queue(task_manager, client, video_downloader, user_stats_manager)

# Wrapper for playlist queue
async def handle_playlist_queue_wrapper():
    """Wrapper for playlist queue handler that passes required dependencies"""
    await handle_playlist_queue(task_manager, client, video_downloader, user_stats_manager)


# --- Telegram Event Handlers ---

@client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.respond(
        "👋 Welcome to Zakulika AIO Downloader!\n\n"
        "I can download files from direct links, torrents, and magnet links.\n\n"
        "📤 To download a file, simply:\n"
        "• Send me a direct download link\n"
        "• Send me a magnet link\n"
        "• Upload a torrent file\n"
        "• Send me a video URL (YouTube, etc.)\n\n"
        "💾 I'll download and send the file(s) to you.\n"
        "📋 Commands:\n"
        "• /help - Show detailed help\n"
        "• /tasks - View your active tasks\n"
        "• /status - Check your usage stats\n"
        "• /cancel <task_id> - Cancel a task\n\n"
        "⚡ Bot by @ZakulikaCompressor_bot"
    )

@client.on(events.NewMessage(pattern='/help'))
async def help_handler(event):
    user_id = event.sender_id
    
    help_text = (
        "📚 **Zakulika AIO Downloader Bot Help**\n\n"
        "🤖 **About:**\n"
        "This bot downloads files from various sources and sends them to Telegram.\n\n"
        "✨ **Features:**\n"
        "• Direct download links\n"
        "• Magnet links & Torrent files\n"
        "• Video/Playlist URLs (YouTube, etc.)\n"
        "• Automatic splitting/compression for large files\n"
        "• Progress tracking & Task queue\n\n"
        "📝 **How to Use:**\n"
        "1️⃣ **Direct Links:** Send the URL.\n"
        "2️⃣ **Torrents:** Upload a `.torrent` file.\n"
        "3️⃣ **Magnet Links:** Send the `magnet:` link.\n"
        "4️⃣ **Videos/Playlists:** Send the URL (e.g., YouTube).\n\n"
        "⚙️ **Commands:**\n"
        "• `/start` - Show welcome message.\n"
        "• `/help` - Show this help message.\n"
        "• `/tasks` - List your active and recent tasks.\n"
        "• `/status` - Show bot status and your usage.\n"
        "• `/cancel <task_id>` - Cancel a queued task.\n"
        # Add video/playlist commands if needed, or rely on URL detection
        # "• `/video <url>` - Download a single video.\n"
        # "• `/playlist <url>` - Download a playlist.\n"
    )
    
    # Add admin commands for admin users
    if user_id in config.ADMIN_USER_IDS:
        admin_help = (
            "\n🔧 **Admin Commands:**\n"
            "• `/delete_all_tasks` - Delete all tasks for all users (with confirmation).\n"
        )
        help_text += admin_help
    
    await event.respond(help_text, parse_mode='markdown')


@client.on(events.NewMessage(pattern='/tasks'))
async def list_tasks_handler(event):
    user_id = event.sender_id
    user_tasks = task_manager.get_user_tasks(user_id)
    if not user_tasks:
        await event.respond("You have no active or recent tasks.")
        return

    response = "**Your Tasks:**\n\n"
    active_tasks = []
    finished_tasks = []

    for task in sorted(user_tasks, key=lambda t: t.created_at, reverse=True):
        status_emoji = {
            TaskStatus.QUEUED: "⏳", TaskStatus.RUNNING: "⚙️",
            TaskStatus.COMPLETED: "✅", TaskStatus.FAILED: "❌",
            TaskStatus.CANCELLED: "🚫"
        }.get(task.status, "❓")

        # Shorten data for display
        data_display = task.data
        if task.type in [TaskType.DIRECT_DOWNLOAD, TaskType.MAGNET, TaskType.VIDEO_DOWNLOAD, TaskType.PLAYLIST_DOWNLOAD] and len(task.data) > 50:
             data_display = task.data[:47] + "..."
        elif task.type == TaskType.TORRENT:
             data_display = os.path.basename(task.data)

        task_line = (
            f"{status_emoji} **ID:** `{task.id}` ({task.type.name})\n"
            f"   **Data:** `{data_display}`\n"
            f"   **Status:** {task.status.name}"
        )
        if task.status == TaskStatus.RUNNING:
            task_line += f" ({task.progress:.1%})"
        elif task.status == TaskStatus.FAILED:
             task_line += f" ({task.error[:50]}{'...' if task.error and len(task.error)>50 else ''})" # Show short error

        if task.status in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
             active_tasks.append(task_line)
        else:
             finished_tasks.append(task_line)

    if active_tasks:
         response += "**Active:**\n" + "\n".join(active_tasks) + "\n\n"
    if finished_tasks:
         response += "**Finished (Recent):**\n" + "\n".join(finished_tasks[:5]) # Show last 5 finished

    await event.respond(response, parse_mode='markdown')


@client.on(events.NewMessage(pattern=r'/cancel(?: (\S+))?'))
async def cancel_task_handler(event):
    user_id = event.sender_id
    task_id_match = event.pattern_match.group(1)

    if not task_id_match:
        await event.respond("Please provide the Task ID to cancel. Usage: `/cancel <task_id>`")
        return

    task_id = task_id_match.strip()
    task = task_manager.get_task(task_id)

    if not task:
        await event.respond(f"Task ID `{task_id}` not found.")
        return

    if task.chat_id != user_id and user_id not in config.ADMIN_USER_IDS:
        await event.respond("You can only cancel your own tasks.")
        return

    if task.status == TaskStatus.QUEUED:
        if task_manager.cancel_task(task_id):
            await event.respond(f"✅ Task `{task_id}` cancelled successfully.")
        else:
            # Should not happen if status is QUEUED, but handle anyway
            await event.respond(f"⚠️ Could not cancel task `{task_id}` (Status: {task.status.name}).")
    elif task.status == TaskStatus.RUNNING:
         # Mark for cancellation, the handler needs to check the status
         task.update_status(TaskStatus.CANCELLED)
         task_manager._save_task(task) # Ensure status is saved
         await event.respond(f"⚠️ Task `{task.id}` is running. Requested cancellation. It might take a moment to stop.")
    else:
        await event.respond(f"Task `{task.id}` cannot be cancelled (Status: {task.status.name}).")


@client.on(events.NewMessage(pattern='/status'))
async def status_command(event):
    user_id = event.sender_id

    # System Stats (Admin only)
    if user_id in config.ADMIN_USER_IDS:
        try:
            disk = psutil.disk_usage(config.DOWNLOAD_DIR)
            mem = psutil.virtual_memory()
            active_tasks = sum(1 for t in task_manager.tasks.values() if t.status in [TaskStatus.RUNNING, TaskStatus.QUEUED])
            sys_stats = user_stats_manager.get_system_stats()

            status_text = (
                f"**⚙️ Bot Status (Admin)**\n\n"
                f"**Disk ({config.DOWNLOAD_DIR}):**\n"
                f"  Used: {format_size(disk.used)} / {format_size(disk.total)} ({disk.percent}%)\n"
                f"  Free: {format_size(disk.free)}\n"
                f"**Memory:**\n"
                f"  Used: {format_size(mem.used)} / {format_size(mem.total)} ({mem.percent}%)\n"
                f"**Tasks:**\n"
                f"  Active/Queued: {active_tasks}\n"
                f"  Total Managed: {len(task_manager.tasks)}\n"
                f"**System Usage:**\n"
                f"  Total Users: {sys_stats['total_users']}\n"
                f"  Total Downloaded: {format_size(sys_stats['total_downloaded'])}\n"
                f"  Tasks Completed: {sys_stats['total_completed']}\n"
                f"  Tasks Failed: {sys_stats['total_failed']}\n"
                f"  Tasks (Last 24h): {sys_stats['recent_tasks_24h']}\n"
                f"**Libtorrent:** {get_libtorrent_version_str()}\n"
            )
            # Add top users if available
            if sys_stats['top_users_by_download']:
                 status_text += "\n**Top Users (Download):**\n"
                 for uid, d_size in sys_stats['top_users_by_download']:
                      status_text += f"  - `{uid}`: {format_size(d_size)}\n"

        except Exception as e:
            logger.error(f"Error getting admin status: {e}")
            status_text = f"❌ Error retrieving admin status: {e}"
        await event.respond(status_text, parse_mode='markdown')

    # User Stats (Always show)
    try:
        user_s = user_stats_manager.get_user_stats(user_id)
        rate_limits = user_stats_manager.get_rate_limit_status(user_id)

        user_status_text = (
            f"\n**📊 Your Stats:**\n"
            f"  Downloaded: {format_size(user_s.get('total_downloaded', 0))}\n"
            # f"  Uploaded: {format_size(user_s.get('total_uploaded', 0))}\n" # Upload stats might be less relevant
            f"  Tasks Completed: {user_s.get('tasks_completed', 0)}\n"
            f"  Tasks Failed: {user_s.get('tasks_failed', 0)}\n\n"
            f"**📈 Rate Limits:**\n"
            f"  Hourly Tasks: {rate_limits['hourly_tasks']['used']}/{rate_limits['hourly_tasks']['limit']}\n"
            f"  Daily Tasks: {rate_limits['daily_tasks']['used']}/{rate_limits['daily_tasks']['limit']}\n"
            f"  Daily Data: {format_size(rate_limits['daily_data']['used'])} / {format_size(rate_limits['daily_data']['limit'])}\n"
            f"  Weekly Data: {format_size(rate_limits['weekly_data']['used'])} / {format_size(rate_limits['weekly_data']['limit'])}\n"
        )
        if user_id in config.ADMIN_USER_IDS:
             status_text += user_status_text # Append user stats for admin
        else:
             status_text = user_status_text # Show only user stats for regular users

    except Exception as e:
        logger.error(f"Error getting user status for {user_id}: {e}")
        user_status_text = f"\n❌ Error retrieving your stats: {e}"
        if user_id in config.ADMIN_USER_IDS:
             status_text += user_status_text
        else:
             status_text = user_status_text

    # Send the combined or user-only status
    if user_id not in config.ADMIN_USER_IDS: # Avoid sending twice for admin
         await event.respond(status_text, parse_mode='markdown')


@client.on(events.NewMessage(pattern='/delete_all_tasks'))
async def delete_all_tasks_handler(event):
    """Admin command to delete all tasks for all users"""
    user_id = event.sender_id
    
    if user_id not in config.ADMIN_USER_IDS:
        await event.respond("❌ This command is restricted to administrators only.")
        return
    
    # Confirmation prompt
    confirmation_msg = await event.respond(
        "⚠️ **WARNING: This will delete ALL tasks for ALL users!**\n\n"
        "This includes:\n"
        "• All queued tasks\n"
        "• All running tasks\n"
        "• All task history\n"
        "• All downloaded files\n\n"
        "Reply with `CONFIRM DELETE` to proceed or `CANCEL` to abort.\n\n"
        "*This action cannot be undone.*",
        parse_mode='markdown'
    )
    
    try:
        # Wait for user's reply
        def check_reply(event):
            return (event.sender_id == user_id and 
                   event.is_reply and 
                   event.reply_to_msg_id == confirmation_msg.id)
        
        # Wait for reply with timeout
        reply_event = await client.wait_for(events.NewMessage, 
                                           condition=check_reply, 
                                           timeout=30)
        
        reply_text = reply_event.message.text.strip().upper()
        
        if reply_text == 'CONFIRM DELETE':
            # Proceed with deletion
            processing_msg = await event.respond("🔄 Processing deletion of all tasks...")
            
            success = task_manager.admin_delete_all_tasks(user_id)
            
            if success:
                await processing_msg.edit(
                    "✅ **All tasks deleted successfully!**\n\n"
                    "All user tasks, queues, and associated files have been removed.",
                    parse_mode='markdown'
                )
                logger.info(f"Admin {user_id} successfully deleted all tasks")
            else:
                await processing_msg.edit(
                    "❌ **Failed to delete all tasks.**\n\n"
                    "Please check the logs for more information."
                )
                logger.error(f"Admin {user_id} failed to delete all tasks")
        elif reply_text == 'CANCEL':
            # User cancelled
            await event.respond(
                "❌ **Operation cancelled.**\n\n"
                "No tasks were deleted.",
                parse_mode='markdown'
            )
        else:
            # Invalid response
            await event.respond(
                "❌ **Invalid response.**\n\n"
                "Please reply with `CONFIRM DELETE` or `CANCEL` exactly.",
                parse_mode='markdown'
            )
    
    except asyncio.TimeoutError:
        await event.respond(
            "⏰ **Confirmation timeout.**\n\n"
            "Operation cancelled due to no response within 30 seconds.",
            parse_mode='markdown'
        )
    except Exception as e:
        logger.error(f"Error in delete_all_tasks_handler: {e}")
        await event.respond(f"❌ An error occurred: {e}")


# --- Main Message Handler ---

@client.on(events.NewMessage)
async def message_handler(event):
    # Ignore messages from other bots, channel posts, edits
    if event.is_private and not event.message.sender.bot:
        user_id = event.sender_id
        message_text = event.text.strip() if event.text else ""
        logger.info(f"Received message from user {user_id}. Text: '{message_text[:50]}...' File: {event.file is not None}")
        
        # Debug log for admin users
        if user_id in config.ADMIN_USER_IDS:
            logger.info(f"Admin user {user_id} detected")

        # Check if user is allowed to start a task
        can_start, reason = user_stats_manager.check_rate_limit(user_id)
        if not can_start:
            await event.respond(f"⚠️ Cannot start new task: {reason}")
            return

        user_dir = os.path.join(config.DOWNLOAD_DIR, str(user_id))
        os.makedirs(user_dir, exist_ok=True)

        task_added = None
        initial_message_sent = False # Flag to track if handler sent a message

        # 1. Handle Torrent Files
        if event.file and event.file.name and event.file.name.lower().endswith('.torrent'):
            if event.file.size > 5 * 1024 * 1024: # 5MB limit for torrent files
                await event.respond("❌ Torrent file is too large (max 5MB).")
                return

            torrent_path = os.path.join(user_dir, f"{int(time.time())}_{event.file.name}")
            try:
                # Download silently
                await client.download_media(event.message, torrent_path)
                task_added = task_manager.add_task(TaskType.TORRENT, user_id, torrent_path)
                initial_message_sent = True # Assume handler will send one
            except Exception as e:
                logger.error(f"Error downloading torrent file from user {user_id}: {e}")
                await event.respond(f"❌ Error receiving torrent file: {e}") # Send error if download fails
                if os.path.exists(torrent_path): os.remove(torrent_path) # Clean up partial download
                return # Stop processing

        # 2. Handle Magnet Links
        elif message_text.startswith("magnet:?"):
            magnet_link = message_text
            task_added = task_manager.add_task(TaskType.MAGNET, user_id, magnet_link)
            initial_message_sent = True # Assume handler will send one

        # 3. Handle URLs (Direct, Video, Playlist)
        elif "http://" in message_text or "https://" in message_text:
            url_match = re.search(r'https?://\S+', message_text)
            if url_match:
                url = url_match.group(0)
                logger.info(f"Detected URL from user {user_id}: {url}")

                # Use VideoDownloader to check if it's a known video/playlist source
                is_video_or_playlist = False
                if video_downloader:
                    try:
                        source, _ = video_downloader.detect_source(url)
                        is_playlist = video_downloader.is_playlist(url) # Basic playlist check

                        if is_playlist:
                             task_added = task_manager.add_task(TaskType.PLAYLIST_DOWNLOAD, user_id, url)
                             is_video_or_playlist = True
                             initial_message_sent = True # Assume handler will send one
                        elif source != VideoSource.OTHER:
                             task_added = task_manager.add_task(TaskType.VIDEO_DOWNLOAD, user_id, url)
                             is_video_or_playlist = True
                             initial_message_sent = True # Assume handler will send one

                    except Exception as e:
                         logger.warning(f"Error during video/playlist detection for {url}: {e}. Treating as direct link.")

                # If not identified as video/playlist, treat as direct download
                if not is_video_or_playlist:
                    task_added = task_manager.add_task(TaskType.DIRECT_DOWNLOAD, user_id, url)
                    initial_message_sent = True # Assume handler will send one
            else:
                 pass
        else:
            if not message_text.startswith('/'):
                 await event.reply("Please send a valid download link, magnet link, torrent file, or video/playlist URL. Use /help for info.")


# --- Callback Query Handler (for Buttons) ---

@client.on(events.CallbackQuery())
async def callback_handler(event):
    user_id = event.sender_id
    data = event.data.decode('utf-8')
    logger.info(f"Received callback query from user {user_id}: {data}")

    # --- Cancel Button ---
    if data.startswith('cancel_'):
        task_id = data.split('_')[1]
        task = task_manager.get_task(task_id)
        if task and task.chat_id == user_id:
            if task.status in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.QUEUED]:
                task.update_status(TaskStatus.CANCELLED)
                task_manager._save_task(task) # Ensure cancellation is saved
                logger.info(f"User {user_id} cancelled task {task_id}")
                await event.edit(f"❌ Task `{task_id}` cancelled.")
                # Trigger cleanup of temp messages associated with this task
                await delete_temp_messages(client, task, task_manager)
            else:
                await event.answer("Task is already completed or failed.", alert=True)
        else:
            await event.answer("Task not found or you don't own this task.", alert=True)

    # --- Cancel Button (Quality Selection) ---
    elif data.startswith('cancelq_'):
        task_id = data.split('_')[1]
        task = task_manager.get_task(task_id)
        if task and task.chat_id == user_id:
            if task.status in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.QUEUED, TaskStatus.AWAITING_USER_INPUT]:
                task.update_status(TaskStatus.CANCELLED)
                task_manager._save_task(task)
                logger.info(f"User {user_id} cancelled task {task_id} (from quality selection)")
                await event.edit(f"❌ Task `{task_id}` cancelled.")
                await delete_temp_messages(client, task, task_manager)
            else:
                await event.answer("Task is already completed or failed.", alert=True)
        else:
            await event.answer("Task not found or you don't own this task.", alert=True)

    # --- Video Quality Selection ---
    elif data.startswith('quality_'):
        parts = data.split('_')
        if len(parts) == 3:
            _, task_id, format_id = parts
            task = task_manager.get_task(task_id)
            # Accept both RUNNING and AWAITING_USER_INPUT for quality selection
            if task and task.chat_id == user_id and task.status in [TaskStatus.RUNNING, TaskStatus.AWAITING_USER_INPUT]:
                logger.info(f"User {user_id} selected quality '{format_id}' for task {task_id}")
                # Edit the original message to show download starting
                if task.message_id:
                    try:
                        await event.edit(f"⏳ Task `{task_id}`: Starting download (Format: {format_id})...")
                    except Exception as e:
                        logger.warning(f"Task {task_id}: Failed to edit message for quality selection: {e}")
                        # Proceed anyway, download_video_with_quality will handle message updates
                else:
                    await event.answer("Starting download...") # Fallback answer

                # Schedule the download (don't await here, let it run in background)
                asyncio.create_task(download_video_with_quality(
                    task_manager, client, video_downloader, task_id, format_id, user_stats_manager
                ))
            elif task and task.status not in [TaskStatus.RUNNING, TaskStatus.AWAITING_USER_INPUT]:
                await event.answer("This task is not ready for quality selection.", alert=True)
            else:
                await event.answer("Task not found or invalid selection.", alert=True)
        else:
            await event.answer("Invalid quality selection data.", alert=True)

    # --- Playlist Download Selection ---
    elif data.startswith('playlist_best_'):
        parts = data.split('_')
        if len(parts) == 3:
            _, _, task_id = parts # format is playlist_best_<task_id>
            format_id = 'best' # Hardcoded for this button
            task = task_manager.get_task(task_id)
            if task and task.chat_id == user_id and task.status == TaskStatus.RUNNING:
                logger.info(f"User {user_id} selected download all (best) for playlist task {task_id}")
                # Edit the original message
                if task.message_id:
                    try:
                        await event.edit(f"⏳ Task `{task_id}`: Starting playlist download (Best Quality)...")
                    except Exception as e:
                        logger.warning(f"Task {task_id}: Failed to edit message for playlist selection: {e}")
                else:
                    await event.answer("Starting playlist download...")

                # Schedule the download
                asyncio.create_task(download_playlist_with_quality(
                    task_manager, client, video_downloader, task_id, format_id, user_stats_manager
                ))
            elif task and task.status != TaskStatus.RUNNING:
                 await event.answer("This task is not ready for download selection.", alert=True)
            else:
                await event.answer("Task not found or invalid selection.", alert=True)
        else:
             await event.answer("Invalid playlist selection data.", alert=True)

    else:
        await event.answer("Unknown action.")


# --- System Cleanup ---

async def cleanup_stalled_downloads():
    """Periodically clean stalled downloads using the StalledDownloadCleaner."""
    while True:
        try:
            logger.info("Running stalled download cleanup...")
            await stalled_download_cleaner.cleanup_stalled_downloads()
            
            # Additional cleanup: check for tasks that have been in PROCESSING state too long
            current_time = time.time()
            stalled_cutoff = current_time - (2 * 60 * 60)  # 2 hours
            
            stalled_count = 0
            for task_id, task in list(task_manager.tasks.items()):
                if task.status == TaskStatus.PROCESSING and task.updated_at < stalled_cutoff:
                    logger.warning(f"Task {task_id} stalled in PROCESSING state for >2h. Marking as failed.")
                    task.update_status(TaskStatus.FAILED, "Task stalled in processing state")
                    task_manager._save_task(task)
                    stalled_count += 1
                    
                    # Clean up any associated messages
                    try:
                        await delete_temp_messages(client, task, task_manager)
                    except Exception as e:
                        logger.error(f"Error cleaning up messages for stalled task {task_id}: {e}")
            
            if stalled_count > 0:
                logger.info(f"Cleaned up {stalled_count} stalled tasks")
                        
        except Exception as e:
            logger.error(f"Error during stalled download cleanup: {e}")
        await asyncio.sleep(3600)  # Run once every hour


# --- Main Execution ---

async def main():
    """Main entry point for the bot."""
    try:
        # Start the client first, before any other async operations
        await client.connect()
        await client.start(bot_token=config.BOT_TOKEN)
        if not await client.is_user_authorized():
            raise RuntimeError("Failed to authenticate with Telegram")
        logger.info("Successfully connected to Telegram")

        # Create placeholder cookies file if it doesn't exist
        try:
            if not os.path.exists(config.COOKIES_FILE):
                os.makedirs(os.path.dirname(config.COOKIES_FILE), exist_ok=True)
                with open(config.COOKIES_FILE, 'w') as f:
                    f.write("# YouTube/Instagram cookies file\n")
                    f.write("# Add your cookies here in Netscape format\n")
                logger.info(f"Created placeholder cookies file at {config.COOKIES_FILE}")
        except Exception as e:
            logger.warning(f"Failed to create cookies file: {e}")

        # Directory setup
        os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
        os.makedirs(config.TEMP_DIR, exist_ok=True)
        os.makedirs(config.SESSION_DIR, exist_ok=True)

        logger.info("Starting background tasks...")
        background_tasks = set()
        
        # Log admin users for debugging
        logger.info(f"Admin user IDs: {config.ADMIN_USER_IDS}")

        # Create all background tasks
        tasks = [
            check_disk_space(),
            memory_monitor(),
            periodic_task_cleanup(),
            periodic_stats_cleanup(),
            periodic_system_cleanup(),
            cleanup_stalled_downloads(),
            handle_direct_download_queue_wrapper(),
            handle_torrent_queue_wrapper(),
            handle_magnet_queue_wrapper()
        ]

        # Add video tasks if video_downloader is available
        if video_downloader:
            tasks.extend([
                handle_video_queue_wrapper(),
                handle_playlist_queue_wrapper()
            ])
        else:
            logger.warning("Video/Playlist queues not started as VideoDownloader failed to initialize.")

        # Start all background tasks
        for task in tasks:
            background_tasks.add(asyncio.create_task(task))

        # Start message debouncer cleanup
        await message_debouncer.start_cleanup()

        logger.info("Bot is running... Press Ctrl+C to stop.")
        
        # Run until disconnected
        await client.run_until_disconnected()

    except Exception as e:
        logger.critical(f"Bot failed with error: {e}", exc_info=True)
    finally:
        logger.info("Bot is stopping, cleaning up background tasks...")
        # Cancel all background tasks
        for task in background_tasks:
            try:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except Exception as e:
                logger.error(f"Error cancelling background task: {e}")
          # Clean up message tracker
        try:
            logger.info("Shutting down message tracker...")
            await message_tracker.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down message tracker: {e}")
            
        # Ensure client is disconnected
        try:
            await client.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting client: {e}")
        
        logger.info("Bot stopped.")


if __name__ == '__main__':
    try:
        # Use asyncio.run() to manage the event loop
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(f"Unhandled exception in main execution: {e}", exc_info=True)
