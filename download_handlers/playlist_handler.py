import os
import re
import time
import asyncio
import logging
import json
import shutil
from typing import Optional, Dict
from datetime import datetime, timedelta

import aiofiles.os as async_os
from asyncio import Lock

from telethon import Button

# Assuming these modules are accessible
from config import DOWNLOAD_DIR, ZIP_PART_SIZE
from enums import TaskStatus
from models import Task
from utils.formatting import format_size
from utils.telegram_helpers import send_to_telegram, upload_zip_parts, delete_temp_messages, send_tracked_message, delete_tracked_messages
from utils.compressor import stream_compress
from utils.debouncer import message_debouncer
from utils.message_tracker import message_tracker  # Import message tracker for auto-deletion

logger = logging.getLogger(__name__)

_playlist_locks: Dict[str, Lock] = {}

async def get_playlist_lock(playlist_dir: str) -> Lock:
    """Get or create a lock for a specific playlist directory."""
    if playlist_dir not in _playlist_locks:
        _playlist_locks[playlist_dir] = Lock()
    return _playlist_locks[playlist_dir]

def retry_rmtree(path, max_retries=3, delay=1):
    """Retry removing a directory tree with exponential backoff."""
    for attempt in range(max_retries):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
                continue
            raise e
    return False

class PlaylistDownloadSession:
    """Context manager for playlist download sessions to ensure proper cleanup."""
    def __init__(self, task_id: str, chat_id: int, title: str, playlist_dir: str):
        self.task_id = task_id
        self.chat_id = chat_id
        self.title = title
        self.playlist_dir = playlist_dir
        self.temp_files = set()
        self.downloaded_files = []
        self.successful_downloads = 0
        self.failed_downloads = 0
        self.total_downloaded_size = 0

    async def __aenter__(self):
        """Setup playlist directory."""
        os.makedirs(self.playlist_dir, exist_ok=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources and temporary files if download failed."""
        # Only clean up if we had an error or task failed
        if exc_type or not self.successful_downloads:
            try:
                # First try to remove individual files
                for file_path in self.temp_files:
                    if os.path.exists(file_path):
                        try:
                            # Add retry for file removal with exponential backoff
                            for attempt in range(3):
                                try:
                                    os.remove(file_path)
                                    logger.debug(f"Task {self.task_id}: Removed temp file: {file_path}")
                                    break
                                except PermissionError:
                                    if attempt < 2:
                                        await asyncio.sleep(1 * (2 ** attempt))
                                        continue
                                    raise
                        except Exception as e:
                            logger.warning(f"Task {self.task_id}: Failed to remove file {file_path}: {e}")
                        except Exception as e:
                            logger.warning(f"Task {self.task_id}: Failed to remove temp file {file_path}: {e}")
    
                # Attempt to remove the playlist directory if empty
                try:
                    if os.path.exists(self.playlist_dir) and not os.listdir(self.playlist_dir):
                        os.rmdir(self.playlist_dir)
                        logger.info(f"Task {self.task_id}: Removed empty playlist directory: {self.playlist_dir}")
                except Exception as e:
                    logger.warning(f"Task {self.task_id}: Failed to remove playlist directory: {e}")
            except Exception as e:
                logger.warning(f"Task {self.task_id}: Error during playlist session cleanup: {e}")

    def add_temp_file(self, file_path: str):
        """Add a file to be cleaned up if download fails."""
        self.temp_files.add(file_path)

    def add_downloaded_file(self, file_path: str, file_size: int):
        """Record a successfully downloaded file."""
        self.downloaded_files.append(file_path)
        self.successful_downloads += 1
        self.total_downloaded_size += file_size

    def record_failed_download(self):
        """Record a failed download attempt."""
        self.failed_downloads += 1

async def update_playlist_progress(client, chat_id, message_id, text, min_interval=5.0):
    """Update playlist progress message with rate limiting."""
    try:
        return await message_debouncer.update(client, chat_id, message_id, text, min_interval=min_interval)
    except Exception as e:
        logger.warning(f"Failed to update playlist progress: {e}")
        return False

async def handle_playlist_queue(task_manager, client, video_downloader, user_stats_manager):
    """Process playlist download tasks from the queue."""
    while True:
        task = await task_manager.playlist_queue.get()
        logger.info(f"Dequeued playlist task {task.id} for user {task.chat_id}")

        if task.status == TaskStatus.CANCELLED:
            logger.info(f"Skipping cancelled playlist task {task.id}")
            task_manager.playlist_queue.task_done()
            continue

        task_done_called = False
        progress_msg = None

        try:
            # Check rate limits before acquiring semaphore
            can_proceed, reason = user_stats_manager.check_rate_limit(task.chat_id, task.type.name)
            if not can_proceed:
                logger.warning(f"Rate limit exceeded for user {task.chat_id}: {reason}")
                task.update_status(TaskStatus.FAILED, f"Rate limit exceeded: {reason}")
                task_manager._save_task(task)
                task_manager.playlist_queue.task_done()
                task_done_called = True
                continue
                
            try:
                # Add timeout to semaphore acquisition
                await asyncio.wait_for(task_manager.video_semaphore.acquire(), timeout=300)  # 5 minute timeout
                try:
                    # Re-check status after acquiring semaphore
                    if task.status == TaskStatus.CANCELLED:
                        logger.info(f"Skipping cancelled playlist task {task.id} after acquiring semaphore.")
                        task_manager.playlist_queue.task_done()
                        task_done_called = True
                        continue

                    logger.info(f"Starting playlist task {task.id} for user {task.chat_id}")
                    task.update_status(TaskStatus.RUNNING)
                    task_manager._save_task(task)

                    url = task.data # Assuming data is just the URL initially
                    format_id = 'best' # Default format                        # Create initial progress message using tracked message
                    progress_msg_text = f"🔄 Task {task.id}: Analyzing playlist...\n" \
                                      f"URL: {url[:60]}{'...' if len(url) > 60 else ''}"
                    progress_msg = await send_tracked_message(
                        client, 
                        task.chat_id,
                        progress_msg_text,
                        task,
                        auto_delete=300  # Auto-delete after 5 minutes if no action taken
                    )
                    task_manager.update_task(task.id, message_id=progress_msg.id)

                    # Get playlist info
                    logger.debug(f"Task {task.id}: Fetching playlist info for {url}")
                    playlist_info = await video_downloader.get_video_info(url)
                    
                    if 'error' in playlist_info:
                        error_msg = playlist_info['error']
                        logger.error(f"Task {task.id}: Error fetching playlist info: {error_msg}")
                        await client.edit_message(task.chat_id, progress_msg.id, f"❌ Task {task.id} Error: {error_msg}")
                        # Schedule this error message for deletion after 60 seconds
                        await message_tracker.schedule_deletion(client, task, progress_msg.id, 60)
                        task.update_status(TaskStatus.FAILED, error_msg)
                        task_manager._save_task(task)
                        task_manager.playlist_queue.task_done()
                        task_done_called = True
                        continue
                        
                    # Verify this is actually a playlist
                    if not playlist_info.get('is_playlist', False):
                        error_msg = "Not a valid playlist URL. Please send a single video URL instead."
                        logger.warning(f"Task {task.id}: URL {url} is not a playlist.")
                        await client.edit_message(task.chat_id, progress_msg.id, f"❌ Task {task.id} Error: {error_msg}")
                        # Schedule error message for deletion after 60 seconds
                        await message_tracker.schedule_deletion(client, task, progress_msg.id, 60)
                        task.update_status(TaskStatus.FAILED, "Not a valid playlist URL")
                        task_manager._save_task(task)
                        task_manager.playlist_queue.task_done()
                        task_done_called = True
                        continue

                    title = playlist_info.get('title', 'Playlist')
                    entries = playlist_info.get('entries', [])
                    video_count = len(entries)
                    
                    if video_count == 0:
                        logger.warning(f"Task {task.id}: Playlist '{title}' is empty.")
                        await client.edit_message(task.chat_id, progress_msg.id, f"❌ Task {task.id}: No videos found in this playlist.")
                        # Schedule error message for deletion after 60 seconds
                        await message_tracker.schedule_deletion(client, task, progress_msg.id, 60)
                        task.update_status(TaskStatus.FAILED, "Playlist empty")
                        task_manager._save_task(task)
                        task_manager.playlist_queue.task_done()
                        task_done_called = True
                        continue

                    # Ask user how to download
                    quality_buttons = [
                        [Button.inline("✅ Download all (Best Quality)", data=f"playlist_best_{task.id}")],
                        # Option to select quality might be complex for playlists, consider adding later if needed
                        [Button.inline("❌ Cancel", data=f"cancel_{task.id}")]
                    ]
                    
                    # Present options through a single message update
                    await client.edit_message(
                        task.chat_id,
                        progress_msg.id,
                        f"📋 Playlist: {title}\n"
                        f"📺 Videos: {video_count}\n\n"
                        f"Task ID: {task.id}\n"
                        f"Please choose download option:",
                        buttons=quality_buttons
                    )
                    # Make sure this message is tracked for auto-deletion after a reasonable time
                    from utils.message_tracker import message_tracker
                    await message_tracker.schedule_deletion(client, task, progress_msg.id, 300)  # 5 minutes timeout
                    logger.info(f"Task {task.id}: Presented download options for playlist '{title}' to user {task.chat_id}")
                    # Task remains RUNNING, waiting for user callback
                finally:
                    task_manager.video_semaphore.release()
                    
            except asyncio.TimeoutError:
                logger.error(f"Task {task.id}: Timeout waiting for video semaphore")
                # Update existing message or send new one if needed
                if progress_msg:
                    await client.edit_message(task.chat_id, progress_msg.id, f"❌ Task {task.id} failed: Timed out waiting for available download slot")
                    # Schedule error message for deletion after 60 seconds
                    await message_tracker.schedule_deletion(client, task, progress_msg.id, 60)
                else:
                    error_msg = await send_tracked_message(
                        client, 
                        task.chat_id, 
                        f"❌ Task {task.id} failed: Timed out waiting for available download slot", 
                        task, 
                        auto_delete=60
                    )
                task.update_status(TaskStatus.FAILED, "Semaphore acquisition timeout")
                task_manager._save_task(task)
                task_manager.playlist_queue.task_done()
                task_done_called = True
            except Exception as outer_e:
                logger.error(f"Error processing playlist task {task.id}: {outer_e}", exc_info=True)
                task.update_status(TaskStatus.FAILED, str(outer_e))
                task_manager._save_task(task)
                if not progress_msg:
                    # Send error message with auto-deletion
                    await send_tracked_message(
                        client, 
                        task.chat_id, 
                        f"❌ Task {task.id} failed: System error occurred", 
                        task,
                        auto_delete=60
                    )
                else:
                    # Update existing message and schedule for deletion
                    try:
                        await client.edit_message(task.chat_id, progress_msg.id, f"❌ Task {task.id} failed: System error occurred")
                        await message_tracker.schedule_deletion(client, task, progress_msg.id, 60)
                    except Exception as e:
                        logger.error(f"Failed to update progress message after error: {e}")
        except Exception as e:
            logger.error(f"Outer exception in playlist queue handler: {e}")
            if task and not task_done_called:
                task.update_status(TaskStatus.FAILED, str(e))
                task_manager._save_task(task)
                
        finally:
            # Clean up any tracked messages if task is completed, failed or cancelled
            if task and task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                await delete_tracked_messages(client, task)
            
            if not task_done_called:
                task_manager.playlist_queue.task_done()

async def download_playlist_with_quality(task_manager, client, video_downloader, task_id: str, format_id: str, user_stats_manager):
    """Downloads a playlist with the selected quality, updating the message via task.message_id."""
    task = task_manager.get_task(task_id)
    if not task:
        logger.error(f"Task {task_id} not found for playlist download.")
        return

    progress_msg_id = task.message_id
    if not progress_msg_id:
        logger.error(f"Task {task_id}: Cannot proceed with playlist download, message ID missing.")
        task.update_status(TaskStatus.FAILED, "Internal error: Progress message ID missing")
        task_manager._save_task(task)
        return

    try:
        # Message already edited by callback handler to show "Starting playlist download..."
        url = task.data
        # Fetch playlist info again (or retrieve from task metadata if stored)
        logger.debug(f"Task {task_id}: Re-fetching playlist info for download.")
        playlist_info = await video_downloader.get_video_info(url)

        if 'error' in playlist_info:
            error_msg = playlist_info['error']
            logger.error(f"Task {task.id}: Error re-fetching playlist info: {error_msg}")
            await client.edit_message(task.chat_id, progress_msg_id, f"❌ Task {task.id} Error: {error_msg}")
            task.update_status(TaskStatus.FAILED, error_msg)
            task_manager._save_task(task)
            return

        title = playlist_info.get('title', 'Playlist')
        entries = playlist_info.get('entries', [])
        video_count = len(entries)

        if video_count == 0:
            logger.warning(f"Task {task.id}: Playlist '{title}' is empty (re-check).")
            await client.edit_message(task.chat_id, progress_msg_id, f"❌ Task {task.id}: Playlist is empty.")
            task.update_status(TaskStatus.FAILED, "Playlist empty")
            task_manager._save_task(task)
            return

        async with PlaylistDownloadSession(task.id, task.chat_id, title, os.path.join(DOWNLOAD_DIR, str(task.chat_id), re.sub(r'[^\w\-.]', '_', title))) as session:
            playlist_dir = session.playlist_dir # Use session's directory
            safe_title = session.title
            await async_os.makedirs(playlist_dir, exist_ok=True)

            # Update message with download details
            await client.edit_message(
                task.chat_id,
                progress_msg_id,
                f"📋 Playlist: {title}\n"
                f"📺 Videos: {video_count}\n"
                f"Task ID: {task.id}\n"
                f"⬇️ Starting download (Format: {format_id})...\n\n"
                f"Progress: 0/{video_count} (0✓ 0✗)" # Initial progress
            )

            last_progress_update_time = time.time()
            for index, entry in enumerate(entries, start=1):
                # Re-check task status periodically
                if task.status == TaskStatus.CANCELLED:
                    logger.info(f"Task {task.id}: Cancellation detected during playlist download.")
                    await client.edit_message(task.chat_id, progress_msg_id, f"❌ Task {task.id}: Download cancelled.")
                    # Cleanup handled by session __aexit__
                    return

                video_url = entry.get('webpage_url') or entry.get('url')
                video_title = entry.get('title', f'Video {index}')
                if not video_url:
                    logger.warning(f"Task {task.id}: Skipping video {index}, no URL found.")
                    session.record_failed_download()
                    continue

                # Define output template for this specific video
                safe_video_title = re.sub(r'[^\w\-.]', '_', video_title)[:80] # Shorter safe title
                output_path_template = os.path.join(playlist_dir, f"{index:03d}_{safe_video_title}.%(ext)s.temp")
                session.add_temp_file(output_path_template) # Add pattern for cleanup

                # Update progress message before starting each video
                now = time.time()
                if now - last_progress_update_time > 2.5: # Rate limit updates
                    try:
                        await client.edit_message(
                            task.chat_id,
                            progress_msg_id,
                            f"📋 Playlist: {title}\n"
                            f"📺 Videos: {video_count}\n"
                            f"Task ID: {task.id}\n"
                            f"⬇️ Downloading Video {index}/{video_count} (Format: {format_id})...\n"
                            f"'{video_title[:50]}...'\n\n"
                            f"Progress: {index-1}/{video_count} ({session.successful_downloads}✓ {session.failed_downloads}✗)"
                        )
                        last_progress_update_time = now
                    except Exception as e:
                        logger.warning(f"Task {task.id}: Failed to update playlist progress: {e}")

                try:
                    result = await video_downloader.download_video(
                        video_url,
                        format_id,
                        output_path_template
                    )

                    temp_file_path = result.get('path')
                    final_file_path = None
                    # Rename .temp file
                    if temp_file_path and os.path.exists(temp_file_path) and temp_file_path.endswith('.temp'):
                        final_file_path = temp_file_path[:-5]
                        try:
                            os.rename(temp_file_path, final_file_path)
                            session.temp_files.discard(output_path_template) # Remove patterns
                            session.temp_files.discard(temp_file_path)
                        except OSError as rename_err:
                            logger.error(f"Task {task.id}: Failed to rename {temp_file_path}: {rename_err}")
                            final_file_path = None # Failed rename

                    if result.get('success', False) and final_file_path and os.path.exists(final_file_path):
                        file_size = os.path.getsize(final_file_path)
                        if file_size > 0:
                            session.add_downloaded_file(final_file_path, file_size)
                            logger.debug(f"Task {task.id}: Video {index} downloaded successfully ({format_size(file_size)}).")
                        else:
                            session.record_failed_download()
                            logger.warning(f"Task {task.id}: Video {index} downloaded but file is empty: {final_file_path}")
                            if os.path.exists(final_file_path): os.remove(final_file_path) # Clean up empty file
                    else:
                        session.record_failed_download()
                        err_msg = result.get('error', 'Unknown error or file missing after rename')
                        logger.warning(f"Task {task.id}: Failed to download video {index}: {err_msg}")
                        # Clean up potentially failed temp file
                        if temp_file_path and os.path.exists(temp_file_path):
                             try: os.remove(temp_file_path)
                             except Exception: pass

                except Exception as e:
                    logger.error(f"Task {task.id}: Error downloading video {index} ('{video_title}'): {e}", exc_info=True)
                    session.record_failed_download()

                await asyncio.sleep(0.1) # Small delay between video downloads

            # --- Playlist Download Finished ---
            logger.info(f"Task {task.id}: Playlist download finished. Success: {session.successful_downloads}, Failed: {session.failed_downloads}")

            if session.successful_downloads > 0:
                # Edit message to show completion and next step (compress/upload)
                await client.edit_message(
                    task.chat_id,
                    progress_msg_id,
                    f"✅ Playlist download completed!\n"
                    f"📋 Playlist: {title}\n"
                    f"Task ID: {task.id}\n"
                    f"📺 Total Videos: {video_count}\n"
                    f"✅ Downloaded: {session.successful_downloads}\n"
                    f"❌ Failed: {session.failed_downloads}\n"
                    f"📂 Total Size: {format_size(session.total_downloaded_size)}\n\n"
                    f"⏳ Compressing {session.successful_downloads} downloaded videos..." # Changed message
                )

                user_stats_manager.record_completed_transfer(
                    task.chat_id, downloaded_size=session.total_downloaded_size, task_type="playlist", success=True
                )

                # --- Compress and Upload ---
                zip_name = f"{safe_title}_{time.strftime('%Y%m%d_%H%M%S')}"
                part_paths, temp_dir = await stream_compress(
                    session.downloaded_files,
                    zip_name,
                    ZIP_PART_SIZE,
                    task.chat_id,
                    task,
                    client,
                    task_manager
                )

                if part_paths:
                    upload_success, status_msgs = await upload_zip_parts(client, task.chat_id, part_paths, task, task_manager)
                    if upload_success:
                        task.update_status(TaskStatus.COMPLETED)
                        task.result_path = playlist_dir
                    else:
                        task.update_status(TaskStatus.FAILED, "Playlist upload failed")
                        await client.send_message(task.chat_id, f"❌ Task {task.id}: Uploading compressed playlist parts failed.")
                else:
                    logger.error(f"Task {task.id}: Compression failed for playlist.")
                    await client.edit_message(
                        task.chat_id, progress_msg_id,
                        f"❌ Task {task.id}: Compression failed. Cannot upload playlist."
                    )
                    task.update_status(TaskStatus.FAILED, "Compression failed")

            else:
                # No videos downloaded successfully
                error_msg = f"Failed to download any videos from the playlist '{title}'."
                logger.warning(f"Task {task.id}: {error_msg}")
                await client.edit_message(task.chat_id, progress_msg_id, f"❌ Task {task.id}: {error_msg}")
                task.update_status(TaskStatus.FAILED, "No videos downloaded")
                task_manager._save_task(task)
                user_stats_manager.record_completed_transfer(task.chat_id, 0, task_type="playlist", success=False)

    except Exception as e:
        logger.error(f"Error during playlist download for task {task_id}: {e}", exc_info=True)
        error_msg = f"An unexpected error occurred: {str(e)}"
        if task:
            task.update_status(TaskStatus.FAILED, error_msg)
            task_manager._save_task(task)
            user_stats_manager.record_completed_transfer(task.chat_id, 0, task_type="playlist", success=False)
            try:
                # Try to update the existing message with the error
                await client.edit_message(task.chat_id, progress_msg_id, f"❌ Task {task.id} Failed: {error_msg[:250]}")
            except Exception:
                pass # Ignore errors sending final error message

    finally:
        # Temp message cleanup handled by delete_temp_messages if task failed/cancelled
        # If successful, send_to_telegram handles the main progress message deletion
        pass
