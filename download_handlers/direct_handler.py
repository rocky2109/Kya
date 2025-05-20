import os
import re
import time
import asyncio
import logging
import shutil
import concurrent.futures
from typing import Optional
from pySmartDL import SmartDL, utils
import urllib.parse

# Assuming these modules are accessible
from config import DOWNLOAD_DIR
from enums import TaskStatus
from models import Task
# from task_manager import task_manager # Pass instance or import
# from bot_client import client # Pass instance or import
from utils.formatting import format_size, format_time
from utils.telegram_helpers import send_to_telegram, delete_temp_messages, send_tracked_message, delete_tracked_messages # Import necessary helpers
from utils.debouncer import message_debouncer # Import debouncer
from utils.message_tracker import message_tracker # Import message tracker for auto-deletion
from utils.message_tracker import message_tracker # Import message tracker for auto-deletion

logger = logging.getLogger(__name__)

def extract_filename_from_url(url: str) -> str:
    """Extract a filename from a URL, handling various edge cases.
    
    Args:
        url: The URL to extract filename from
        
    Returns:
        A filename extracted from the URL or a timestamp-based default
    """
    try:
        # Parse the URL
        parsed_url = urllib.parse.urlparse(url)
        
        # Get the path part and remove query parameters
        path = parsed_url.path
        
        # Extract the basename (last part of the path)
        filename = os.path.basename(path)
        
        # URL decode the filename (handle URL encoding like %20 for spaces)
        filename = urllib.parse.unquote(filename)
        
        # If filename is empty or has no extension, try to find it in the URL
        if not filename or '.' not in filename:
            # Try to find a pattern that looks like a filename in the URL
            match = re.search(r'/([^/?#]+\.[^/?#]+)(?:[?#]|$)', url)
            if match:
                filename = match.group(1)
            else:
                # Return default filename if extraction fails
                return f"download_{int(time.time())}"
        
        # Final validation and cleanup
        if not filename or len(filename) < 3:
            return f"download_{int(time.time())}"
        
        return filename
    
    except Exception as e:
        logger.warning(f"Error extracting filename from URL '{url}': {e}")
        return f"download_{int(time.time())}"

class DirectDownloadSession:
    """Context manager for direct download sessions to ensure proper cleanup."""
    def __init__(self, task_id: str, chat_id: int, filename: str):
        self.task_id = task_id
        self.chat_id = chat_id
        self.filename = filename
        self.user_dir = os.path.join(DOWNLOAD_DIR, str(chat_id))
        self.temp_files = set()
        self.dl_obj = None
        self.dl_thread_executor = None
        self.download_future = None
        self.progress_msg = None

    async def __aenter__(self):
        """Setup download directory and resources."""
        os.makedirs(self.user_dir, exist_ok=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper cleanup of download resources."""
        # Stop download if still running
        if self.dl_obj and not self.dl_obj.isFinished():
            try:
                self.dl_obj.stop()
                logger.info(f"Task {self.task_id}: Stopped download in cleanup.")
                
                # Wait briefly for thread to finish
                if self.download_future:
                    try:
                        await asyncio.wait_for(self.download_future, timeout=5.0)
                    except (asyncio.TimeoutError, concurrent.futures.CancelledError):
                        pass  # Ignore errors during cleanup wait
            except Exception as e:
                logger.warning(f"Task {self.task_id}: Error stopping download: {e}")

        # Shutdown thread executor
        if self.dl_thread_executor:
            try:
                self.dl_thread_executor.shutdown(wait=False)
            except Exception as e:
                logger.warning(f"Task {self.task_id}: Error shutting down executor: {e}")

        # Clean up temporary files
        if exc_type or self.temp_files:  # Clean on error or if files marked
            for temp_file in self.temp_files:
                if os.path.exists(temp_file):
                    try:
                        # Add retry logic for busy files
                        for attempt in range(3):
                            try:
                                os.remove(temp_file)
                                logger.debug(f"Task {self.task_id}: Removed temp file: {temp_file}")
                                break
                            except PermissionError:
                                if attempt < 2:  # Last attempt
                                    await asyncio.sleep(1 * (2 ** attempt))  # Exponential backoff
                                    continue
                                raise
                    except Exception as e:
                        logger.warning(f"Task {self.task_id}: Failed to remove temp file {temp_file}: {e}")

    def set_dl_obj(self, dl_obj):
        """Set the SmartDL object for cleanup."""
        self.dl_obj = dl_obj

    def set_thread_executor(self, executor, future):
        """Set the thread executor and future for cleanup."""
        self.dl_thread_executor = executor
        self.download_future = future

    def set_progress_msg(self, msg):
        """Set the progress message for potential cleanup."""
        self.progress_msg = msg

    def add_temp_file(self, file_path: str):
        """Add a file to be cleaned up when the session ends."""
        self.temp_files.add(file_path)

async def update_download_progress(client, chat_id, message_id, text, min_interval=2.5):
    """Update download progress message with rate limiting."""
    try:
        return await message_debouncer.update(client, chat_id, message_id, text, min_interval=min_interval)
    except Exception as e:
        logger.warning(f"Failed to update download progress: {e}")
        return False

async def download_with_pysmartdl(task_manager, client, url: str, chat_id: int, task: Optional[Task], user_stats_manager):
    """
    Handle direct downloads using PySmartDL with progress tracking and error handling.
    Uses the message_id stored in the task object for updates.
    """
    filename = "download" # Default filename
    progress_msg_id = task.message_id if task else None # Get message ID from task

    if not progress_msg_id:
        logger.error(f"Task {task.id}: Cannot proceed with download, initial progress message ID not found in task.")
        # Update task status if possible
        if task:
            task.update_status(TaskStatus.FAILED, "Internal error: Progress message ID missing")
            task_manager._save_task(task)
        # No message to update, so just return/raise
        return # Or raise an exception

    try:
        # --- Prepare Download ---
        async with DirectDownloadSession(task.id, chat_id, filename) as session:
            # Extract filename (basic attempt)
            try:
                filename = extract_filename_from_url(url)
                filename = re.sub(r'[^\w\-.]', '_', filename)[:100]
                if not filename: filename = f"download_{int(time.time())}" # Ensure filename is not empty
            except Exception as e:
                logger.warning(f"Could not reliably determine filename from URL '{url}': {e}. Using default.")
                filename = f"download_{int(time.time())}"

            dest_path = os.path.join(session.user_dir, filename)
            temp_dest = f"{dest_path}.partial" # Temporary file path
            session.add_temp_file(temp_dest)            # --- Update Initial Message (Edit) ---
            try:
                await client.edit_message(
                    chat_id, progress_msg_id,
                    f"⏳ Task {task.id}: Preparing download...\n"
                    f"📁 File: {filename}\n"
                    f"URL: {url[:60]}{'...' if len(url) > 60 else ''}"
                )
                # Schedule for auto-deletion
                await message_tracker.schedule_deletion(client, task, progress_msg_id, 600)  # 10 minutes timeout
            except Exception as e:
                 logger.warning(f"Task {task.id}: Failed to edit initial message: {e}")
                 # Continue anyway, progress loop will try to edit

            # --- Initialize SmartDL ---
            num_threads = 4
            logger.info(f"Task {task.id}: Initializing SmartDL for {url} with {num_threads} threads.")
            try:
                dl_obj = SmartDL(
                    url, dest=temp_dest, progress_bar=False, threads=num_threads, timeout=60
                )
                session.set_dl_obj(dl_obj)
            except Exception as e:
                logger.error(f"Task {task.id}: Failed to initialize SmartDL: {e}")
                raise ConnectionError(f"Failed to initialize download: {e}")

            # --- Start Download in Thread ---
            dl_thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            loop = asyncio.get_event_loop()
            download_future = loop.run_in_executor(dl_thread_executor, dl_obj.start)
            session.set_thread_executor(dl_thread_executor, download_future)
            logger.info(f"Task {task.id}: Download thread started.")

            # --- Progress Monitoring Loop ---
            last_update_time = time.time()
            last_percent_reported = -1
            update_interval = 2.5 # Seconds between updates

            while not dl_obj.isFinished():
                # Check task status for cancellation
                if task and task.status == TaskStatus.CANCELLED:
                    logger.info(f"Task {task.id}: Cancellation detected during direct download.")
                    if not dl_obj.isFinished():
                        dl_obj.stop() # Attempt to stop pySmartDL
                    # Edit the message to show cancellation
                    await update_download_progress(client, chat_id, progress_msg_id, f"❌ Task {task.id}: Download cancelled.", min_interval=0)
                    # Wait briefly for thread to potentially finish stopping
                    try:
                        await asyncio.wait_for(download_future, timeout=5.0)
                    except (asyncio.TimeoutError, concurrent.futures.CancelledError):
                        pass # Ignore errors during cancellation cleanup
                    dl_thread_executor.shutdown(wait=False) # Don't wait indefinitely
                    return # Exit handler

                await asyncio.sleep(1) # Check progress every second

                try:
                    if dl_obj.isFinished(): break # Exit loop if finished

                    current_size = dl_obj.get_dl_size()
                    total_size = dl_obj.get_final_filesize()

                    if total_size and total_size > 0:
                        percent = dl_obj.get_progress() * 100
                        speed_str = dl_obj.get_speed(human=True)
                        eta_str = dl_obj.get_eta(human=True)
                        downloaded_str = format_size(current_size)
                        total_str = format_size(total_size)

                        # Update task object progress
                        if task and abs(percent - (task.progress * 100)) > 1.0: # Update if > 1% change
                            task.update_progress(percent / 100)
                            # task_manager._save_task(task) # Avoid saving on every update

                        # Update message periodically using debouncer
                        now = time.time()
                        if now - last_update_time >= update_interval or percent == 100.0:
                            bar = '█' * int(percent / 5) + '░' * (20 - int(percent / 5))
                            status_text = (
                                f"⬇️ Downloading: {filename}\n"
                                f"Task ID: {task.id}\n"
                                f"📊 Progress: {percent:.1f}% ({downloaded_str}/{total_str})\n"
                                f"🚀 Speed: {speed_str}\n"
                                f"⏱️ ETA: {eta_str}\n"
                                f"[{bar}]"
                            )
                            # Use the message ID from the task
                            updated = await update_download_progress(client, chat_id, progress_msg_id, status_text, min_interval=update_interval - 0.5)
                            if updated:
                                last_update_time = now
                                last_percent_reported = percent
                    else:
                        # Still connecting or getting file size
                        status = dl_obj.get_status()
                        now = time.time()
                        if now - last_update_time >= 5.0: # Update less frequently if no size yet
                            status_text = (
                                f"⏳ Downloading: {filename}\n"
                                f"Task ID: {task.id}\n"
                                f"📊 Status: {status}..."
                            )
                            # Use the message ID from the task
                            updated = await update_download_progress(client, chat_id, progress_msg_id, status_text, min_interval=4.5)
                            if updated: last_update_time = now

                except Exception as e:
                    logger.warning(f"Task {task.id}: Error during progress update: {e}")

            # --- Wait for Download Thread to Complete ---
            logger.info(f"Task {task.id}: Waiting for download thread to finish...")
            await download_future # Wait for the dl_obj.start() call to return
            dl_thread_executor.shutdown() # Clean up executor
            logger.info(f"Task {task.id}: Download thread finished.")

            # --- Process Download Result ---
            if dl_obj.isSuccessful():
                final_size = dl_obj.get_final_filesize()
                dl_time = dl_obj.get_dl_time()
                avg_speed = final_size / dl_time if dl_time > 0 else 0

                logger.info(f"Task {task.id}: Download successful! Size: {format_size(final_size)}, Time: {format_time(dl_time)}, Avg Speed: {format_size(avg_speed)}/s")

                # Move temp file to final destination
                if os.path.exists(temp_dest):
                    logger.debug(f"Task {task.id}: Moving {temp_dest} to {dest_path}")
                    shutil.move(temp_dest, dest_path)
                else:
                    raise FileNotFoundError(f"Download completed but temporary file not found: {temp_dest}")

                # Final success message - Edit to "Uploading..."
                final_text = (
                    f"✅ Download complete: {filename}\n"
                    f"💾 Size: {format_size(final_size)}\n"
                    f"Task ID: {task.id}\n"
                    f"📤 Uploading..."
                )
                # Use the message ID from the task
                await update_download_progress(client, chat_id, progress_msg_id, final_text, min_interval=0) # Force update

                # Update Task and Stats
                if task:
                    task.result_path = dest_path
                    task.update_status(TaskStatus.COMPLETED) # Mark download as completed
                    task_manager._save_task(task)

                user_stats_manager.record_completed_transfer(
                    chat_id, downloaded_size=final_size, task_type="direct", success=True
                )                # Send file to Telegram (will delete progress message on success)
                await send_to_telegram(client, dest_path, chat_id, task, task_manager)
                
                # Clean up any remaining tracked messages
                await delete_tracked_messages(client, task)

                return dest_path # Return final path

            else:
                # Download failed
                errors = dl_obj.get_errors()
                error_msg = "Download failed."
                if errors:
                    error_details = " | ".join(str(e)[:100] for e in errors)
                    error_msg += f" Reason: {error_details}"
                logger.error(f"Task {task.id}: {error_msg}")
                raise Exception(error_msg) # Raise exception to be caught below    except Exception as e:
        error_msg = str(e)
        logger.error(f"Direct download error for task {task.id}: {error_msg}", exc_info=True)

        # Try to update progress message with error using the task's message ID
        if progress_msg_id:
            try:
                await update_download_progress(client, chat_id, progress_msg_id, f"❌ Download failed: {error_msg[:200]}", min_interval=0)
                # Schedule error message for deletion after 60 seconds
                await message_tracker.schedule_deletion(client, task, progress_msg_id, 60)
            except Exception as update_err:
                logger.error(f"Failed to update error message: {update_err}")
        # else: If no message ID, we can't update, error already logged.        # Update task status and stats
        if task:
            task.update_status(TaskStatus.FAILED, error_msg)
            task_manager._save_task(task)
            logger.error(f"Direct download failed for task {task.id}: {error_msg}")
            # Clean up all tracked messages after failure
            await delete_tracked_messages(client, task)

        user_stats_manager.record_completed_transfer(chat_id, 0, task_type="direct", success=False)

    except Exception as e:
        logger.error(f"Direct download error for task {task.id}: {e}", exc_info=True)
