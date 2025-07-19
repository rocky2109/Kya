import os
import re
import time
import asyncio
import logging
from typing import Optional
import glob
from asyncio import TimeoutError

from telethon import Button

# Assuming these modules are accessible from the parent directory or PYTHONPATH
from config import DOWNLOAD_DIR
from enums import TaskStatus
from models import Task
# from task_manager import task_manager # Pass instance or import
# from bot_client import client, video_downloader # Pass instances or import
from utils.formatting import format_size
from utils.telegram_helpers import send_to_telegram, delete_temp_messages, send_tracked_message, delete_tracked_messages

logger = logging.getLogger(__name__)

class DownloadSession:
    """Context manager for video download sessions to ensure proper cleanup."""
    def __init__(self, task_id: str, chat_id: int, title: str = None):
        self.task_id = task_id
        self.chat_id = chat_id
        self.title = title or 'video'
        self.safe_title = re.sub(r'[^\w\-.]', '_', self.title)
        self.user_dir = os.path.join(DOWNLOAD_DIR, str(chat_id))
        self.temp_files = set()
        self.progress_processor = None

    async def __aenter__(self):
        """Setup download directory and resources."""
        os.makedirs(self.user_dir, exist_ok=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper cleanup of resources and temporary files."""
        # Cancel progress processor if it exists and is still running
        if self.progress_processor and not self.progress_processor.done():
            logger.debug(f"Task {self.task_id}: Cancelling progress processor.")
            self.progress_processor.cancel()
            try:
                await self.progress_processor
            except asyncio.CancelledError:
                pass

        # Clean up temporary files if we had an error or specifically marked files
        if exc_type or self.temp_files:
            try:
                # Look for any temporary files that may have been created
                patterns = [
                    os.path.join(self.user_dir, f"{self.safe_title}.*"),
                    os.path.join(self.user_dir, f"*.part"),
                    os.path.join(self.user_dir, f"*.ytdl")
                ]
                
                for pattern in patterns:
                    for temp_file in glob.glob(pattern):
                        if temp_file not in self.temp_files:
                            self.temp_files.add(temp_file)

                # Try to remove all temporary files
                for temp_file in self.temp_files:
                    try:
                        if os.path.exists(temp_file):
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

            except Exception as e:
                logger.warning(f"Task {self.task_id}: Error during session cleanup: {e}")

    def add_temp_file(self, file_path: str):
        """Add a file to be cleaned up when the session ends."""
        self.temp_files.add(file_path)

    def set_progress_processor(self, processor: asyncio.Task):
        """Set the progress processor task for cleanup."""
        self.progress_processor = processor

async def update_video_progress(client, chat_id, message_id, text, min_interval=2.5):
    """Update video progress message with rate limiting to prevent flood.
       Uses a simple time-based check per message ID.
    """
    key = f"progress_{chat_id}_{message_id}"
    current_time = time.time()

    # Store last update time as an attribute of the function itself
    last_update = getattr(update_video_progress, key, 0)

    if current_time - last_update >= min_interval:
        try:
            await client.edit_message(chat_id, message_id, text)
            setattr(update_video_progress, key, current_time)
            logger.debug(f"Updated video progress for message {message_id}")
            return True
        except Exception as e:
            # Ignore common errors like message not modified or flood waits
            if "Message not modified" not in str(e) and "FloodWait" not in str(e):
                 logger.warning(f"Video progress update error for {message_id}: {e}")
            # Update timestamp even on ignored errors to prevent rapid retries
            setattr(update_video_progress, key, current_time)
    return False


async def handle_video_queue(task_manager, client, video_downloader, user_stats_manager):
    """Process video download tasks from the queue."""
    while True:
        task = await task_manager.video_queue.get()
        logger.info(f"Dequeued video task {task.id} for user {task.chat_id}")

        if task.status == TaskStatus.CANCELLED:
            logger.info(f"Task {task.id} was cancelled, skipping processing.")
            await delete_tracked_messages(client, task) # Clean up any stray messages
            task_manager.video_queue.task_done()
            continue

        task_done_called = False
        initial_processing_msg = None 

        try:
            task.update_status(TaskStatus.PROCESSING, "Fetching video info")
            task_manager._save_task(task)

            url = task.data
            initial_processing_msg_text = f"⏳ Fetching video information for your link..."
            initial_processing_msg = await send_tracked_message(client, task.chat_id, initial_processing_msg_text, task)

            logger.debug(f"Task {task.id}: Getting video info for {url}")
            video_info = await video_downloader.get_video_info(url, task.chat_id)  # Pass user_id for cookies

            if initial_processing_msg:
                await delete_tracked_messages(client, task, specific_ids=[initial_processing_msg.id])
                initial_processing_msg = None 

            if 'error' in video_info:
                error_msg = video_info['error']
                error_type = video_info.get('error_type', 'unknown')
                
                # Provide user-friendly error message based on error type
                if error_type == 'authentication':
                    user_error_msg = (
                        f"❌ Authentication Required\n\n"
                        f"🔒 This content requires login/cookies.\n\n"
                        f"💡 **Solution**: Upload your cookies using `/setcookies`\n"
                        f"This enables downloading:\n"
                        f"• Private content\n"
                        f"• Age-restricted videos\n"
                        f"• Content without rate limits\n\n"
                        f"📖 Use `/help` for setup instructions."
                    )
                elif error_type == 'content_unavailable':
                    user_error_msg = (
                        f"❌ Content Not Available\n\n"
                        f"📵 This content cannot be accessed:\n"
                        f"• May be private or deleted\n"
                        f"• Could be region-restricted\n"
                        f"• Account might be suspended\n\n"
                        f"Try a different link or check availability."
                    )
                else:
                    user_error_msg = f"❌ Error: {error_msg}"
                
                logger.error(f"Task {task.id}: Error getting video info: {error_msg}")
                task.update_status(TaskStatus.FAILED, f"Video info error: {error_msg}")
                await send_tracked_message(client, task.chat_id, user_error_msg, task=task, auto_delete=120) 
                task_manager._save_task(task)
                task_manager.video_queue.task_done()
                task_done_called = True
                await delete_tracked_messages(client, task) 
                continue

            title = video_info.get('title', 'video')
            formats = video_info.get('formats', [])
            uploader = video_info.get('uploader', '')
            thumbnail_url = video_info.get('thumbnail')

            if not formats:
                logger.warning(f"Task {task.id}: No formats found for {url}")
                task.update_status(TaskStatus.FAILED, "No downloadable formats found.")
                await send_tracked_message(client, task.chat_id, "❌ No downloadable formats found for this video.", task=task, auto_delete=60)
                task_manager._save_task(task)
                task_manager.video_queue.task_done()
                task_done_called = True
                await delete_tracked_messages(client, task)
                continue

            buttons = []
            video_rows = []
            audio_rows = []
            for fmt in formats:
                # Build label for video and audio
                if fmt.get('vcodec') == 'none':
                    # Audio only
                    audio_label = "\U0001F3B5 audio"
                    audio_rows.append([Button.inline(audio_label, data=f"quality_{task.id}_{fmt['format_id']}")])
                else:
                    # Video: show as e.g. 1920x1080, 1280x720, etc.
                    width = fmt.get('width')
                    height = fmt.get('height')
                    if width and height:
                        res = f"{width}x{height}"
                    else:
                        res = fmt.get('format_note', '') or fmt.get('desc', '') or f"{fmt.get('height', '')}p"
                    video_label = f"\u2B07\uFE0F {res}"
                    video_rows.append([Button.inline(video_label, data=f"quality_{task.id}_{fmt['format_id']}")])
            buttons.extend(video_rows)
            buttons.extend(audio_rows)
            buttons.append([Button.inline("❌ Cancel Download", data=f"cancelq_{task.id}")])

            caption = f"🎬 Video: {title}\n"
            if uploader:
                caption += f"👤 Uploader: {uploader}\n\n"
            caption += "Please select the quality you want to download:"
            
            if task.message_id: 
                await client.edit_message(task.chat_id, task.message_id, caption, buttons=buttons, link_preview=False)
                task.add_temp_message(task.message_id) 
            else: 
                quality_options_msg = await send_tracked_message(client, task.chat_id, caption, task, buttons=buttons, link_preview=False, auto_delete=300)  # Auto-delete after 5 minutes if not selected
                task.message_id = quality_options_msg.id

            task.update_status(TaskStatus.AWAITING_USER_INPUT, "Waiting for quality selection")
            task_manager._save_task(task)

        except Exception as outer_e:
            logger.error(f"Outer error in handle_video_queue for task {task.id}: {outer_e}", exc_info=True)
            if task:
                task.update_status(TaskStatus.FAILED, f"Queue error: {str(outer_e)}")
                task_manager._save_task(task)
                await send_tracked_message(client, task.chat_id, f"❌ An unexpected error occurred: {str(outer_e)}", task=task, auto_delete=60)
                await delete_tracked_messages(client, task)
            # Ensure task_done is called
            if not task_done_called:
                task_manager.video_queue.task_done()
                task_done_called = True
        finally:
            # Clean up tracked messages for completed/failed/cancelled tasks
            if task and task.status in (TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.COMPLETED): 
                await delete_tracked_messages(client, task)
            
            # Ensure task_done() is always called exactly once
            if not task_done_called:
                task_manager.video_queue.task_done()


async def download_video_with_quality(task_manager, client, video_downloader, task_id: str, format_id: str, user_stats_manager):
    """Downloads a video with the selected quality, updating the message via task.message_id."""
    task = task_manager.get_task(task_id)
    if not task:
        logger.error(f"Task {task_id} not found for download.")
        return

    quality_options_message_id = task.message_id 
    task.message_id = None 

    progress_update_msg = None 

    try:
        if quality_options_message_id:
            await delete_tracked_messages(client, task, specific_ids=[quality_options_message_id])

        url = task.data 
        video_info = await video_downloader.get_video_info(url, task.chat_id)  # Pass user_id for cookies
        
        # Check if video_info contains an error
        if 'error' in video_info:
            error_msg = video_info['error']
            error_type = video_info.get('error_type', 'unknown')
            
            # Provide user-friendly error message based on error type
            if error_type == 'authentication':
                user_error_msg = (
                    f"❌ Instagram Download Failed\n\n"
                    f"🔒 Authentication required for this Instagram content.\n\n"
                    f"💡 **Solution**: Upload your Instagram cookies using `/setcookies`\n"
                    f"This will allow you to download:\n"
                    f"• Private posts and stories\n"
                    f"• Content without rate limits\n"
                    f"• Better success rates\n\n"
                    f"📖 Use `/help` for cookie setup instructions."
                )
            elif error_type == 'content_unavailable':
                user_error_msg = (
                    f"❌ Content Not Available\n\n"
                    f"📵 This Instagram content is not accessible:\n"
                    f"• May be private or deleted\n"
                    f"• Could be region-restricted\n"
                    f"• Account might be suspended\n\n"
                    f"Try a different link or check if the content is still available."
                )
            else:
                user_error_msg = f"❌ Download failed: {error_msg}"
            
            logger.error(f"Task {task_id}: {error_msg}")
            task.update_status(TaskStatus.FAILED, error_msg)
            await send_tracked_message(client, task.chat_id, user_error_msg, task=task, auto_delete=120)
            task_manager._save_task(task)
            await delete_tracked_messages(client, task) 
            return
        
        title = video_info.get('title', 'Video')
        selected_format = next((f for f in video_info.get('formats', []) if f['format_id'] == format_id), None)
        
        if not selected_format:
            error_msg = "Selected quality not found."
            logger.error(f"Task {task_id}: {error_msg}")
            task.update_status(TaskStatus.FAILED, error_msg)
            await send_tracked_message(client, task.chat_id, f"❌ Error: {error_msg}", task=task, auto_delete=60)
            task_manager._save_task(task)
            await delete_tracked_messages(client, task) 
            return

        download_status_text = f"🎬 Video: {title}\n⏳ Download starting for quality: `{selected_format.get('format_note', format_id)}`..."
        progress_update_msg = await send_tracked_message(client, task.chat_id, download_status_text, task)
        task.message_id = progress_update_msg.id 
        
        task.update_status(TaskStatus.RUNNING, f"Downloading format {format_id}")
        task_manager._save_task(task)

        async with DownloadSession(task.id, task.chat_id, title) as session:
            last_update_time = 0
            async def _progress_callback(d):
                nonlocal last_update_time
                if d['status'] == 'downloading':
                    current_time = time.time()
                    if current_time - last_update_time < 2.5: 
                        return
                    last_update_time = current_time

                    total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate')
                    downloaded_bytes = d.get('downloaded_bytes', 0)
                    speed = d.get('speed', 0)
                    eta = d.get('eta', 0)
                    
                    if total_bytes:
                        percent = downloaded_bytes / total_bytes * 100
                        task.update_progress(percent)
                        progress_text = f"Downloading: {percent:.1f}%\n" \
                                        f"{format_size(downloaded_bytes)} / {format_size(total_bytes)}\n" \
                                        f"Speed: {format_size(speed)}/s ETA: {eta}s"
                        if task.message_id: 
                           await client.edit_message(task.chat_id, task.message_id, f"🎬 **Video:** `{title}`\n{progress_text}")
                elif d['status'] == 'finished':
                    task.update_progress(100)
                    if task.message_id:
                        await client.edit_message(task.chat_id, task.message_id, f"🎬 **Video:** `{title}`\nDownload finished, processing...")
                    logger.info(f"Task {task.id}: Starting download for format {format_id} into {session.user_dir}")
                    logger.info(f"Task {task.id}: Download finished by yt-dlp: {d.get('filename')}")
            
            final_video_path = await video_downloader.download_video(
                url,
                format_id,
                output_path=os.path.join(session.user_dir, f"{session.safe_title}.%(ext)s"),
                progress_callback=_progress_callback
            )
            
            if not final_video_path or 'error' in final_video_path:
                error_detail = final_video_path.get('error', 'Download failed, no path returned.') if isinstance(final_video_path, dict) else 'Download failed'
                raise Exception(error_detail)
            
            # Extract the actual file path from the response
            if isinstance(final_video_path, dict):
                actual_path = final_video_path.get('path')
                if not actual_path or not os.path.exists(actual_path):
                    raise Exception(f"Downloaded video file not found: {actual_path}")
                final_video_path = actual_path
            elif isinstance(final_video_path, str):
                if not os.path.exists(final_video_path):
                    raise Exception(f"Downloaded video file not found: {final_video_path}")
            else:
                raise Exception(f"Invalid download response type: {type(final_video_path)}")
                
            session.add_temp_file(final_video_path)
            
            if progress_update_msg: 
                await delete_tracked_messages(client, task, specific_ids=[progress_update_msg.id])
                task.message_id = None 

            # The send_to_telegram function should handle sending the final file without auto-deletion
            await send_to_telegram(client, final_video_path, task.chat_id, task=task, task_manager=task_manager)
            task.update_status(TaskStatus.COMPLETED, f"File sent: {os.path.basename(final_video_path)}")
            task.result_path = final_video_path 

        # --- End of DownloadSession block ---

    except Exception as e:
        logger.error(f"Error in download_video_with_quality for task {task.id}: {e}", exc_info=True)
        if task:
            task.update_status(TaskStatus.FAILED, f"Download error: {str(e)}")
            if progress_update_msg and progress_update_msg.id == task.message_id : 
                 await delete_tracked_messages(client, task, specific_ids=[progress_update_msg.id])
                 task.message_id = None 
            
            await send_tracked_message(client, task.chat_id, f"❌ Download failed: {str(e)}", task=task, auto_delete=60) 
    finally:
        if task:
            task_manager._save_task(task) 
            await delete_tracked_messages(client, task)
        logger.info(f"Finished processing download_video_with_quality for task {task.id}")
