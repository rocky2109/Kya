import os
import time
import logging
import asyncio
import shutil
import re
from typing import List, Tuple, Optional

from telethon.tl.types import InputFile, InputFileBig, DocumentAttributeFilename
from telethon.errors import FloodWaitError, MessageNotModifiedError  # Removed FileTooLargeError

# Assuming config and utils.formatting are accessible
from config import DOWNLOAD_DIR, MAX_FILE_SIZE, ZIP_PART_SIZE
from utils.formatting import format_size, format_time
from utils.compressor import stream_compress  # Import the compressor function

from models import Task
from enums import TaskStatus

logger = logging.getLogger(__name__)

# --- Progress Update Helpers ---

async def update_progress_message(client, chat_id, msg_id, current, total, filename, start_time, status_text_prefix=""):
    """Update progress message for uploads/downloads with rate limiting."""
    # Use a simple time-based debouncer per message ID
    now = time.time()
    message_key = f"progress_{chat_id}_{msg_id}"
    last_update_time = getattr(update_progress_message, message_key, 0)

    if now - last_update_time < 2.0:  # Update at most every 2 seconds
        return

    try:
        if total > 0:
            percent = current / total * 100
            elapsed_time = now - start_time
            speed = current / elapsed_time if elapsed_time > 0 else 0
            eta_seconds = (total - current) / speed if speed > 0 else 0

            bar = '█' * int(percent / 5) + '░' * (20 - int(percent / 5))
            status_text = (
                f"{status_text_prefix}"
                f"📊 Progress: {percent:.1f}% ({format_size(current)} / {format_size(total)})\n"
                f"🚀 Speed: {format_size(speed)}/s\n"
                f"⏱️ ETA: {format_time(eta_seconds)}\n"
                f"[{bar}]"
            )

            await client.edit_message(chat_id, msg_id, status_text)
            setattr(update_progress_message, message_key, now)  # Store last update time

    except FloodWaitError as e:
        logger.warning(f"Flood wait during progress update: {e.seconds}s. Skipping update.")
        # Store future time to prevent updates during flood wait
        setattr(update_progress_message, message_key, now + e.seconds)
    except MessageNotModifiedError:
        # If the message hasn't changed, update the timestamp anyway to prevent rapid retries
        setattr(update_progress_message, message_key, now)
    except Exception as e:
        logger.warning(f"Failed to update progress message {msg_id}: {e}")

async def safe_edit_message(client, chat_id, message_id, new_text):
    """
    Safely edit a message by comparing content first to avoid 'not modified' errors.
    DEPRECATED in favor of handling MessageNotModifiedError in update_progress_message.
    Kept for reference or other use cases if needed.
    """
    try:
        await client.edit_message(chat_id, message_id, new_text)
    except MessageNotModifiedError:
        logger.debug(f"Message {message_id} not modified.")
        pass  # Ignore if message is the same
    except FloodWaitError as e:
        logger.warning(f"Flood wait editing message {message_id}: {e.seconds}s")
        await asyncio.sleep(e.seconds)  # Wait before potentially retrying elsewhere
    except Exception as e:
        logger.error(f"Error editing message {message_id}: {e}")


# --- File Upload Helpers ---

async def upload_file_with_telethon(client, chat_id, file_path, caption=None, task=None, task_manager=None) -> Tuple[bool, Optional[int]]:
    """Upload a single file using Telethon with progress tracking and status messages."""
    status_msg = None
    status_msg_id = None
    success = False
    try:
        if not os.path.exists(file_path):
            logger.error(f"File not found for upload: {file_path}")
            return False, None

        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)

        # Send initial status message
        try:
            status_msg = await client.send_message(
                chat_id, f"📤 Preparing to upload: {file_name}\nSize: {format_size(file_size)}"
            )
            status_msg_id = status_msg.id
            if task and task_manager:
                # Link status message to task for potential cleanup
                task.add_temp_message(status_msg_id)
                task_manager.update_task(task.id, message_id=status_msg_id)  # Maybe update main task msg?
        except Exception as e:
            logger.error(f"Failed to send initial upload status message: {e}")
            # Proceed without status updates if sending fails

        upload_start_time = time.time()

        # Define the progress callback function
        async def progress_callback_async(current, total):
            if status_msg_id:  # Only update if we have a status message
                await update_progress_message(
                    client, chat_id, status_msg_id, current, total,
                    file_name, upload_start_time, status_text_prefix=f"📤 Uploading: {file_name}\n"
                )

        # Use the built-in Telethon upload with the async progress callback
        await client.send_file(
            chat_id,
            file=file_path,
            caption=caption or f"📁 {file_name}",
            progress_callback=progress_callback_async  # Pass the async function directly
        )

        success = True
        upload_duration = time.time() - upload_start_time

        # Delete the progress message AFTER successful upload
        if status_msg_id:
            try:
                await client.delete_messages(chat_id, status_msg_id)
                logger.debug(f"Deleted progress message {status_msg_id}")
                if task and status_msg_id in task.temp_message_ids:
                    task.temp_message_ids.remove(status_msg_id)
                    if task_manager:
                        task_manager._save_task(task)
            except Exception as e:
                logger.warning(f"Failed to delete progress message {status_msg_id}: {e}")

        logger.info(f"Successfully uploaded {file_name} ({format_size(file_size)}) in {format_time(upload_duration)}")
        return True, status_msg_id  # Return success and the ID of the status message

    except Exception as e:
        logger.error(f"Upload error for {file_path}: {e}", exc_info=True)
        error_msg = f"❌ Upload failed: {os.path.basename(file_path)}\nError: {str(e)[:100]}"  # Truncate long errors
        if status_msg_id:
            try:
                await client.edit_message(chat_id, status_msg_id, error_msg)
            except Exception:
                try:
                    await client.send_message(chat_id, error_msg)
                except Exception:
                    pass
        else:
            try:
                await client.send_message(chat_id, error_msg)
            except Exception:
                pass

        return False, status_msg_id  # Return failure and the status message ID (if any)


async def send_to_telegram(client, file_path: str, chat_id: int, task: Optional[Task] = None, task_manager=None):
    """Sends the file to Telegram, handling potential splitting and cleanup."""
    
    if not os.path.exists(file_path):
        logger.error(f"File not found for sending: {file_path}")
        if task and task_manager and task.message_id:
             try:
                 await client.edit_message(chat_id, task.message_id, f"❌ Task {task.id} Error: Final file not found.")
             except Exception: pass
        elif task: # Send new message if edit failed or no message_id
             try:
                 await client.send_message(chat_id, f"❌ Task {task.id} Error: Final file not found.")
             except Exception: pass
        # Update task status if it wasn't already failed
        if task and task.status == TaskStatus.COMPLETED: # Check if it was marked completed before upload
             task.update_status(TaskStatus.FAILED, "Final file not found before upload")
             if task_manager: task_manager._save_task(task)
        return

    file_size = os.path.getsize(file_path)
    filename = os.path.basename(file_path)
    progress_msg_id = task.message_id if task else None    # Use a generic upload message if the specific one wasn't set or failed
    if progress_msg_id:
        try:
            await client.edit_message(chat_id, progress_msg_id, f"📤 Uploading: {filename} ({format_size(file_size)})...")
            # Make sure we track this message if we're using it for progress updates
            if task:
                task.add_temp_message(progress_msg_id)
                # Schedule for auto-deletion after upload completes
                from utils.message_tracker import message_tracker
                await message_tracker.schedule_deletion(client, task, progress_msg_id, 60)  # Auto-delete after 1 minute
        except Exception:
            logger.warning(f"Task {task.id if task else 'N/A'}: Failed to edit message to 'Uploading'.")
            progress_msg_id = None # Prevent trying to delete it later if edit failed

    upload_progress_callback = None
    last_upload_update_time = 0

    async def _upload_progress(current, total):
        nonlocal upload_progress_callback  # Moved nonlocal declaration to the top
        now = time.time()
        if now - last_upload_update_time > 3.0: # Update every 3 seconds
            percent = round((current / total) * 100, 1)
            if progress_msg_id:
                try:
                    await client.edit_message(
                        chat_id, progress_msg_id,
                        f"📤 Uploading: {filename}\n"
                        f"Progress: {percent}% ({format_size(current)}/{format_size(total)})"
                    )
                    last_upload_update_time = now
                except FloodWaitError as fwe:
                    logger.warning(f"Upload progress flood wait: {fwe.seconds}s")
                    await asyncio.sleep(fwe.seconds + 1)
                except Exception as e:
                    logger.warning(f"Failed to update upload progress: {e}")
                    upload_progress_callback = None
            else:
                upload_progress_callback = None

    upload_progress_callback = _upload_progress

    try:
        logger.info(f"Task {task.id if task else 'N/A'}: Sending {file_path} ({format_size(file_size)}) to chat {chat_id}")
        await client.send_file(
            chat_id,
            file_path,
            caption=f"`{filename}`",
            progress_callback=upload_progress_callback,
            supports_streaming=True
        )
        logger.info(f"Task {task.id if task else 'N/A'}: Successfully sent {filename} to chat {chat_id}")

        # Delete the progress message AFTER successful upload
        if progress_msg_id:
            try:
                await client.delete_messages(chat_id, progress_msg_id)
                logger.debug(f"Task {task.id if task else 'N/A'}: Deleted progress message {progress_msg_id}")
                if task and progress_msg_id in task.temp_message_ids:
                    task.temp_message_ids.remove(progress_msg_id)
                    if task_manager: task_manager._save_task(task)
            except Exception as e:
                logger.warning(f"Task {task.id if task else 'N/A'}: Failed to delete progress message {progress_msg_id}: {e}")

    except FloodWaitError as fwe:
         logger.error(f"Flood wait error during file upload: {fwe.seconds} seconds. Task {task.id if task else 'N/A'}")
         if progress_msg_id:
             try: await client.edit_message(chat_id, progress_msg_id, f"⏳ Upload delayed due to Telegram limits. Please wait...")
             except Exception: pass
         if task and task_manager:
             task.update_status(TaskStatus.FAILED, f"Upload failed due to flood wait ({fwe.seconds}s)")
             task_manager._save_task(task)

    except Exception as e:
        logger.error(f"Failed to send file {filename} for task {task.id if task else 'N/A'}: {e}", exc_info=True)
        if progress_msg_id:
            try:
                await client.edit_message(chat_id, progress_msg_id, f"❌ Upload Failed: An error occurred while sending '{filename}'.")
            except Exception: pass
        if task and task_manager:
            task.update_status(TaskStatus.FAILED, f"Upload failed: {str(e)}")
            task_manager._save_task(task)

    finally:
        if os.path.exists(file_path):
            try:
                if os.path.isdir(file_path):
                     shutil.rmtree(file_path)
                else:
                     os.remove(file_path)
                logger.info(f"Task {task.id if task else 'N/A'}: Cleaned up uploaded file: {file_path}")
            except Exception as e:
                logger.warning(f"Task {task.id if task else 'N/A'}: Failed to clean up file {file_path}: {e}")


async def upload_zip_parts(client, chat_id, part_paths, task=None, task_manager=None):
    """
    Uploads a list of zip part files to Telegram, one by one, handling progress and cleanup.
    Returns True if all parts uploaded successfully, False otherwise.
    """
    success = True
    for idx, (part_path, part_size) in enumerate(part_paths, start=1):
        if not os.path.exists(part_path):
            logger.error(f"ZIP part not found: {part_path}")
            success = False
            continue
        part_name = os.path.basename(part_path)
        caption = f"📦 Part {idx}/{len(part_paths)}: `{part_name}`\nSize: {format_size(part_size)}"
        try:
            await client.send_file(
                chat_id,
                part_path,
                caption=caption,
                force_document=True
            )
            logger.info(f"Uploaded ZIP part {idx}/{len(part_paths)}: {part_name}")
            # Optionally delete part after upload
            try:
                os.remove(part_path)
                logger.debug(f"Deleted uploaded ZIP part: {part_path}")
            except Exception as e:
                logger.warning(f"Failed to delete ZIP part {part_path}: {e}")
        except Exception as e:
            logger.error(f"Failed to upload ZIP part {part_name}: {e}")
            success = False
    return success


async def clean_user_directory(chat_id: int):
    """Clean up a user's entire download directory. Use with caution!"""
    user_dir = os.path.join(DOWNLOAD_DIR, str(chat_id))
    logger.warning(f"Attempting to clean entire directory: {user_dir}")

    if not os.path.exists(user_dir):
        logger.info(f"User directory {user_dir} does not exist, nothing to clean.")
        return

    try:
        shutil.rmtree(user_dir)
        logger.info(f"Successfully cleaned user directory: {user_dir}")
    except Exception as e:
        logger.error(f"Error cleaning user directory {user_dir}: {e}")


async def delete_temp_messages(client, task: Task, task_manager):
    """Deletes temporary messages associated with a task, typically on failure or cancellation."""
    if not task or not task.temp_message_ids:
        return

    logger.debug(f"Task {task.id}: Attempting to delete temporary messages: {task.temp_message_ids}")
    message_ids_to_delete = list(task.temp_message_ids)

    for msg_id in message_ids_to_delete:
        try:
            await client.delete_messages(task.chat_id, msg_id)
            logger.debug(f"Task {task.id}: Deleted temporary message {msg_id}")
            if msg_id in task.temp_message_ids:
                 task.temp_message_ids.remove(msg_id)
        except Exception as e:
            logger.warning(f"Task {task.id}: Failed to delete temporary message {msg_id}: {e}")
            if msg_id in task.temp_message_ids:
                 task.temp_message_ids.remove(msg_id)

    if len(message_ids_to_delete) > 0:
         task_manager._save_task(task)


async def send_tracked_message(client, chat_id: int, text: str, task: Optional[Task], parse_mode=None, buttons=None, auto_delete=None, **kwargs):
    """
    Sends a message and, if a task is provided, tracks its ID for later deletion.
    
    Args:
        client: The Telegram client
        chat_id: The chat ID to send the message to
        text: The message text
        task: The task to track the message with
        parse_mode: Telegram parse mode
        buttons: Message buttons
        auto_delete: If set, number of seconds after which the message should be automatically deleted
        **kwargs: Additional arguments for client.send_message
    """
    message = None
    try:
        message = await client.send_message(chat_id, text, parse_mode=parse_mode, buttons=buttons, **kwargs)
        if task and message:
            task.add_temp_message(message.id)
            
            # If auto_delete is set, schedule message for deletion
            if auto_delete is not None:
                from utils.message_tracker import message_tracker
                await message_tracker.schedule_deletion(client, task, message.id, auto_delete)
                logger.debug(f"Set message {message.id} to auto-delete after {auto_delete}s for task {task.id}")
    except Exception as e:
        logger.error(f"Failed to send/track message for task {task.id if task else 'N/A'} in chat {chat_id}: {e}")
    return message


async def delete_tracked_messages(client, task: Task, specific_ids: Optional[List[int]] = None):
    """
    Deletes messages tracked in a task.
    If specific_ids are provided, only those are deleted from chat and removed from task tracking.
    Otherwise, all messages currently in task.temp_message_ids are deleted and the list is cleared.
    """
    if not task:
        logger.warning("delete_tracked_messages_for_task called with no task.")
        return

    ids_to_delete_from_chat = []
    if specific_ids:
        # Filter out IDs not actually in the task's list to avoid errors if called with arbitrary IDs
        ids_to_delete_from_chat = [msg_id for msg_id in specific_ids if msg_id in task.temp_message_ids]
        # Remove only the specified (and existing) IDs from tracking
        for msg_id in ids_to_delete_from_chat:
            if msg_id in task.temp_message_ids: # Double check before removing
                task.temp_message_ids.remove(msg_id)
    else:
        ids_to_delete_from_chat = list(task.temp_message_ids) # Copy list for deletion
        task.temp_message_ids.clear() # Clear all from tracking

    if ids_to_delete_from_chat:
        try:
            await client.delete_messages(task.chat_id, ids_to_delete_from_chat)
            logger.info(f"Task {task.id}: Deleted messages: {ids_to_delete_from_chat} from chat {task.chat_id}")
        except Exception as e:
            # Log warning but don't re-add to task.temp_message_ids as they are meant to be deleted.
            logger.warning(f"Task {task.id}: Failed to delete messages {ids_to_delete_from_chat} from chat {task.chat_id}: {e}")
