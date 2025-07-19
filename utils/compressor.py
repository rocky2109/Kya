import os
import time
import logging
import asyncio
import shutil
import zipstream_ng as zipstream
import gc
from typing import List, Tuple, Optional

# Assuming config and utils.formatting are in the parent directory or accessible
from config import TEMP_DIR
from utils.formatting import format_size

# Assuming bot_client.client and task_manager are initialized elsewhere and passed or imported
# from bot_client import client # Example import
# from task_manager import task_manager # Example import

logger = logging.getLogger(__name__)

async def stream_compress(file_paths: List[str], zip_name: str, max_part_size: int, chat_id: int, task=None, client=None, task_manager=None) -> Tuple[List[Tuple[str, int]], Optional[str]]:
    """Stream compress files to disk with progress tracking using improved memory management"""
    part_number = 1
    current_size = 0
    total_size = sum(os.path.getsize(path) for path in file_paths if os.path.exists(path))
    processed_size = 0
    last_update_time = time.time()

    # Use the configured TEMP_DIR
    temp_dir = os.path.join(TEMP_DIR, f"compress_{chat_id}_{int(time.time())}")
    os.makedirs(temp_dir, exist_ok=True)
    multipart_generated = False

    part_paths = [] # List of (path, size) tuples
    current_part_path = None
    current_zipstream = None
    current_zipfile = None

    # Better file description for progress messages
    file_list = [os.path.basename(p) for p in file_paths]
    file_str = ", ".join(file_list) if len(file_list) <= 3 else f"{len(file_list)} files"
    progress_msg = None

    try:
        # Get or create progress message
        if task and task.message_id and client:
            try:
                progress_msg = await client.get_messages(chat_id, ids=task.message_id)
            except Exception as e:
                logger.warning(f"Failed to get progress message {task.message_id}: {e}")
                progress_msg = None # Proceed without editing if message not found

        if not progress_msg and client:
            try:
                progress_msg = await client.send_message(
                    chat_id,
                    f"🔄 Creating ZIP for {file_str}...\nPreparing compression...\n\n⚡Powered by @ZakulikaCompressor_bot"
                )
                if task and task_manager:
                    task_manager.update_task(task.id, message_id=progress_msg.id)
                    task.add_temp_message(progress_msg.id) # Track for cleanup
            except Exception as e:
                logger.error(f"Failed to send initial progress message: {e}")
                # Continue without progress updates if sending fails

        def start_new_part():
            nonlocal part_number, current_part_path, current_zipstream, current_zipfile, multipart_generated
            if current_zipfile:
                current_zipfile.close() # Close previous part file

            part_suffix = f".part{part_number:03d}.zip" if multipart_generated or total_size > max_part_size else ".zip"
            current_part_path = os.path.join(temp_dir, f"{zip_name}{part_suffix}")
            current_zipfile = open(current_part_path, 'wb')
            current_zipstream = zipstream.ZipFile(mode='w', compression=zipstream.ZIP_DEFLATED, allowZip64=True)
            part_number += 1
            return current_zipstream, current_zipfile, current_part_path

        # Start the first part
        current_zipstream, current_zipfile, current_part_path = start_new_part()
        last_progress_text = ""

        for file_path in file_paths:
            if not os.path.exists(file_path):
                logger.warning(f"File not found during compression: {file_path}")
                continue

            file_size = os.path.getsize(file_path)
            arcname = os.path.basename(file_path) # Use relative path within zip

            # Check if adding this file exceeds max part size
            # Estimate header/metadata size per file (can vary, ~50-100 bytes typical)
            estimated_overhead = 100
            if current_size > 0 and current_size + file_size + estimated_overhead > max_part_size:
                multipart_generated = True
                part_paths.append((current_part_path, current_size)) # Record completed part
                logger.info(f"Completed ZIP part: {os.path.basename(current_part_path)} Size: {format_size(current_size)}")
                current_zipstream, current_zipfile, current_part_path = start_new_part()
                current_size = 0 # Reset size for new part

            # Add file to current zipstream part
            try:
                # Use the instance method properly
                current_zipstream.write(file_path, arcname=arcname)
                for chunk in current_zipstream:
                    current_zipfile.write(chunk)
                    chunk_len = len(chunk)
                    current_size += chunk_len
                    processed_size += chunk_len
                    bytes_written_for_file = processed_size

                    # Update progress periodically
                    now = time.time()
                    if now - last_update_time > 2.5: # Update every 2.5 seconds
                        if progress_msg and client:
                            percent = (processed_size / total_size) * 100 if total_size > 0 else 0
                            progress_text = (
                                f"🔄 Compressing: {file_str}\n"
                                f"📦 Part: {part_number - 1}, File: {arcname[:20]}...\n"
                                f"📊 Progress: {percent:.1f}% ({format_size(processed_size)} / {format_size(total_size)})"
                                f"\n\n⚡Powered by @ZakulikaCompressor_bot"
                            )
                            if progress_text != last_progress_text:
                                try:
                                    await client.edit_message(chat_id, progress_msg.id, progress_text)
                                    last_progress_text = progress_text
                                except Exception as e:
                                    # Ignore flood waits or message not modified
                                    if "Message not modified" not in str(e) and "FloodWait" not in str(e):
                                         logger.warning(f"Failed to edit compression progress: {e}")
                        last_update_time = now
                        await asyncio.sleep(0.01) # Yield control briefly

                logger.debug(f"Added {arcname} ({format_size(bytes_written_for_file)}) to part {part_number - 1}")

            except Exception as e:
                logger.error(f"Error adding file {file_path} to zip: {e}")
                # Optionally: Decide whether to skip the file or abort

        # Finalize the last part
        if current_zipfile:
            current_zipfile.close()
        if current_part_path and current_size > 0:
             part_paths.append((current_part_path, current_size))
             logger.info(f"Completed final ZIP part: {os.path.basename(current_part_path)} Size: {format_size(current_size)}")
        elif current_part_path and current_size == 0:
             # Remove empty last part file if created
             try:
                 os.remove(current_part_path)
                 logger.info(f"Removed empty final part file: {os.path.basename(current_part_path)}")
             except OSError as e:
                 logger.warning(f"Could not remove empty part file {current_part_path}: {e}")

        # Final progress update
        if progress_msg and client:
            final_text = (
                f"✅ Compression complete for {file_str}!\n"
                f"📦 Total Parts: {len(part_paths)}\n"
                f"💾 Total Size: {format_size(processed_size)}"
                f"\n\n⚡Powered by @ZakulikaCompressor_bot"
            )
            try:
                await client.edit_message(chat_id, progress_msg.id, final_text)
            except Exception as e:
                logger.warning(f"Failed to send final compression message: {e}")

        gc.collect() # Explicitly run garbage collection
        return part_paths, temp_dir

    except Exception as e:
        logger.error(f"Error during stream compression: {e}", exc_info=True)
        # Cleanup partially created files and temp dir
        if current_zipfile and not current_zipfile.closed:
            current_zipfile.close()
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temp directory {temp_dir}: {cleanup_error}")
        # Send error message if possible
        if progress_msg and client:
             try:
                 await client.edit_message(chat_id, progress_msg.id, f"❌ Compression failed: {e}")
             except Exception: pass # Ignore errors editing message here

        return [], None # Return empty list and no temp_dir on failure
