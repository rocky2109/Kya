import os
import logging
from typing import List, Tuple, Optional
from utils.improved_upload import upload_zip_parts_improved, handle_massive_file_upload
from utils.formatting import format_size
from config import MASSIVE_FILE_THRESHOLD, ZIP_PART_SIZE, MASSIVE_FILE_PART_SIZE

logger = logging.getLogger(__name__)

async def smart_upload_handler(client, chat_id, files_or_parts, task=None, task_manager=None, is_zip_parts=True):
    """
    Intelligent upload handler that chooses the best upload method based on file size and type.
    
    Args:
        client: Telegram client
        chat_id: Chat ID to upload to
        files_or_parts: List of (file_path, file_size) tuples OR single file path
        task: Task object for progress tracking
        task_manager: Task manager instance
        is_zip_parts: True if files_or_parts contains pre-split ZIP parts, False for single file
    
    Returns:
        Tuple[bool, str]: (success, message)
    """
    
    if isinstance(files_or_parts, str):
        # Single file path provided
        file_path = files_or_parts
        if not os.path.exists(file_path):
            return False, "File not found"
        
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        
        logger.info(f"Smart upload handler: Single file {file_name} ({format_size(file_size)})")
        
        # Check if this is a massive file that needs special handling
        if file_size > MASSIVE_FILE_THRESHOLD:
            logger.info(f"File {file_name} exceeds massive file threshold ({format_size(MASSIVE_FILE_THRESHOLD)}), using massive file handler")
            return await handle_massive_file_upload(client, chat_id, file_path, task, task_manager)
        
        # For smaller files, convert to part format and use standard handler
        part_paths = [(file_path, file_size)]
        return await upload_zip_parts_improved(client, chat_id, part_paths, task, task_manager)
    
    elif isinstance(files_or_parts, list):
        # List of parts or files
        if not files_or_parts:
            return False, "No files provided"
        
        # Calculate total size
        total_size = sum(size for _, size in files_or_parts)
        num_parts = len(files_or_parts)
        
        logger.info(f"Smart upload handler: {num_parts} parts, total size {format_size(total_size)}")
        
        # Check if we have extremely large parts that might cause issues
        largest_part = max(size for _, size in files_or_parts)
        
        if largest_part > ZIP_PART_SIZE * 1.2:  # 20% larger than normal part size
            logger.warning(f"Detected oversized part: {format_size(largest_part)}. This might cause upload issues.")
            
            # Send warning message
            try:
                warning_msg = await client.send_message(
                    chat_id,
                    f"⚠️ Warning: Detected oversized ZIP part\n"
                    f"📦 Largest part: {format_size(largest_part)}\n"
                    f"🎯 Recommended: {format_size(ZIP_PART_SIZE)}\n"
                    f"🔄 Upload will proceed but may be slower\n\n"
                    f"⚡Powered by @ZakulikaCompressor_bot"
                )
                if task:
                    task.add_temp_message(warning_msg.id)
                
                import asyncio
                await asyncio.sleep(3)
                await warning_msg.delete()
            except Exception as e:
                logger.warning(f"Failed to send oversized part warning: {e}")
        
        # Check if total size suggests this might be challenging
        if total_size > 50 * 1024**3:  # >50GB total
            logger.info(f"Large dataset detected ({format_size(total_size)}), enabling enhanced stability features")
            
            # Send preparation message
            try:
                prep_msg = await client.send_message(
                    chat_id,
                    f"🚀 Preparing large dataset upload...\n"
                    f"📦 {num_parts} parts\n"
                    f"💾 Total: {format_size(total_size)}\n"
                    f"🛡️ Enhanced stability mode enabled\n"
                    f"⏱️ This will take significant time\n\n"
                    f"⚡Powered by @ZakulikaCompressor_bot"
                )
                if task:
                    task.add_temp_message(prep_msg.id)
                
                import asyncio
                await asyncio.sleep(5)
                await prep_msg.delete()
            except Exception as e:
                logger.warning(f"Failed to send large dataset message: {e}")
        
        # Use the improved upload handler for ZIP parts
        return await upload_zip_parts_improved(client, chat_id, files_or_parts, task, task_manager)
    
    else:
        return False, "Invalid input format"


async def estimate_upload_time(file_size_bytes, connection_speed_mbps=5.0):
    """
    Estimate upload time based on file size and connection speed.
    
    Args:
        file_size_bytes: Size of file in bytes
        connection_speed_mbps: Connection speed in Mbps (default 5 Mbps)
    
    Returns:
        Tuple[int, str]: (seconds, human_readable_time)
    """
    # Convert Mbps to bytes per second
    speed_bps = (connection_speed_mbps * 1024 * 1024) / 8
    
    # Add overhead for Telegram's processing, retries, etc.
    overhead_factor = 1.5
    
    estimated_seconds = int((file_size_bytes / speed_bps) * overhead_factor)
    
    # Convert to human readable format
    if estimated_seconds < 60:
        time_str = f"{estimated_seconds}s"
    elif estimated_seconds < 3600:
        minutes = estimated_seconds // 60
        seconds = estimated_seconds % 60
        time_str = f"{minutes}m {seconds}s"
    else:
        hours = estimated_seconds // 3600
        minutes = (estimated_seconds % 3600) // 60
        time_str = f"{hours}h {minutes}m"
    
    return estimated_seconds, time_str


def optimize_part_size_for_file(total_file_size):
    """
    Determine optimal part size based on total file size to minimize upload issues.
    
    Args:
        total_file_size: Total size of the file to be split
    
    Returns:
        int: Optimal part size in bytes
    """
    if total_file_size > 100 * 1024**3:  # >100GB
        return MASSIVE_FILE_PART_SIZE  # 1.5GB parts
    elif total_file_size > 50 * 1024**3:  # >50GB
        return int(1.6 * 1024**3)  # 1.6GB parts
    elif total_file_size > 20 * 1024**3:  # >20GB
        return int(1.7 * 1024**3)  # 1.7GB parts
    elif total_file_size > 10 * 1024**3:  # >10GB
        return int(1.8 * 1024**3)  # 1.8GB parts
    else:
        return ZIP_PART_SIZE  # Standard 1.8GB parts


def should_use_massive_handler(file_size):
    """
    Determine if a file should use the massive file handler.
    
    Args:
        file_size: Size of file in bytes
    
    Returns:
        bool: True if massive handler should be used
    """
    return file_size > MASSIVE_FILE_THRESHOLD


async def send_upload_estimate(client, chat_id, total_size, num_parts=1):
    """
    Send an upload time estimate to the user.
    
    Args:
        client: Telegram client
        chat_id: Chat ID
        total_size: Total size in bytes
        num_parts: Number of parts to upload
    """
    try:
        # Estimate for different connection speeds
        slow_time = await estimate_upload_time(total_size, 2.0)  # 2 Mbps
        medium_time = await estimate_upload_time(total_size, 5.0)  # 5 Mbps
        fast_time = await estimate_upload_time(total_size, 10.0)  # 10 Mbps
        
        estimate_msg = await client.send_message(
            chat_id,
            f"📊 Upload Time Estimates\n"
            f"💾 Size: {format_size(total_size)}\n"
            f"📦 Parts: {num_parts}\n\n"
            f"🐌 Slow (2 Mbps): ~{slow_time[1]}\n"
            f"🚀 Medium (5 Mbps): ~{medium_time[1]}\n"
            f"⚡ Fast (10 Mbps): ~{fast_time[1]}\n\n"
            f"ℹ️ Actual time may vary based on connection\n"
            f"⚡Powered by @ZakulikaCompressor_bot"
        )
        
        import asyncio
        await asyncio.sleep(8)
        await estimate_msg.delete()
        
    except Exception as e:
        logger.warning(f"Failed to send upload estimate: {e}")
