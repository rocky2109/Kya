import os
import logging
from typing import List, Tuple, Optional, Union
from utils.smart_upload import smart_upload_handler, should_use_massive_handler, send_upload_estimate
from utils.formatting import format_size

logger = logging.getLogger(__name__)

async def handle_large_file_upload(client, chat_id, file_path, task=None, task_manager=None):
    """
    Handle upload of large files with intelligent method selection.
    This is the main entry point for uploading any large file.
    
    Args:
        client: Telegram client
        chat_id: Chat ID to upload to
        file_path: Path to the file to upload
        task: Task object for progress tracking
        task_manager: Task manager instance
    
    Returns:
        Tuple[bool, str]: (success, message)
    """
    
    if not os.path.exists(file_path):
        logger.error(f"File not found for upload: {file_path}")
        return False, "File not found"
    
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    
    logger.info(f"Handling large file upload: {file_name} ({format_size(file_size)})")
    
    # Send upload time estimate for very large files
    if file_size > 5 * 1024**3:  # >5GB
        await send_upload_estimate(client, chat_id, file_size, 1)
    
    # Use smart upload handler to choose the best method
    return await smart_upload_handler(client, chat_id, file_path, task, task_manager, is_zip_parts=False)


async def handle_zip_parts_upload(client, chat_id, part_paths, task=None, task_manager=None):
    """
    Handle upload of ZIP parts with intelligent optimization.
    
    Args:
        client: Telegram client
        chat_id: Chat ID to upload to
        part_paths: List of (part_path, part_size) tuples
        task: Task object for progress tracking
        task_manager: Task manager instance
    
    Returns:
        bool: True if all parts uploaded successfully
    """
    
    if not part_paths:
        logger.error("No ZIP parts provided for upload")
        return False
    
    total_size = sum(size for _, size in part_paths)
    logger.info(f"Handling ZIP parts upload: {len(part_paths)} parts, total {format_size(total_size)}")
    
    # Send upload time estimate for large datasets
    if total_size > 10 * 1024**3:  # >10GB
        await send_upload_estimate(client, chat_id, total_size, len(part_paths))
    
    # Use smart upload handler
    success, message = await smart_upload_handler(client, chat_id, part_paths, task, task_manager, is_zip_parts=True)
    return success


def get_recommended_part_size(total_file_size):
    """
    Get recommended part size for splitting large files.
    
    Args:
        total_file_size: Total size of the file in bytes
    
    Returns:
        int: Recommended part size in bytes
    """
    from utils.smart_upload import optimize_part_size_for_file
    return optimize_part_size_for_file(total_file_size)


async def upload_with_fallback(client, chat_id, files_or_parts, task=None, task_manager=None):
    """
    Upload with automatic fallback to different methods if the primary method fails.
    
    Args:
        client: Telegram client
        chat_id: Chat ID to upload to
        files_or_parts: Either a single file path or list of (path, size) tuples
        task: Task object for progress tracking
        task_manager: Task manager instance
    
    Returns:
        Tuple[bool, str]: (success, message)
    """
    
    try:
        # Primary attempt with smart upload handler
        success, message = await smart_upload_handler(client, chat_id, files_or_parts, task, task_manager)
        
        if success:
            return True, message
        
        logger.warning(f"Primary upload method failed: {message}")
        
        # If it's a single large file and the smart handler failed, try the massive file handler
        if isinstance(files_or_parts, str):
            file_path = files_or_parts
            file_size = os.path.getsize(file_path)
            
            if should_use_massive_handler(file_size):
                logger.info("Attempting fallback with massive file handler")
                from utils.improved_upload import handle_massive_file_upload
                return await handle_massive_file_upload(client, chat_id, file_path, task, task_manager)
        
        # If all methods fail, return the original error
        return False, f"All upload methods failed: {message}"
        
    except Exception as e:
        logger.error(f"Error in upload_with_fallback: {e}", exc_info=True)
        return False, f"Upload failed with error: {e}"


# Compatibility function to replace the old upload_zip_parts function
async def upload_zip_parts_enhanced(client, chat_id, part_paths, task=None, task_manager=None):
    """
    Enhanced version of upload_zip_parts with better error handling and stability.
    This function can be used as a drop-in replacement for the old upload_zip_parts.
    
    Args:
        client: Telegram client
        chat_id: Chat ID to upload to
        part_paths: List of (part_path, part_size) tuples
        task: Task object for progress tracking
        task_manager: Task manager instance
    
    Returns:
        bool: True if all parts uploaded successfully
    """
    return await handle_zip_parts_upload(client, chat_id, part_paths, task, task_manager)
