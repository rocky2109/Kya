import os
import time
import logging
import asyncio
import shutil
import gc
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)

# Try to import zipstream package, fallback to zipfile if not available
try:
    import zipstream
    HAS_ZIPSTREAM = True
    
    # Try to determine which zipstream version we have
    if hasattr(zipstream, '__version__'):
        logger.info(f"zipstream version: {zipstream.__version__}")
    
    # Test zipstream initialization to detect API
    try:
        if hasattr(zipstream, 'ZipFile'):
            test_zip = zipstream.ZipFile()
            ZIPSTREAM_CLASS = 'ZipFile'
        elif hasattr(zipstream, 'ZipStream'):
            test_zip = zipstream.ZipStream()
            ZIPSTREAM_CLASS = 'ZipStream'
        else:
            raise ImportError("No ZipFile or ZipStream class found")
            
        test_zip = None  # Clean up test object
        ZIPSTREAM_INIT_METHOD = "default"
        logger.info(f"zipstream.{ZIPSTREAM_CLASS}() works with default constructor")
    except Exception as e:
        logger.warning(f"zipstream initialization failed: {e}")
        ZIPSTREAM_INIT_METHOD = "fallback"
        ZIPSTREAM_CLASS = None
        
except ImportError:
    import zipfile
    HAS_ZIPSTREAM = False
    ZIPSTREAM_INIT_METHOD = None
    ZIPSTREAM_CLASS = None
    zipfile = zipfile  # Ensure zipfile is available for fallback

if not HAS_ZIPSTREAM:
    logger.warning("zipstream not available, using standard zipfile (memory intensive for large files)")

# Always import zipfile for fallback
if 'zipfile' not in locals():
    import zipfile

# Assuming config and utils.formatting are in the parent directory or accessible
from config import TEMP_DIR
from utils.formatting import format_size

# Assuming bot_client.client and task_manager are initialized elsewhere and passed or imported
# from bot_client import client # Example import
# from task_manager import task_manager # Example import

logger = logging.getLogger(__name__)

async def stream_compress(file_paths: List[str], zip_name: str, max_part_size: int, chat_id: int, task=None, client=None, task_manager=None) -> Tuple[List[Tuple[str, int]], Optional[str]]:
    """Stream compress files to disk with progress tracking - robust implementation"""
    
    if not HAS_ZIPSTREAM:
        # Fallback to simple zipfile compression
        return await _fallback_compress(file_paths, zip_name, max_part_size, chat_id, task, client, task_manager)
    
    # Validate inputs
    if not file_paths:
        logger.error("No file paths provided for compression")
        return [], "No files to compress"
        
    # Check if all files exist and flatten directory structures
    valid_files = []
    for file_path in file_paths:
        if os.path.isfile(file_path):
            valid_files.append(file_path)
        elif os.path.isdir(file_path):
            # Walk through directory and add all files
            for root, dirs, files in os.walk(file_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    valid_files.append(full_path)
        else:
            logger.warning(f"Path not found, skipping: {file_path}")
    
    if not valid_files:
        logger.error("No valid files found for compression")
        return [], "No valid files to compress"
    
    # Calculate total size
    total_size = 0
    for file_path in valid_files:
        try:
            total_size += os.path.getsize(file_path)
        except OSError as e:
            logger.warning(f"Could not get size for {file_path}: {e}")
    
    part_number = 1
    processed_size = 0
    last_update_time = time.time()
    multipart_generated = False

    # Use temporary directory
    temp_dir = os.path.join(TEMP_DIR, f"compress_{chat_id}_{int(time.time())}")
    os.makedirs(temp_dir, exist_ok=True)
    
    part_paths = []  # List of (path, size) tuples
    
    # Better file description for progress messages
    file_list = [os.path.basename(p) for p in valid_files[:3]]
    file_str = ", ".join(file_list) if len(file_list) <= 3 else f"{len(valid_files)} files"
    progress_msg = None

    try:
        # Get or create progress message
        if task and task.message_id and client:
            try:
                progress_msg = await client.get_messages(chat_id, ids=task.message_id)
            except Exception as e:
                logger.warning(f"Failed to get progress message {task.message_id}: {e}")
                progress_msg = None

        if not progress_msg and client:
            try:
                progress_msg = await client.send_message(
                    chat_id,
                    f"🔄 Creating ZIP for {file_str}...\nInitializing compression...\n\n⚡Powered by @ZakulikaCompressor_bot"
                )
                if task and task_manager:
                    task_manager.update_task(task.id, message_id=progress_msg.id)
                    task.add_temp_message(progress_msg.id)
            except Exception as e:
                logger.error(f"Failed to send initial progress message: {e}")

        # Create zipstream generator using the pre-tested method
        try:
            logger.info(f"zipstream module available: {HAS_ZIPSTREAM}, class: {ZIPSTREAM_CLASS}, init method: {ZIPSTREAM_INIT_METHOD}")
            
            if ZIPSTREAM_INIT_METHOD == "default" and ZIPSTREAM_CLASS:
                # Use the method we know works
                zipstream_class = getattr(zipstream, ZIPSTREAM_CLASS)
                z = zipstream_class()
                logger.info(f"Using zipstream.{ZIPSTREAM_CLASS}() with default constructor")
            else:
                # Try different approaches for compatibility
                z = None
                
                # Try ZipStream first (more common in zipstream-ng)
                if hasattr(zipstream, 'ZipStream'):
                    for init_args in [
                        {},  # No arguments
                        {'allowZip64': True},  # With allowZip64
                        {'compress_type': 8},  # With compress_type (ZIP_DEFLATED = 8)
                    ]:
                        try:
                            z = zipstream.ZipStream(**init_args)
                            logger.info(f"Using zipstream.ZipStream with args: {init_args}")
                            break
                        except TypeError as te:
                            logger.debug(f"zipstream.ZipStream failed with {init_args}: {te}")
                            continue
                        except Exception as e:
                            logger.debug(f"zipstream.ZipStream unexpected error with {init_args}: {e}")
                            continue
                
                # Try ZipFile if ZipStream failed
                if z is None and hasattr(zipstream, 'ZipFile'):
                    for init_args in [
                        {},  # No arguments
                        {'allowZip64': True},  # With allowZip64
                        {'compress_type': 8},  # With compress_type (ZIP_DEFLATED = 8)
                    ]:
                        try:
                            z = zipstream.ZipFile(**init_args)
                            logger.info(f"Using zipstream.ZipFile with args: {init_args}")
                            break
                        except TypeError as te:
                            logger.debug(f"zipstream.ZipFile failed with {init_args}: {te}")
                            continue
                        except Exception as e:
                            logger.debug(f"zipstream.ZipFile unexpected error with {init_args}: {e}")
                            continue
                
                if z is None:
                    raise ImportError("Could not initialize any zipstream class")
                    
        except Exception as e:
            logger.warning(f"Failed to create zipstream object: {e}")
            logger.info("Falling back to standard zipfile compression")
            # Fall back to zipfile
            return await _fallback_compress(file_paths, zip_name, max_part_size, chat_id, task, client, task_manager)
        
        # Add all files to the zipstream
        for file_path in valid_files:
            if os.path.isfile(file_path):
                # Use relative path for archive name
                if len(file_paths) == 1 and os.path.isdir(file_paths[0]):
                    # If compressing a single directory, preserve structure
                    base_dir = file_paths[0]
                    arcname = os.path.relpath(file_path, os.path.dirname(base_dir))
                else:
                    # For multiple files or single files, use basename
                    arcname = os.path.basename(file_path)
                
                # Try different methods to add files (zipstream-ng compatibility)
                try:
                    if hasattr(z, 'add'):
                        # zipstream-ng uses add() method
                        z.add(file_path, arcname=arcname)
                        logger.debug(f"Added {arcname} to zipstream using add()")
                    elif hasattr(z, 'write'):
                        # Standard zipstream uses write() method
                        z.write(file_path, arcname=arcname)
                        logger.debug(f"Added {arcname} to zipstream using write()")
                    elif hasattr(z, 'writestr'):
                        # Fallback: read file and use writestr
                        with open(file_path, 'rb') as f:
                            z.writestr(arcname, f.read())
                        logger.debug(f"Added {arcname} to zipstream using writestr()")
                    else:
                        raise AttributeError("No suitable method found to add files to zipstream")
                except Exception as e:
                    logger.warning(f"Failed to add {file_path} to zipstream: {e}")
                    # If adding files fails, fall back to standard compression
                    return await _fallback_compress(file_paths, zip_name, max_part_size, chat_id, task, client, task_manager)
        
        # Always start writing to a part file
        part_path = os.path.join(temp_dir, f"{zip_name}.zip.{part_number:03d}")
        current_size = 0
        
        # Add memory monitoring for large files
        import psutil
        initial_memory = psutil.virtual_memory().available
        memory_warning_threshold = 500 * 1024 * 1024  # 500MB minimum free memory
        
        with open(part_path, 'wb') as f:
            chunk_count = 0
            for chunk in z:
                f.write(chunk)
                current_size += len(chunk)
                processed_size += len(chunk)
                chunk_count += 1
                
                # Monitor memory usage for large files
                if chunk_count % 100 == 0:  # Check every 100 chunks
                    try:
                        available_memory = psutil.virtual_memory().available
                        if available_memory < memory_warning_threshold:
                            logger.warning(f"Low memory detected: {format_size(available_memory)} available")
                            # Force garbage collection
                            gc.collect()
                    except Exception:
                        pass  # Don't let memory monitoring break compression
                
                # Update progress periodically
                now = time.time()
                if now - last_update_time > 2.0:  # Update every 2 seconds
                    last_update_time = now
                    if progress_msg and client:
                        try:
                            percentage = (processed_size / total_size) * 100 if total_size else 0
                            bar_length = 20
                            filled_length = int(bar_length * (processed_size / max(total_size, 1)))
                            bar = '█' * filled_length + '░' * (bar_length - filled_length)
                            
                            progress_text = (
                                f"🔄 Creating ZIP archive...\n"
                                f"[{bar}] {percentage:.1f}%\n"
                                f"Size: {format_size(processed_size)}/{format_size(total_size)}\n"
                                f"Part {part_number:03d}\n\n⚡Powered by @ZakulikaCompressor_bot"
                            )
                            await client.edit_message(chat_id, progress_msg.id, progress_text)
                        except Exception as e:
                            if "Message not modified" not in str(e):
                                logger.warning(f"Failed to edit compression progress: {e}")
                
        # Check if we need to split into multiple parts
        if current_size >= max_part_size:
            multipart_generated = True
            part_paths.append((part_path, current_size))
            logger.info(f"Completed ZIP part {part_number}: {format_size(current_size)}")
            
            part_number += 1
            part_path = os.path.join(temp_dir, f"{zip_name}.zip.{part_number:03d}")
            current_size = 0
            f.close()
            f = open(part_path, 'wb')
            
            # Enhanced memory cleanup between parts for large files
            if current_size > 1024**3:  # If part was > 1GB, do aggressive cleanup
                gc.collect()
                gc.collect()  # Double collection for large parts
                logger.debug(f"Aggressive memory cleanup after large part {part_number-1}")
            else:
                gc.collect()  # Standard cleanup        # Handle final part
        if multipart_generated:
            # If we split into multiple parts, add the final part if it has content
            if current_size > 0:
                part_paths.append((part_path, current_size))
                logger.info(f"Completed final ZIP part {part_number}: {format_size(current_size)}")
        else:
            # If no splitting occurred, rename to remove the .001 extension
            single_part_path = os.path.join(temp_dir, f"{zip_name}.zip")
            try:
                os.rename(part_path, single_part_path)
                part_paths.append((single_part_path, current_size))
                logger.info(f"Created single ZIP file: {format_size(current_size)}")
            except OSError as e:
                logger.error(f"Failed to rename single part: {e}")
                part_paths.append((part_path, current_size))

        # Final progress update
        if progress_msg and client:
            try:
                total_parts = len(part_paths)
                final_text = (
                    f"✅ ZIP creation complete!\n"
                    f"Parts: {total_parts}\n"
                    f"Total size: {format_size(processed_size)}\n\n⚡Powered by @ZakulikaCompressor_bot"
                )
                await client.edit_message(chat_id, progress_msg.id, final_text)
                
                # Schedule message deletion after 3 seconds
                await asyncio.sleep(3)
                try:
                    await progress_msg.delete()
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"Failed to send final compression message: {e}")

        gc.collect()
        return part_paths, temp_dir

    except Exception as e:
        logger.error(f"Error during stream compression: {e}", exc_info=True)
        
        # Cleanup on error
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temp directory {temp_dir}: {cleanup_error}")
        
        # Send error message if possible
        if progress_msg and client:
            try:
                await client.edit_message(chat_id, progress_msg.id, f"❌ Compression failed: {e}")
            except Exception:
                pass

        return [], f"Compression failed: {e}"
async def _fallback_compress(file_paths: List[str], zip_name: str, max_part_size: int, chat_id: int, task=None, client=None, task_manager=None) -> Tuple[List[Tuple[str, int]], Optional[str]]:
    """Fallback compression using standard zipfile when zipstream is not available"""
    logger.info("Using fallback compression with standard zipfile")
    
    temp_dir = os.path.join(TEMP_DIR, f"compress_{chat_id}_{int(time.time())}")
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Flatten directory structures and collect all files
        all_files = []
        for file_path in file_paths:
            if os.path.isfile(file_path):
                all_files.append(file_path)
            elif os.path.isdir(file_path):
                for root, dirs, files in os.walk(file_path):
                    for file in files:
                        full_path = os.path.join(root, file)
                        all_files.append(full_path)
        
        if not all_files:
            return [], "No files found to compress"
        
        total_size = sum(os.path.getsize(path) for path in all_files if os.path.exists(path))
        
        # Send progress message
        progress_msg = None
        if task and task.message_id and client:
            try:
                progress_msg = await client.get_messages(chat_id, ids=task.message_id)
            except Exception:
                progress_msg = None
        
        if not progress_msg and client:
            try:
                file_list = [os.path.basename(p) for p in all_files[:3]]
                file_str = ", ".join(file_list) if len(file_list) <= 3 else f"{len(all_files)} files"
                progress_msg = await client.send_message(
                    chat_id,
                    f"🔄 Creating ZIP for {file_str}...\nUsing fallback compression...\n\n⚡Powered by @ZakulikaCompressor_bot"
                )
                if task and task_manager:
                    task_manager.update_task(task.id, message_id=progress_msg.id)
            except Exception as e:
                logger.error(f"Failed to send progress message: {e}")
        
        # Determine if we need multiple parts
        if total_size <= max_part_size:
            # Single ZIP file
            zip_path = os.path.join(temp_dir, f"{zip_name}.zip")
            processed = 0
            last_update = time.time()
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in all_files:
                    if os.path.exists(file_path):
                        # Create appropriate archive name
                        if len(file_paths) == 1 and os.path.isdir(file_paths[0]):
                            # Preserve directory structure
                            base_dir = file_paths[0]
                            arcname = os.path.relpath(file_path, os.path.dirname(base_dir))
                        else:
                            arcname = os.path.basename(file_path)
                        
                        zipf.write(file_path, arcname)
                        logger.debug(f"Added {arcname} to zip")
                        
                        # Update progress
                        processed += os.path.getsize(file_path)
                        now = time.time()
                        if progress_msg and client and (now - last_update > 2.0):  # Every 2 seconds
                            try:
                                percent = (processed / total_size) * 100
                                bar_length = 20
                                filled_length = int(bar_length * (processed / max(total_size, 1)))
                                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                                
                                await client.edit_message(
                                    chat_id,
                                    progress_msg.id,
                                    f"🔄 Compressing... (fallback mode)\n"
                                    f"[{bar}] {percent:.1f}%\n"
                                    f"Processed: {format_size(processed)}/{format_size(total_size)}\n\n"
                                    f"⚡Powered by @ZakulikaCompressor_bot"
                                )
                                last_update = now
                            except Exception as e:
                                if "Message not modified" not in str(e):
                                    logger.warning(f"Failed to update fallback progress: {e}")
            
            zip_size = os.path.getsize(zip_path)
            
            # Update progress message
            if progress_msg and client:
                try:
                    await client.edit_message(
                        chat_id, 
                        progress_msg.id, 
                        f"✅ Compression complete!\n💾 Size: {format_size(zip_size)}\n\n⚡Powered by @ZakulikaCompressor_bot"
                    )
                    await asyncio.sleep(3)
                    await progress_msg.delete()
                except Exception:
                    pass
            
            return [(zip_path, zip_size)], temp_dir
        else:
            # Multiple parts needed - use a simpler approach
            logger.warning("Large file set detected with zipfile fallback - this may use significant memory")
            
            # For now, create a single large file and let the upload handler split it
            zip_path = os.path.join(temp_dir, f"{zip_name}.zip")
            processed = 0
            last_update = time.time()
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in all_files:
                    if os.path.exists(file_path):
                        if len(file_paths) == 1 and os.path.isdir(file_paths[0]):
                            base_dir = file_paths[0]
                            arcname = os.path.relpath(file_path, os.path.dirname(base_dir))
                        else:
                            arcname = os.path.basename(file_path)
                        zipf.write(file_path, arcname)
                        processed += os.path.getsize(file_path)
                        
                        # Update progress every 2 seconds
                        now = time.time()
                        if progress_msg and client and (now - last_update > 2.0):
                            try:
                                percent = (processed / total_size) * 100
                                bar_length = 20
                                filled_length = int(bar_length * (processed / max(total_size, 1)))
                                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                                
                                await client.edit_message(
                                    chat_id,
                                    progress_msg.id,
                                    f"🔄 Compressing... (large dataset)\n"
                                    f"[{bar}] {percent:.1f}%\n"
                                    f"Processed: {format_size(processed)}/{format_size(total_size)}\n\n"
                                    f"⚡Powered by @ZakulikaCompressor_bot"
                                )
                                last_update = now
                            except Exception as e:
                                if "Message not modified" not in str(e):
                                    logger.warning(f"Failed to update large file progress: {e}")
            
            zip_size = os.path.getsize(zip_path)
            
            if progress_msg and client:
                try:
                    await client.edit_message(
                        chat_id,
                        progress_msg.id,
                        f"✅ Compression complete!\n💾 Size: {format_size(zip_size)}\n\n⚡Powered by @ZakulikaCompressor_bot"
                    )
                    await asyncio.sleep(3)
                    await progress_msg.delete()
                except Exception:
                    pass
            
            return [(zip_path, zip_size)], temp_dir
        
    except Exception as e:
        logger.error(f"Error during fallback compression: {e}")
        # Cleanup
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass
        return [], f"Fallback compression failed: {e}"
