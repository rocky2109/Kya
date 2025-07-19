import os
import time
import asyncio
import logging
import shutil
import psutil
import libtorrent as lt
from typing import Optional

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

logger = logging.getLogger(__name__)

# Helper to get libtorrent version (consider moving to utils if used elsewhere)
def get_libtorrent_version():
    """Get libtorrent version as a tuple"""
    try:
        version_str = lt.version
        parts = version_str.split('.')
        return tuple(int(p) for p in parts if p.isdigit())
    except Exception:
        return (0, 0, 0)

LT_VERSION = get_libtorrent_version()
logger.info(f"Using libtorrent version: {'.'.join(map(str, LT_VERSION))}")

# --- Libtorrent Session Context Manager ---

class LibtorrentSession:
    """Context manager for libtorrent session to ensure proper cleanup."""
    def __init__(self, settings=None):
        self.session = None
        self.settings = settings or {
            'user_agent': 'ZakulikaAIO/1.0 libtorrent/' + lt.version,
            'announce_to_all_tiers': True,
            'enable_dht': True,
            'enable_lsd': True,
            'enable_upnp': True,
            'enable_natpmp': True,
        }
        self.handles = set()

    async def __aenter__(self):
        """Start libtorrent session with settings."""
        if LT_VERSION >= (2, 0, 0):
            self.session = lt.session(self.settings)
        else:
            self.session = lt.session()
            self.session.apply_settings(self.settings)
        
        # Start session services
        self.session.start_dht()
        self.session.start_lsd()
        self.session.start_upnp()
        self.session.start_natpmp()
        logger.info("libtorrent session started with services.")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper cleanup of session resources."""
        if self.session:
            try:
                # Remove all tracked handles
                for handle in list(self.handles):
                    if handle and handle.is_valid():
                        try:
                            # Only delete files if we have an error
                            delete_files = exc_type is not None
                            self.session.remove_torrent(handle, delete_files)
                            self.handles.discard(handle)
                        except Exception as e:
                            logger.warning(f"Error removing torrent handle: {e}")

                # Stop session services
                try:
                    self.session.stop_dht()
                    self.session.stop_lsd()
                    self.session.stop_upnp()
                    self.session.stop_natpmp()
                except Exception as e:
                    logger.warning(f"Error stopping session services: {e}")

                # Clear session
                self.session = None
                logger.info("libtorrent session cleaned up successfully.")
            except Exception as e:
                logger.error(f"Error during libtorrent session cleanup: {e}")

    def add_torrent(self, params):
        """Add a torrent and track its handle."""
        handle = self.session.add_torrent(params)
        self.handles.add(handle)
        return handle

    def remove_torrent(self, handle, delete_files=False):
        """Remove a tracked torrent handle."""
        if handle in self.handles:
            self.session.remove_torrent(handle, delete_files)
            self.handles.discard(handle)
            return True
        return False

# --- Torrent Download Session Context Manager ---

class TorrentDownloadSession:
    """Context manager for torrent download sessions to ensure proper cleanup."""
    def __init__(self, task_id: str, chat_id: int, torrent_name: str):
        self.task_id = task_id
        self.chat_id = chat_id
        self.torrent_name = torrent_name
        self.user_dir = os.path.join(DOWNLOAD_DIR, str(chat_id))
        self.temp_files = set()
        self.handle = None
        self.download_path = None
        self.progress_msg = None

    async def __aenter__(self):
        """Setup download directory and resources."""
        os.makedirs(self.user_dir, exist_ok=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper cleanup of download resources."""
        # Handle cleanup is managed by LibtorrentSession
        
        # Clean up temporary files on error
        if exc_type or self.temp_files:
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
                                if attempt < 2:
                                    await asyncio.sleep(1 * (2 ** attempt))
                                    continue
                                raise
                    except Exception as e:
                        logger.warning(f"Task {self.task_id}: Failed to remove temp file {temp_file}: {e}")

            # Clean up download directory if it exists and is empty
            if self.download_path and os.path.exists(self.download_path):
                try:
                    if os.path.isdir(self.download_path) and not os.listdir(self.download_path):
                        os.rmdir(self.download_path)
                        logger.info(f"Task {self.task_id}: Removed empty download directory: {self.download_path}")
                except Exception as e:
                    logger.warning(f"Task {self.task_id}: Failed to remove download directory: {e}")

    def set_handle(self, handle):
        """Set the torrent handle for tracking."""
        self.handle = handle

    def set_progress_msg(self, msg):
        """Set the progress message for potential cleanup."""
        self.progress_msg = msg

    def set_download_path(self, path: str):
        """Set the download path for cleanup tracking."""
        self.download_path = path

    def add_temp_file(self, file_path: str):
        """Add a file to be cleaned up when the session ends."""
        self.temp_files.add(file_path)

# --- Status Tracker Context Manager ---

class StatusTracker:
    """Context manager for tracking torrent status and cleanup."""
    def __init__(self, handle, total_size):
        self.handle = handle
        self.total_size = total_size
        self.last_update_time = 0
        self.last_progress = 0
        self.last_downloaded = 0
        self.stall_start_time = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.handle and self.handle.is_valid():
                self.handle.pause()
                await asyncio.sleep(0.5)  # Give time for the pause to take effect
        except Exception as e:
            logger.warning(f"Error cleaning up torrent handle: {e}")

    def update(self, status) -> tuple[float, float, float, float]:
        """Update status tracking and return progress metrics."""
        try:
            current_downloaded = status.total_done
            current_progress = (current_downloaded / self.total_size) * 100 if self.total_size > 0 else 0
            download_rate = status.download_rate

            # Check for stalls
            if current_downloaded == self.last_downloaded and download_rate == 0:
                if self.stall_start_time is None:
                    self.stall_start_time = time.time()
            else:
                self.stall_start_time = None

            self.last_downloaded = current_downloaded
            self.last_progress = current_progress

            # Calculate ETA
            if download_rate > 0:
                remaining_bytes = self.total_size - current_downloaded
                eta = remaining_bytes / download_rate
            else:
                eta = float('inf')

            # Calculate stall duration
            stall_duration = time.time() - self.stall_start_time if self.stall_start_time else 0

            return current_progress, download_rate, eta, stall_duration

        except Exception as e:
            logger.error(f"Error updating torrent status: {e}")
            return 0.0, 0.0, float('inf'), 0.0

# --- Torrent Handlers ---

async def update_torrent_progress(client, chat_id, message_id, text, min_interval=2.5):
    """Update torrent progress message with rate limiting."""
    try:
        return await message_debouncer.update(client, chat_id, message_id, text, min_interval=min_interval)
    except Exception as e:
        logger.warning(f"Failed to update torrent progress: {e}")
        return False

async def handle_torrent(task_manager, client, user_stats_manager, file_path: str, chat_id: int, task: Optional[Task] = None):
    """
    Handle downloading from a .torrent file.
    Uses libtorrent for the download process.
    Uses the message_id stored in the task object for updates.
    """
    progress_msg_id = task.message_id if task else None # Get message ID from task
    torrent_name = "Unknown Torrent"

    if not progress_msg_id:
        logger.error(f"Task {task.id}: Cannot proceed with torrent download, initial progress message ID not found in task.")
        if task:
            task.update_status(TaskStatus.FAILED, "Internal error: Progress message ID missing")
            task_manager._save_task(task)
        # Clean up the downloaded .torrent file if it exists
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as rm_err:
                logger.warning(f"Task {task.id}: Failed to remove torrent file {file_path} after error: {rm_err}")
        return

    try:
        # --- Load Torrent Info First ---
        try:
            if LT_VERSION >= (2, 0, 0):
                info = lt.torrent_info(file_path)
            else:
                with open(file_path, 'rb') as f:
                    torrent_data = f.read()
                info = lt.torrent_info(lt.bdecode(torrent_data))

            total_size = info.total_size()
            torrent_name = info.name()
            num_files = info.num_files()
        except Exception as e:
            # Clean up the downloaded .torrent file if it exists
            if os.path.exists(file_path):
                try: os.remove(file_path)
                except Exception: pass
            raise ValueError(f"Invalid or corrupted torrent file: {e}")

        # --- Start Download Session ---
        async with TorrentDownloadSession(task.id if task else "N/A", chat_id, torrent_name) as session:
            session.add_temp_file(file_path)  # Mark .torrent file for cleanup
            logger.info(f"Task {task.id if task else 'N/A'}: Torrent '{torrent_name}', Size: {format_size(total_size)}, Files: {num_files}")

            # --- Check Disk Space ---
            try:
                disk_space = psutil.disk_usage(DOWNLOAD_DIR).free
                required_space = total_size * 1.01 + 100 * 1024 * 1024
                if disk_space < required_space:
                    raise ValueError(f"Insufficient disk space: {format_size(disk_space)} available, need ~{format_size(required_space)}")
            except Exception as e:
                logger.warning(f"Could not verify disk space: {e}")            # --- Update Initial Message (Edit) ---
            try:
                await client.edit_message(
                    chat_id, progress_msg_id,
                    f"⏳ Task {task.id}: Starting torrent download...\n"
                    f"📦 Name: {torrent_name}\n"
                    f"💾 Size: {format_size(total_size)}"
                )
                # Don't schedule auto-deletion for main progress message - only delete on success/failure
                if task:
                    task.add_temp_message(progress_msg_id)
            except Exception as e:
                 logger.warning(f"Task {task.id}: Failed to edit initial torrent message: {e}")
                 # Continue anyway

            async with LibtorrentSession() as ses:
                # --- Add Torrent to Session ---
                add_params = lt.add_torrent_params()
                add_params.ti = info
                add_params.save_path = session.user_dir
                add_params.storage_mode = lt.storage_mode_t.storage_mode_sparse

                handle = ses.add_torrent(add_params)
                session.set_handle(handle)
                logger.info(f"Task {task.id if task else 'N/A'}: Torrent added to session. Infohash: {handle.info_hash()}")

                # --- Download Loop with Status Tracking ---
                async with StatusTracker(handle, total_size) as tracker:
                    while not handle.status().is_seeding:
                        if task and task.status == TaskStatus.CANCELLED:
                            logger.info(f"Task {task.id}: Cancellation detected during torrent download.")
                            await update_torrent_progress(client, chat_id, progress_msg_id, f"❌ Task {task.id}: Download cancelled.", min_interval=0)
                            return

                        await asyncio.sleep(2)
                        s = handle.status()
                        state_str = ['queued', 'checking', 'downloading metadata', 'downloading',
                                   'finished', 'seeding', 'allocating', 'checking resume']
                        current_state = state_str[s.state] if s.state < len(state_str) else f"unknown ({s.state})"

                        percent, download_rate, eta, stalled_timer = tracker.update(s)
                        peers = s.num_peers
                        seeds = s.num_seeds
                        downloaded = s.total_done

                        if task and s.state != lt.torrent_status.states.checking_files:
                            task.update_progress(s.progress)

                        now = time.time()
                        update_interval = 3.0 if percent < 99 else 5.0
                        if now - tracker.last_update_time > update_interval:
                            bar = '█' * int(percent / 5) + '░' * (20 - int(percent / 5))
                            status_text = (
                                f"⬇️ Torrent: {torrent_name}\n"
                                f"Task ID: {task.id}\n"
                                f"📊 Status: {current_state}\n"
                                f"💾 Size: {format_size(total_size)}\n"
                                f"✅ Progress: {percent:.1f}% ({format_size(downloaded)}/{format_size(total_size)})\n"
                                f"⚡ Speed: {format_size(download_rate)}/s\n"
                                f"👥 Peers/Seeds: {peers}/{seeds}\n"
                                f"⏱️ ETA: {format_time(eta)}\n"
                                f"[{bar}]"
                            )

                            if stalled_timer > 60:
                                status_text += f"\n⚠️ Download stalled for {int(stalled_timer)}s. Waiting for peers..."
                            elif 0 < download_rate < 10240 and s.state == lt.torrent_status.states.downloading:
                                status_text += f"\n⚠️ Download speed is very low."

                            # Use message ID from task
                            updated = await update_torrent_progress(client, chat_id, progress_msg_id, status_text, min_interval=update_interval - 0.5)
                            if updated:
                                tracker.last_update_time = now

                        if stalled_timer > 15 * 60 and seeds == 0 and peers == 0:
                            raise TimeoutError("Download stalled for 15 minutes with no peers or seeds.")

                # --- Handle Completion ---
                logger.info(f"Task {task.id if task else 'N/A'}: Torrent download complete for '{torrent_name}'.")
                final_message = (
                    f"✅ Torrent download complete!\n"
                    f"📦 Name: {torrent_name}\n"
                    f"💾 Size: {format_size(total_size)}\n"
                    f"Task ID: {task.id}\n"
                    f"📤 Uploading..." # Changed message
                )
                # Use message ID from task
                await update_torrent_progress(client, chat_id, progress_msg_id, final_message, min_interval=0)

                # Set and verify download path
                download_path = os.path.join(session.user_dir, torrent_name)
                session.set_download_path(download_path)

                if not os.path.exists(download_path):
                    logger.error(f"Download path {download_path} does not exist after completion.")
                    raise FileNotFoundError(f"Downloaded content not found at expected path: {download_path}")

                if task:
                    task.result_path = download_path
                    task.update_status(TaskStatus.COMPLETED)
                    task_manager._save_task(task)
                
                user_stats_manager.record_completed_transfer(
                    chat_id, downloaded_size=total_size, task_type="torrent", success=True
                )

                # Send file to Telegram (will delete progress message on success)
                # Verify the download path exists before sending
                if not os.path.exists(download_path):
                    logger.error(f"Download path does not exist: {download_path}")
                    raise FileNotFoundError(f"Downloaded content not found at: {download_path}")
                
                logger.info(f"Task {task.id if task else 'N/A'}: Sending torrent content from {download_path}")
                await send_to_telegram(client, download_path, chat_id, task, task_manager)
                
                # Clean up any remaining tracked messages
                await delete_tracked_messages(client, task)

                return download_path

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Torrent download error for task {task.id if task else 'N/A'}: {error_msg}", exc_info=True)
        # Use message ID from task
        if progress_msg_id:
            try:
                await update_torrent_progress(client, chat_id, progress_msg_id, f"❌ Torrent download failed: {error_msg[:200]}", min_interval=0)
                # Schedule error message for deletion after 60 seconds
                if task:
                    await message_tracker.schedule_deletion(client, task, progress_msg_id, 60)
            except Exception:
                pass
        # else: Cannot update message if ID is missing

        if task:
            task.update_status(TaskStatus.FAILED, error_msg)
            task_manager._save_task(task)
            # Clean up all tracked messages
            await delete_tracked_messages(client, task)

        user_stats_manager.record_completed_transfer(chat_id, 0, task_type="torrent", success=False)
        # Clean up the downloaded .torrent file if it exists and wasn't cleaned by session
        if os.path.exists(file_path) and file_path not in session.temp_files:
             try: os.remove(file_path)
             except Exception: pass
        # Do not re-raise, let queue handler manage

    finally:
        # Temp message cleanup is handled by delete_temp_messages if task failed/cancelled
        # If successful, send_to_telegram handles the main progress message deletion
        pass # No specific action needed here now


async def handle_magnet(task_manager, client, user_stats_manager, magnet_link: str, chat_id: int, task: Optional[Task] = None):
    """
    Handle downloading from a magnet link.
    Uses libtorrent, similar to handle_torrent but starts with magnet URI.
    Uses the message_id stored in the task object for updates.
    """
    progress_msg_id = task.message_id if task else None # Get message ID from task
    torrent_name = "Unknown Torrent"

    if not progress_msg_id:
        logger.error(f"Task {task.id}: Cannot proceed with magnet download, initial progress message ID not found in task.")
        if task:
            task.update_status(TaskStatus.FAILED, "Internal error: Progress message ID missing")
            task_manager._save_task(task)
        return

    try:
        # --- Start Download Session ---
        async with TorrentDownloadSession(task.id if task else "N/A", chat_id, torrent_name) as session:
            # --- Update Initial Message (Edit) ---
            # The queue wrapper already sent "Processing magnet link..."
            # Edit it to show "Fetching metadata..."
            try:
                await client.edit_message(
                    chat_id, progress_msg_id,
                    f"🧲 Task {task.id}: Processing magnet link...\n"
                    f"🔄 Fetching metadata..."
                )
                # Schedule for auto-deletion
                if task:
                    task.add_temp_message(progress_msg_id)
                    await message_tracker.schedule_deletion(client, task, progress_msg_id, 600)  # 10 minutes timeout
            except Exception as e:
                logger.warning(f"Task {task.id}: Failed to edit initial magnet message: {e}")
                # Continue anyway

            async with LibtorrentSession() as ses:
                add_params = lt.parse_magnet_uri(magnet_link)
                add_params.save_path = session.user_dir
                add_params.storage_mode = lt.storage_mode_t.storage_mode_sparse

                handle = ses.add_torrent(add_params)
                session.set_handle(handle)
                logger.info(f"Task {task.id if task else 'N/A'}: Magnet link added. Infohash: {handle.info_hash()}")

                logger.info(f"Task {task.id if task else 'N/A'}: Waiting for metadata...")
                metadata_timeout = 120
                start_time = time.time()

                # --- Metadata Fetch Loop ---
                async with StatusTracker(handle, 0) as metadata_tracker: # Use 0 size during metadata fetch
                    while not handle.has_metadata():
                        if time.time() - start_time > metadata_timeout:
                            raise TimeoutError("Timeout waiting for torrent metadata from magnet link.")

                        # Check for cancellation during metadata fetch
                        if task and task.status == TaskStatus.CANCELLED:
                            logger.info(f"Task {task.id}: Cancellation detected during metadata fetch.")
                            await update_torrent_progress(client, chat_id, progress_msg_id, f"❌ Task {task.id}: Download cancelled.", min_interval=0)
                            return

                        s = handle.status()
                        meta_state_str = ['queued', 'checking', 'downloading metadata']
                        current_state = meta_state_str[s.state] if s.state < len(meta_state_str) else f"unknown ({s.state})"
                        _, _, _, stalled_timer = metadata_tracker.update(s) # Use stall timer from tracker

                        status_text = (
                            f"🧲 Task {task.id}: Processing magnet link...\n"
                            f"🔄 Status: {current_state}\n"
                            f"👥 Peers: {s.num_peers}"
                        )
                        if stalled_timer > 30: # Show stall warning earlier for metadata
                            status_text += f"\n⚠️ Metadata fetch stalled for {int(stalled_timer)}s. Waiting for peers..."

                        # Use message ID from task
                        await update_torrent_progress(client, chat_id, progress_msg_id, status_text, min_interval=5.0)
                        await asyncio.sleep(2)

                # --- Metadata Received ---
                logger.info(f"Task {task.id if task else 'N/A'}: Metadata received.")
                info = handle.get_torrent_info()
                total_size = info.total_size()
                torrent_name = info.name()
                num_files = info.num_files()
                session.torrent_name = torrent_name  # Update session with actual name

                try:
                    disk_space = psutil.disk_usage(DOWNLOAD_DIR).free
                    required_space = total_size * 1.01 + 100 * 1024 * 1024
                    if disk_space < required_space:
                        raise ValueError(f"Insufficient disk space: {format_size(disk_space)} available, need ~{format_size(required_space)}")
                except Exception as e:
                    logger.warning(f"Could not verify disk space: {e}")

                # Edit message to show metadata and start download
                await update_torrent_progress(
                    client, chat_id, progress_msg_id,
                    f"✅ Metadata received!\n"
                    f"📦 Name: {torrent_name}\n"
                    f"💾 Size: {format_size(total_size)}\n"
                    f"🗂️ Files: {num_files}\n"
                    f"Task ID: {task.id}\n"
                    f"▶️ Starting download...",
                    min_interval=0 # Force update
                )

                # --- Download Loop ---
                async with StatusTracker(handle, total_size) as tracker:
                    while not handle.status().is_seeding:
                        if task and task.status == TaskStatus.CANCELLED:
                            logger.info(f"Task {task.id}: Cancellation detected during magnet download.")
                            await update_torrent_progress(client, chat_id, progress_msg_id, f"❌ Task {task.id}: Download cancelled.", min_interval=0)
                            return

                        await asyncio.sleep(2)
                        s = handle.status()
                        state_str = ['queued', 'checking', 'downloading metadata', 'downloading', 'finished', 'seeding', 'allocating', 'checking resume']
                        current_state = state_str[s.state] if s.state < len(state_str) else f"unknown ({s.state})"
                        percent, download_rate, eta, stalled_timer = tracker.update(s)
                        peers = s.num_peers
                        seeds = s.num_seeds
                        downloaded = s.total_done

                        if task and s.state != lt.torrent_status.states.checking_files:
                            task.update_progress(s.progress)

                        now = time.time()
                        update_interval = 3.0 if percent < 99 else 5.0
                        if now - tracker.last_update_time > update_interval:
                            bar = '█' * int(percent / 5) + '░' * (20 - int(percent / 5))
                            status_text = (
                                f"⬇️ Magnet: {torrent_name}\n"
                                f"Task ID: {task.id}\n"
                                f"📊 Status: {current_state}\n"
                                f"💾 Size: {format_size(total_size)}\n"
                                f"✅ Progress: {percent:.1f}% ({format_size(downloaded)}/{format_size(total_size)})\n"
                                f"⚡ Speed: {format_size(download_rate)}/s\n"
                                f"👥 Peers/Seeds: {peers}/{seeds}\n"
                                f"⏱️ ETA: {format_time(eta)}\n"
                                f"[{bar}]"
                            )
                            if stalled_timer > 60:
                                status_text += f"\n⚠️ Download stalled for {int(stalled_timer)}s."
                            elif 0 < download_rate < 10240 and s.state == lt.torrent_status.states.downloading:
                                status_text += f"\n⚠️ Low speed."

                            # Use message ID from task
                            updated = await update_torrent_progress(client, chat_id, progress_msg_id, status_text, min_interval=update_interval - 0.5)
                            if updated:
                                tracker.last_update_time = now

                        if stalled_timer > 15 * 60 and seeds == 0 and peers == 0:
                            raise TimeoutError("Download stalled for 15 minutes with no peers or seeds.")

                # --- Handle Completion ---
                logger.info(f"Task {task.id if task else 'N/A'}: Magnet download complete for '{torrent_name}'.")
                final_message = (
                    f"✅ Magnet download complete!\n"
                    f"📦 Name: {torrent_name}\n"
                    f"💾 Size: {format_size(total_size)}\n"
                    f"Task ID: {task.id}\n"
                    f"📤 Uploading..." # Changed message
                )
                # Use message ID from task
                await update_torrent_progress(client, chat_id, progress_msg_id, final_message, min_interval=0)

                # Set and verify download path
                download_path = os.path.join(session.user_dir, torrent_name)
                session.set_download_path(download_path)

                if not os.path.exists(download_path):
                    raise FileNotFoundError(f"Downloaded content not found at expected path: {download_path}")

                if task:
                    task.result_path = download_path
                    task.update_status(TaskStatus.COMPLETED)
                    task_manager._save_task(task)
                
                user_stats_manager.record_completed_transfer(
                    chat_id, downloaded_size=total_size, task_type="magnet", success=True
                )

                # Send file to Telegram (will delete progress message on success)
                await send_to_telegram(client, download_path, chat_id, task, task_manager)
                
                # Clean up any remaining tracked messages
                await delete_tracked_messages(client, task)

                return download_path

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Magnet download error for task {task.id if task else 'N/A'}: {error_msg}", exc_info=True)
        # Use message ID from task
        if progress_msg_id:
            try:
                await update_torrent_progress(client, chat_id, progress_msg_id, f"❌ Magnet download failed: {error_msg[:200]}", min_interval=0)
                # Schedule error message for deletion after 60 seconds
                if task:
                    await message_tracker.schedule_deletion(client, task, progress_msg_id, 60)
            except Exception:
                pass
        # else: Cannot update message if ID is missing

        if task:
            task.update_status(TaskStatus.FAILED, error_msg)
            task_manager._save_task(task)
            # Clean up all tracked messages
            await delete_tracked_messages(client, task)

        user_stats_manager.record_completed_transfer(chat_id, 0, task_type="magnet", success=False)
        # Do not re-raise, let queue handler manage

    finally:
        # Temp message cleanup handled by delete_temp_messages if task failed/cancelled
        # If successful, send_to_telegram handles the main progress message deletion
        pass # No specific action needed here now