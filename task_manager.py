import os
import json
import time
import asyncio
import logging
import random
import shutil
from collections import defaultdict
from typing import Dict, List, Optional, Set

from config import (
    MAX_CONCURRENT_DOWNLOADS, MAX_CONCURRENT_TORRENTS, MAX_CONCURRENT_ZIPS,
    DB_PATH, MAX_TASKS_PER_USER
)
from enums import TaskType, TaskStatus
from models import Task

logger = logging.getLogger(__name__)

def generate_simple_task_id(user_id: int) -> str:
    """Generate a simple, user-friendly task ID"""
    timestamp = int(time.time() % 10000)
    random_part = random.randint(100, 999)
    return f"t{timestamp}{random_part}"

class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.user_tasks: Dict[int, Set[str]] = defaultdict(set)
        self.download_queue = asyncio.Queue()
        self.torrent_queue = asyncio.Queue()
        self.magnet_queue = asyncio.Queue()
        self.zip_queue = asyncio.Queue()
        self.video_queue = asyncio.Queue()
        self.playlist_queue = asyncio.Queue()
        self.download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.torrent_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TORRENTS)
        self.zip_semaphore = asyncio.Semaphore(MAX_CONCURRENT_ZIPS)
        self.video_semaphore = asyncio.Semaphore(3) # Keep video semaphore lower
        self._load_tasks()

    def _save_tasks(self):
        """Save tasks to JSON file"""
        try:
            tasks_data = {}
            for task_id, task in self.tasks.items():
                tasks_data[task_id] = {
                    'id': task.id,
                    'type': task.type.name,
                    'chat_id': task.chat_id,
                    'data': task.data,
                    'status': task.status.name,
                    'progress': task.progress,
                    'message_id': task.message_id,
                    'created_at': task.created_at,
                    'updated_at': task.updated_at,
                    'error': task.error,
                    'result_path': task.result_path,
                    'temp_message_ids': task.temp_message_ids or []
                }

            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            # Use a temporary file for atomic write
            temp_db_path = DB_PATH + '.json.tmp'
            with open(temp_db_path, 'w') as f:
                json.dump(tasks_data, f, indent=4) # Add indent for readability
            # Replace the old file with the new one
            os.replace(temp_db_path, DB_PATH + '.json')
        except Exception as e:
            logger.error(f"Error saving tasks to file: {e}")

    def _load_tasks(self):
        """Load tasks from JSON file"""
        db_file = DB_PATH + '.json'
        try:
            if not os.path.exists(db_file):
                logger.info("Task database file not found, starting fresh.")
                return

            with open(db_file, 'r') as f:
                tasks_data = json.load(f)

            loaded_count = 0
            queued_count = 0
            for task_id, task_data in tasks_data.items():
                # Skip tasks that are already finished
                if task_data.get('status') in ["COMPLETED", "FAILED", "CANCELLED"]:
                    continue

                try:
                    task = Task(
                        id=task_data['id'],
                        type=TaskType[task_data['type']],
                        chat_id=task_data['chat_id'],
                        data=task_data['data'],
                        status=TaskStatus[task_data.get('status', 'QUEUED')], # Default to QUEUED if missing
                        progress=task_data.get('progress', 0.0),
                        message_id=task_data.get('message_id'),
                        created_at=task_data.get('created_at', time.time()),
                        updated_at=task_data.get('updated_at', time.time()),
                        error=task_data.get('error'),
                        result_path=task_data.get('result_path'),
                        temp_message_ids=task_data.get('temp_message_ids', [])
                    )

                    self.tasks[task_id] = task
                    self.user_tasks[task.chat_id].add(task_id)
                    loaded_count += 1

                    # Re-queue tasks that were running or queued
                    if task.status in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
                         # Reset running tasks to queued on restart
                        task.status = TaskStatus.QUEUED
                        self._queue_task(task)
                        queued_count += 1
                except KeyError as e:
                    logger.warning(f"Skipping task {task_id} due to missing key: {e}")
                except Exception as e:
                    logger.error(f"Error loading task {task_id}: {e}")

            logger.info(f"Loaded {loaded_count} tasks from database, re-queued {queued_count}.")
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {db_file}. Starting with empty task list.")
            self.tasks = {}
            self.user_tasks = defaultdict(set)
        except Exception as e:
            logger.error(f"Error loading tasks from file {db_file}: {e}")

    def _save_task(self, task: Task):
        """Save a single task (delegates to save all for simplicity now)"""
        # Could be optimized later to update only one entry if needed
        self._save_tasks()

    def _queue_task(self, task: Task):
        if task.type == TaskType.DIRECT_DOWNLOAD:
            self.download_queue.put_nowait(task)
        elif task.type == TaskType.TORRENT:
            self.torrent_queue.put_nowait(task)
        elif task.type == TaskType.MAGNET:
            self.magnet_queue.put_nowait(task)
        elif task.type == TaskType.ZIP:
            self.zip_queue.put_nowait(task)
        elif task.type == TaskType.VIDEO_DOWNLOAD:
            self.video_queue.put_nowait(task)
        elif task.type == TaskType.PLAYLIST_DOWNLOAD:
            self.playlist_queue.put_nowait(task)
        else:
            logger.warning(f"Unknown task type {task.type} for task {task.id}")

    def add_task(self, task_type: TaskType, chat_id: int, data: str) -> Optional[Task]:
        """Add a new task to the queue"""
        if len(self.user_tasks.get(chat_id, set())) >= MAX_TASKS_PER_USER:
             logger.warning(f"User {chat_id} reached max tasks limit ({MAX_TASKS_PER_USER}).")
             return None # Indicate task was not added

        task_id = generate_simple_task_id(chat_id)
        # Ensure unique task ID
        while task_id in self.tasks:
            task_id = generate_simple_task_id(chat_id)

        task = Task(id=task_id, type=task_type, chat_id=chat_id, data=data)

        self.tasks[task_id] = task
        self.user_tasks[chat_id].add(task_id)

        self._queue_task(task)
        self._save_task(task) # Save after adding to memory and queueing

        logger.info(f"Added task {task.id} ({task.type.name}) for user {chat_id}")
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID"""
        return self.tasks.get(task_id)

    def get_user_tasks(self, chat_id: int) -> List[Task]:
        """Get all tasks for a user"""
        task_ids = self.user_tasks.get(chat_id, set())
        return [self.tasks[task_id] for task_id in task_ids if task_id in self.tasks]

    def update_task(self, task_id: str, **kwargs) -> bool:
        """Update task properties"""
        task = self.tasks.get(task_id)
        if not task:
            logger.warning(f"Attempted to update non-existent task {task_id}")
            return False

        updated = False
        for key, value in kwargs.items():
            if hasattr(task, key):
                if getattr(task, key) != value:
                    setattr(task, key, value)
                    updated = True

        if updated:
            task.updated_at = time.time()
            self._save_task(task)
            logger.debug(f"Updated task {task_id} with {kwargs}")
        return True

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task. Only QUEUED tasks can be truly cancelled instantly."""
        task = self.tasks.get(task_id)
        if not task:
            logger.warning(f"Attempted to cancel non-existent task {task_id}")
            return False

        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            logger.info(f"Task {task_id} is already in a final state ({task.status.name}). Cannot cancel.")
            return False

        # Mark as cancelled. The queue handlers should check this status.
        task.update_status(TaskStatus.CANCELLED)
        self._save_task(task)
        logger.info(f"Marked task {task_id} as CANCELLED.")
        # Note: If the task was already running, it might continue for a bit
        # until the handler checks the status again. True cancellation might
        # require more complex inter-process/thread communication.
        return True

    def clean_old_tasks(self, older_than_days: int = 7):
        """Remove old completed/failed/cancelled tasks and potentially stuck tasks from memory and disk"""
        now = time.time()
        cutoff = now - (older_than_days * 24 * 60 * 60)
        stuck_cutoff = now - (24 * 60 * 60)  # 24 hours for stuck tasks
        to_remove = []
        
        for task_id, task in list(self.tasks.items()):  # Iterate over a copy
            try:
                # Remove completed/failed/cancelled tasks older than cutoff
                if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED] and task.updated_at < cutoff:
                    to_remove.append(task_id)
                # Also remove stuck running/queued tasks after 24 hours
                elif task.status in [TaskStatus.RUNNING, TaskStatus.QUEUED] and task.updated_at < stuck_cutoff:
                    logger.warning(f"Task {task_id} appears stuck in {task.status.name} state for >24h. Marking as failed.")
                    task.update_status(TaskStatus.FAILED, "Task appears stuck (no progress for 24 hours)")
                    to_remove.append(task_id)
                # Check for resource cleanup
                elif task.status == TaskStatus.RUNNING:
                    # If task is running but progress hasn't updated in 6 hours, mark for cleanup
                    if now - task.updated_at > 6 * 60 * 60:
                        logger.warning(f"Task {task_id} has been running without progress for >6h. Marking as failed.")
                        task.update_status(TaskStatus.FAILED, "Task stalled (no progress for 6 hours)")
                        to_remove.append(task_id)
                
                # Remove from user's task set and cleanup resources
                if task_id in to_remove:
                    # Clean up any lingering temporary files
                    if task.result_path and os.path.exists(task.result_path):
                        try:
                            if os.path.isfile(task.result_path):
                                os.remove(task.result_path)
                            elif os.path.isdir(task.result_path):
                                shutil.rmtree(task.result_path)
                            logger.info(f"Cleaned up result path for task {task_id}: {task.result_path}")
                        except Exception as e:
                            logger.warning(f"Failed to clean up result path for task {task_id}: {e}")

                    # Remove from user's task set
                    if task.chat_id in self.user_tasks:
                        self.user_tasks[task.chat_id].discard(task_id)
                        if not self.user_tasks[task.chat_id]:
                            del self.user_tasks[task.chat_id]
            except Exception as e:
                logger.error(f"Error cleaning up task {task_id}: {e}")
                # Still add to removal list to prevent stuck tasks
                to_remove.append(task_id)

        removed_count = 0
        for task_id in to_remove:
            if task_id in self.tasks:
                del self.tasks[task_id]
                removed_count += 1

        if removed_count > 0:
            logger.info(f"Cleaned {removed_count} old/stuck tasks (older than {older_than_days} days or stuck >24h).")
            self._save_tasks()  # Save changes after removal

# Add a modification to the TaskManager class for in-memory operation
class MemoryOnlyTaskManager(TaskManager):
    """TaskManager that only uses in-memory storage (overrides save/load)"""

    def _save_tasks(self):
        """Skip saving to disk"""
        logger.debug("Skipping task save to disk (MemoryOnlyTaskManager)")
        return

    def _save_task(self, task: Task):
        """Skip saving individual task"""
        # logger.debug(f"Skipping save for task {task.id} (MemoryOnlyTaskManager)")
        return

    def _load_tasks(self):
        """Skip loading from disk"""
        logger.info("Skipping task load from disk (MemoryOnlyTaskManager)")
        self.tasks = {}
        self.user_tasks = defaultdict(set)
        return

    def clean_old_tasks(self, older_than_days: int = 7):
        """Clean old completed tasks from memory only"""
        now = time.time()
        cutoff = now - (older_than_days * 24 * 60 * 60)
        to_remove = []
        for task_id, task in list(self.tasks.items()): # Iterate over a copy
            if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED] and task.updated_at < cutoff:
                to_remove.append(task_id)
                # Remove from user's task set
                if task.chat_id in self.user_tasks:
                    self.user_tasks[task.chat_id].discard(task_id)
                    if not self.user_tasks[task.chat_id]:
                        del self.user_tasks[task.chat_id]

        removed_count = 0
        for task_id in to_remove:
            if task_id in self.tasks:
                del self.tasks[task_id]
                removed_count += 1

        if removed_count > 0:
            logger.info(f"Cleaned {removed_count} old tasks from memory (older than {older_than_days} days).")
        # No _save_tasks() call needed here

class StalledDownloadCleaner:
    """Handles cleanup of stalled or abandoned downloads."""
    def __init__(self, base_dir: str, stall_timeout: int = 3600):  # 1 hour default
        self.base_dir = base_dir
        self.stall_timeout = stall_timeout
        self._last_modified_cache = {}
        self._last_clean = 0
        self._clean_interval = 300  # Clean every 5 minutes

    async def should_clean(self) -> bool:
        """Check if enough time has passed since last cleanup."""
        now = time.time()
        if now - self._last_clean >= self._clean_interval:
            self._last_clean = now
            return True
        return False

    def is_stalled(self, path: str) -> bool:
        """Check if a file or directory is stalled based on last modification time."""
        try:
            current_mtime = os.path.getmtime(path)
            last_known_mtime = self._last_modified_cache.get(path)
            
            if last_known_mtime is None:
                self._last_modified_cache[path] = current_mtime
                return False
            
            if current_mtime == last_known_mtime:
                # File hasn't been modified since last check
                if time.time() - current_mtime > self.stall_timeout:
                    return True
            else:
                # Update cache with new modification time
                self._last_modified_cache[path] = current_mtime
            
            return False
        except (OSError, FileNotFoundError):
            # Clean up cache if file no longer exists
            self._last_modified_cache.pop(path, None)
            return False

    async def cleanup_stalled_downloads(self):
        """Check for and clean up stalled downloads."""
        if not await self.should_clean():
            return

        try:
            # Check user directories
            for user_dir in os.listdir(self.base_dir):
                user_path = os.path.join(self.base_dir, user_dir)
                if not os.path.isdir(user_path):
                    continue

                # Check files and subdirectories in user directory
                for item in os.listdir(user_path):
                    item_path = os.path.join(user_path, item)
                    
                    # Skip items that are actively being used
                    if item_path in self._last_modified_cache and not self.is_stalled(item_path):
                        continue

                    try:
                        if os.path.isfile(item_path):
                            # Check for partial downloads and temporary files
                            if item.endswith('.part') or item.endswith('.temp') or item.endswith('.ytdl'):
                                if self.is_stalled(item_path):
                                    os.remove(item_path)
                                    logger.info(f"Removed stalled temporary file: {item_path}")
                        
                        elif os.path.isdir(item_path):
                            # For directories, check if they're stalled
                            if self.is_stalled(item_path):
                                # Remove all files in directory first
                                for root, _, files in os.walk(item_path, topdown=False):
                                    for name in files:
                                        try:
                                            os.remove(os.path.join(root, name))
                                        except OSError as e:
                                            logger.warning(f"Failed to remove file in stalled directory: {e}")
                                
                                # Then try to remove the directory
                                try:
                                    os.rmdir(item_path)
                                    logger.info(f"Removed stalled directory: {item_path}")
                                except OSError as e:
                                    logger.warning(f"Failed to remove stalled directory: {e}")
                    
                    except Exception as e:
                        logger.error(f"Error cleaning up stalled item {item_path}: {e}")
                        continue

        except Exception as e:
            logger.error(f"Error during stalled download cleanup: {e}")
        
        finally:
            # Clean up cache entries for non-existent paths
            self._last_modified_cache = {
                path: mtime for path, mtime in self._last_modified_cache.items()
                if os.path.exists(path)
            }
