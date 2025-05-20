import asyncio
import logging
import time
from typing import Dict, List, Optional, Set

from models import Task
from enums import TaskStatus

logger = logging.getLogger(__name__)

class MessageTracker:
    """
    Class to track and automatically clean up temporary bot messages.
    This ensures that operational/status messages are deleted after they've served their purpose.
    """
    
    def __init__(self):
        self.task_message_delays: Dict[str, float] = {}  # Task_ID_Message_ID -> Delay in seconds
        self.auto_cleanup_tasks: Dict[str, asyncio.Task] = {}  # Task ID -> asyncio cleanup task
        self.running = True
    
    async def schedule_deletion(self, client, task: Task, message_id: int, delay: float = None):
        """
        Schedule a message for deletion after a specific delay.
        If delay is None, the message will be tracked but not automatically deleted.
        """
        # Add to task's tracked messages
        task.add_temp_message(message_id)
        
        # If delay specified, schedule for auto-deletion
        if delay is not None:
            task_key = f"{task.id}_{message_id}"
            
            # Cancel existing cleanup task for this message if it exists
            if task_key in self.auto_cleanup_tasks:
                if not self.auto_cleanup_tasks[task_key].done():
                    self.auto_cleanup_tasks[task_key].cancel()
            
            # Create new cleanup task
            cleanup_task = asyncio.create_task(
                self._delayed_delete(client, task, message_id, delay)
            )
            self.auto_cleanup_tasks[task_key] = cleanup_task
            logger.debug(f"Scheduled message {message_id} for deletion in {delay} seconds (task {task.id})")
    
    async def _delayed_delete(self, client, task: Task, message_id: int, delay: float):
        """Delete a message after a specified delay."""
        try:
            await asyncio.sleep(delay)
            # Only proceed if the task is still running
            if task and message_id in task.temp_message_ids:
                await self.delete_message(client, task, message_id)
                task_key = f"{task.id}_{message_id}"
                self.auto_cleanup_tasks.pop(task_key, None)
        except asyncio.CancelledError:
            logger.debug(f"Cleanup task for message {message_id} in task {task.id} was cancelled")
        except Exception as e:
            logger.error(f"Error in delayed message deletion for task {task.id}, message {message_id}: {e}")
    
    async def delete_message(self, client, task: Task, message_id: int):
        """Delete a specific tracked message."""
        from utils.telegram_helpers import delete_tracked_messages
        
        await delete_tracked_messages(client, task, specific_ids=[message_id])
    
    async def delete_all_task_messages(self, client, task: Task):
        """Delete all tracked messages for a task."""
        from utils.telegram_helpers import delete_tracked_messages
        
        await delete_tracked_messages(client, task)
    
    def cancel_task_deletions(self, task_id: str):
        """Cancel all pending message deletions for a task."""
        keys_to_remove = []
        
        for task_key in list(self.auto_cleanup_tasks.keys()):
            if task_key.startswith(f"{task_id}_"):
                cleanup_task = self.auto_cleanup_tasks[task_key]
                if not cleanup_task.done():
                    cleanup_task.cancel()
                keys_to_remove.append(task_key)
        
        for key in keys_to_remove:
            self.auto_cleanup_tasks.pop(key, None)
        
        logger.debug(f"Cancelled {len(keys_to_remove)} pending message deletions for task {task_id}")
    
    def cleanup_for_task_status(self, status: TaskStatus):
        """Whether messages should be cleaned up for tasks with this status."""
        return status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]
    
    async def cleanup_old_tasks(self, max_age_hours: float = 24.0):
        """Clean up tracking for old tasks."""
        current_time = time.time()
        threshold = current_time - (max_age_hours * 3600)
        
        keys_to_remove = []
        for task_key in self.auto_cleanup_tasks:
            # Extract timestamp from task ID if possible
            try:
                task_id = task_key.split('_')[0]
                if task_id.startswith('t'):
                    # Assuming your task IDs follow a pattern with timestamp
                    tasks_keys_to_cancel = [k for k in self.auto_cleanup_tasks if k.startswith(f"{task_id}_")]
                    for k in tasks_keys_to_cancel:
                        task = self.auto_cleanup_tasks[k]
                        if not task.done():
                            task.cancel()
                        keys_to_remove.append(k)
            except Exception:
                continue
                
        for key in keys_to_remove:
            self.auto_cleanup_tasks.pop(key, None)
            
        if keys_to_remove:
            logger.info(f"Cleaned up tracking for {len(keys_to_remove)} old task messages")
    
    async def shutdown(self):
        """Shutdown the tracker, cancelling all pending deletions."""
        self.running = False
        for task_key, task in list(self.auto_cleanup_tasks.items()):
            if not task.done():
                task.cancel()
        
        # Wait for all tasks to be cancelled
        pending = [t for t in self.auto_cleanup_tasks.values() if not t.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        logger.info(f"Message tracker shutdown complete, cancelled {len(pending)} pending deletions")

# Create a global instance
message_tracker = MessageTracker()
