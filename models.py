import time
from dataclasses import dataclass, field
from typing import Optional, List
from enums import TaskStatus, TaskType

@dataclass
class Task:
    id: str
    type: TaskType
    chat_id: int
    data: str
    status: TaskStatus = TaskStatus.QUEUED
    progress: float = 0.0
    message_id: Optional[int] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: Optional[str] = None
    result_path: Optional[str] = None
    temp_message_ids: List[int] = field(default_factory=list)

    def update_progress(self, progress: float):
        self.progress = progress
        self.updated_at = time.time()

    def update_status(self, status: TaskStatus, error: Optional[str] = None):
        self.status = status
        self.updated_at = time.time()
        if error:
            self.error = error

    def add_temp_message(self, message_id: int):
        """Add a temporary message ID to be cleaned up later"""
        if message_id and message_id not in self.temp_message_ids:
            self.temp_message_ids.append(message_id)
