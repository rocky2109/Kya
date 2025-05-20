from enum import Enum, auto

class TaskStatus(Enum):
    QUEUED = auto()
    PROCESSING = auto() # Added PROCESSING state
    AWAITING_USER_INPUT = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    PENDING = auto() # Added PENDING state
    DOWNLOADING = auto()

class TaskType(Enum):
    DIRECT_DOWNLOAD = auto()
    TORRENT = auto()
    MAGNET = auto()
    ZIP = auto()
    UPLOAD = auto()
    VIDEO_DOWNLOAD = auto()
    PLAYLIST_DOWNLOAD = auto()
