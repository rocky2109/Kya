import time
import logging
import asyncio
from telethon.errors import FloodWaitError, MessageNotModifiedError, MessageIdInvalidError

logger = logging.getLogger(__name__)

class MessageDebouncer:
    """Helps prevent too frequent message updates by tracking last update time and content."""
    def __init__(self):
        self.last_update_time: dict[str, float] = {}  # Per-message update times
        self.last_message_content: dict[str, str] = {}  # Per-message content cache
        self.global_flood_wait_until: float = 0  # Global flood wait expiry time
        self._lock = asyncio.Lock()  # Lock for thread safety
        self._cleanup_task = None
        self._message_locks: dict[str, asyncio.Lock] = {}  # Per-message locks

    async def _get_message_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a specific message."""
        async with self._lock:
            if key not in self._message_locks:
                self._message_locks[key] = asyncio.Lock()
            return self._message_locks[key]

    async def update(self, client, chat_id: int, message_id: int, text: str, min_interval: float = 2.5, max_retries: int = 3) -> bool:
        """Update a message with rate limiting and retry logic.
        
        Args:
            client: Telegram client instance
            chat_id: Chat ID where message exists
            message_id: ID of message to update
            text: New text for the message
            min_interval: Minimum time between updates
            max_retries: Maximum number of retries for failed updates
        
        Returns:
            bool: True if update was successful or skipped intentionally
        """
        key = f"{chat_id}_{message_id}"
        message_lock = await self._get_message_lock(key)
        
        try:
            async with message_lock:  # Ensure only one update per message at a time
                now = time.time()
                
                # Check global flood wait
                if now < self.global_flood_wait_until:
                    wait_time = self.global_flood_wait_until - now
                    logger.debug(f"Global flood wait active, waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                
                # Check message-specific rate limit
                last_update = self.last_update_time.get(key, 0)
                if now - last_update < min_interval:
                    # Skip if content unchanged or too soon
                    if text == self.last_message_content.get(key):
                        return True
                    
                    # Check if update is final (contains ✅ or ❌)
                    if "✅" in text or "❌" in text:
                        # Force update for final status
                        await asyncio.sleep(min_interval - (now - last_update))
                    else:
                        return True  # Skip non-final updates if too soon
                
                # Try to update with retries
                for attempt in range(max_retries):
                    try:
                        await client.edit_message(chat_id, message_id, text)
                        self.last_update_time[key] = now
                        self.last_message_content[key] = text
                        return True
                    except FloodWaitError as e:
                        # Update global flood wait
                        self.global_flood_wait_until = now + e.seconds
                        if attempt < max_retries - 1:
                            await asyncio.sleep(e.seconds)
                        else:
                            raise  # Re-raise on final attempt
                    except MessageNotModifiedError:
                        # Message content hasn't changed, count as success
                        return True
                    except MessageIdInvalidError:
                        # Message no longer exists, remove from tracking
                        async with self._lock:
                            self.last_update_time.pop(key, None)
                            self.last_message_content.pop(key, None)
                            self._message_locks.pop(key, None)
                        return False
                    except Exception as e:
                        # Check if the exception is about a deleted message
                        error_str = str(e).lower()
                        if "message not found" in error_str or "message deleted" in error_str:
                            # Handle deleted message case
                            async with self._lock:
                                self.last_update_time.pop(key, None)
                                self.last_message_content.pop(key, None)
                                self._message_locks.pop(key, None)
                            return False
                        
                        if attempt < max_retries - 1:
                            await asyncio.sleep(1 * (2 ** attempt))  # Exponential backoff
                            continue
                        logger.error(f"Failed to update message after {max_retries} attempts: {e}")
                        raise

        except Exception as e:
            logger.error(f"Error in message update for {key}: {e}")
            return False

    async def start_cleanup(self, cleanup_interval: int = 3600):
        """Start periodic cleanup of old message tracking data."""
        async def cleanup_loop():
            while True:
                try:
                    await asyncio.sleep(cleanup_interval)
                    await self.cleanup_old_data()
                except Exception as e:
                    logger.error(f"Error in message debouncer cleanup: {e}")

        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(cleanup_loop())

    async def cleanup_old_data(self, max_age: int = 24 * 3600):
        """Clean up old message tracking data."""
        now = time.time()
        async with self._lock:
            # Clean up old message data
            old_keys = [
                key for key, last_time in self.last_update_time.items()
                if now - last_time > max_age
            ]
            for key in old_keys:
                self.last_update_time.pop(key, None)
                self.last_message_content.pop(key, None)
                self._message_locks.pop(key, None)
            
            if old_keys:
                logger.debug(f"Cleaned up {len(old_keys)} old message entries")

# Global instance (or manage it within your application structure)
message_debouncer = MessageDebouncer()
