import os
import psutil
import logging
import asyncio
import gc
from typing import Optional

logger = logging.getLogger(__name__)

class MemoryMonitor:
    """Monitor system memory and prevent crashes due to memory exhaustion"""
    
    def __init__(self, warning_threshold=80, critical_threshold=90):
        """
        Initialize memory monitor
        
        Args:
            warning_threshold: Memory usage percentage to log warnings
            critical_threshold: Memory usage percentage to force cleanup
        """
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.last_warning = 0
        self.monitoring = False
    
    def get_memory_usage(self) -> dict:
        """Get current memory usage statistics"""
        try:
            memory = psutil.virtual_memory()
            return {
                'total': memory.total,
                'available': memory.available,
                'used': memory.used,
                'percentage': memory.percent,
                'free': memory.available
            }
        except Exception as e:
            logger.warning(f"Failed to get memory stats: {e}")
            return {}
    
    def check_memory(self, force_gc=True) -> bool:
        """
        Check memory usage and perform cleanup if needed
        
        Returns:
            True if memory is OK, False if critical
        """
        import time
        
        try:
            stats = self.get_memory_usage()
            if not stats:
                return True
            
            percentage = stats['percentage']
            available_mb = stats['available'] / (1024 * 1024)
            
            now = time.time()
            
            if percentage >= self.critical_threshold:
                logger.error(f"CRITICAL: Memory usage at {percentage:.1f}% (Available: {available_mb:.0f}MB)")
                if force_gc:
                    logger.info("Forcing garbage collection due to critical memory usage")
                    gc.collect()
                return False
            
            elif percentage >= self.warning_threshold:
                # Only log warning once per minute to avoid spam
                if now - self.last_warning > 60:
                    logger.warning(f"WARNING: High memory usage at {percentage:.1f}% (Available: {available_mb:.0f}MB)")
                    self.last_warning = now
                
                if force_gc:
                    gc.collect()
                return True
            
            return True
            
        except Exception as e:
            logger.warning(f"Memory check failed: {e}")
            return True
    
    def format_size(self, bytes_size: int) -> str:
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} TB"
    
    def log_memory_stats(self):
        """Log detailed memory statistics"""
        try:
            stats = self.get_memory_usage()
            if stats:
                logger.info(
                    f"Memory: {stats['percentage']:.1f}% used "
                    f"({self.format_size(stats['used'])}/{self.format_size(stats['total'])}) "
                    f"Available: {self.format_size(stats['available'])}"
                )
        except Exception as e:
            logger.warning(f"Failed to log memory stats: {e}")

# Global memory monitor instance
memory_monitor = MemoryMonitor()

async def safe_large_operation(operation_name: str, operation_func, *args, **kwargs):
    """
    Safely execute a large operation with memory monitoring
    
    Args:
        operation_name: Name of the operation for logging
        operation_func: The async function to execute
        *args, **kwargs: Arguments for the operation function
    
    Returns:
        Result of the operation or None if failed
    """
    logger.info(f"Starting {operation_name} with memory monitoring")
    memory_monitor.log_memory_stats()
    
    try:
        # Check memory before starting
        if not memory_monitor.check_memory():
            logger.error(f"Cannot start {operation_name} - insufficient memory")
            return None
        
        # Execute the operation
        result = await operation_func(*args, **kwargs)
        
        # Force cleanup after operation
        gc.collect()
        memory_monitor.log_memory_stats()
        
        logger.info(f"Completed {operation_name} successfully")
        return result
        
    except Exception as e:
        logger.error(f"Error during {operation_name}: {e}", exc_info=True)
        
        # Emergency cleanup
        gc.collect()
        memory_monitor.log_memory_stats()
        
        return None

def emergency_cleanup():
    """Perform emergency cleanup to free memory"""
    logger.warning("Performing emergency memory cleanup")
    
    try:
        # Force garbage collection multiple times
        for i in range(3):
            collected = gc.collect()
            logger.debug(f"GC round {i+1}: collected {collected} objects")
        
        # Log memory after cleanup
        memory_monitor.log_memory_stats()
        
    except Exception as e:
        logger.error(f"Emergency cleanup failed: {e}")
