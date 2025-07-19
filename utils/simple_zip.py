# Simple alternative to zipstream-ng for Heroku compatibility
import os
import zipfile
import logging
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)

def create_simple_zip(file_paths: List[str], output_path: str) -> bool:
    """Create a simple ZIP file without streaming"""
    try:
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in file_paths:
                if os.path.exists(file_path):
                    arcname = os.path.basename(file_path)
                    zipf.write(file_path, arcname)
                    logger.debug(f"Added {arcname} to zip")
        return True
    except Exception as e:
        logger.error(f"Error creating zip file: {e}")
        return False
