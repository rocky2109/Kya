import os
import time
import logging
import shutil
from typing import Optional, Dict, Tuple, List
from config import TEMP_DIR

logger = logging.getLogger(__name__)

class UserCookiesManager:
    """Manages individual user cookie files for video downloads"""
    
    def __init__(self):
        self.cookies_dir = os.path.join(TEMP_DIR, "user_cookies")
        os.makedirs(self.cookies_dir, exist_ok=True)
        self.user_cookies: Dict[int, str] = {}  # user_id -> cookie_file_path
        
    def get_user_cookie_path(self, user_id: int) -> str:
        """Get the path where a user's cookie file should be stored"""
        return os.path.join(self.cookies_dir, f"cookies_{user_id}.txt")
    
    def has_user_cookies(self, user_id: int) -> bool:
        """Check if a user has uploaded their own cookies"""
        cookie_path = self.get_user_cookie_path(user_id)
        return os.path.exists(cookie_path) and os.path.getsize(cookie_path) > 0
    
    def save_user_cookies(self, user_id: int, cookie_content: str) -> Tuple[bool, str]:
        """Save cookie content for a specific user and return detailed feedback"""
        try:
            cookie_path = self.get_user_cookie_path(user_id)
            
            # Validate cookie content
            if not self._is_valid_cookie_content(cookie_content):
                logger.warning(f"Invalid cookie content provided by user {user_id}")
                return False, "Invalid cookie format. Please ensure you're uploading cookies in Netscape format (.txt)"
            
            # Detect supported platforms in the cookies
            detected_platforms = self._detect_platforms_in_cookies(cookie_content)
            
            # Save the cookie file
            with open(cookie_path, 'w', encoding='utf-8') as f:
                f.write(cookie_content)
            
            self.user_cookies[user_id] = cookie_path
            logger.info(f"Saved cookies for user {user_id} with platforms: {detected_platforms}")
            
            # Create success message with detected platforms
            if detected_platforms:
                platforms_text = ", ".join(detected_platforms)
                message = f"Successfully saved cookies for: {platforms_text}"
            else:
                message = "Cookies saved successfully (platform detection uncertain)"
            
            return True, message
            
        except Exception as e:
            logger.error(f"Failed to save cookies for user {user_id}: {e}")
            return False, f"Error saving cookies: {str(e)}"
    
    def _detect_platforms_in_cookies(self, content: str) -> List[str]:
        """Detect which platforms are included in the cookie content"""
        platforms = []
        content_lower = content.lower()
        
        platform_checks = {
            "YouTube": "youtube.com",
            "Instagram": "instagram.com", 
            "TikTok": "tiktok.com",
            "Twitter/X": ["twitter.com", "x.com"],
            "Facebook": "facebook.com"
        }
        
        for platform, domains in platform_checks.items():
            if isinstance(domains, list):
                if any(domain in content_lower for domain in domains):
                    platforms.append(platform)
            else:
                if domains in content_lower:
                    platforms.append(platform)
        
        return platforms
    
    def get_user_cookies(self, user_id: int) -> Optional[str]:
        """Get the cookie file path for a user, fallback to global if none exists"""
        user_cookie_path = self.get_user_cookie_path(user_id)
        
        if os.path.exists(user_cookie_path):
            # Check if cookies are not too old (30 days)
            if time.time() - os.path.getmtime(user_cookie_path) < 30 * 24 * 3600:
                return user_cookie_path
            else:
                logger.info(f"User {user_id} cookies are older than 30 days, removing")
                self.remove_user_cookies(user_id)
        
        # Fallback to global cookies
        from config import COOKIES_FILE
        if os.path.exists(COOKIES_FILE):
            return COOKIES_FILE
        
        return None
    
    def remove_user_cookies(self, user_id: int) -> bool:
        """Remove a user's cookie file"""
        try:
            cookie_path = self.get_user_cookie_path(user_id)
            if os.path.exists(cookie_path):
                os.remove(cookie_path)
                logger.info(f"Removed cookies for user {user_id}")
            
            if user_id in self.user_cookies:
                del self.user_cookies[user_id]
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove cookies for user {user_id}: {e}")
            return False
    
    def _is_valid_cookie_content(self, content: str) -> bool:
        """Basic validation of cookie file content"""
        if not content.strip():
            return False
        
        # Check for Netscape format or JSON format
        lines = content.strip().split('\n')
        
        # Check for Netscape format (most common)
        valid_netscape_lines = 0
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                # Netscape format should have at least 6 tab-separated fields
                if len(line.split('\t')) >= 6:
                    valid_netscape_lines += 1
        
        if valid_netscape_lines > 0:
            return True
        
        # Check for JSON format (alternative)
        if content.strip().startswith('[') and content.strip().endswith(']'):
            try:
                import json
                cookies = json.loads(content)
                if isinstance(cookies, list) and len(cookies) > 0:
                    return True
            except:
                pass
        
        # Enhanced check for supported platforms
        supported_domains = [
            'youtube.com', 'instagram.com', 'tiktok.com', 
            'twitter.com', 'x.com', 'facebook.com'
        ]
        
        content_lower = content.lower()
        if any(domain in content_lower for domain in supported_domains):
            return True
        
        # Check for common cookie attributes
        if any(keyword in content_lower for keyword in ['domain', 'path', 'secure', 'httponly']):
            return True
        
        return False
    
    def get_user_cookie_info(self, user_id: int) -> Dict[str, any]:
        """Get information about user's cookies"""
        cookie_path = self.get_user_cookie_path(user_id)
        
        if not os.path.exists(cookie_path):
            return {
                "has_cookies": False,
                "file_path": None,
                "created_at": None,
                "size": 0,
                "platforms": []
            }
        
        # Read cookie content to detect platforms
        try:
            with open(cookie_path, 'r', encoding='utf-8') as f:
                content = f.read()
            platforms = self._detect_platforms_in_cookies(content)
        except:
            platforms = []
        
        stat = os.stat(cookie_path)
        return {
            "has_cookies": True,
            "file_path": cookie_path,
            "created_at": stat.st_mtime,
            "size": stat.st_size,
            "platforms": platforms
        }
    
    def cleanup_old_cookies(self, max_age_days: int = 30):
        """Clean up old cookie files"""
        try:
            current_time = time.time()
            max_age_seconds = max_age_days * 24 * 3600
            
            for filename in os.listdir(self.cookies_dir):
                if filename.startswith("cookies_") and filename.endswith(".txt"):
                    file_path = os.path.join(self.cookies_dir, filename)
                    if current_time - os.path.getmtime(file_path) > max_age_seconds:
                        try:
                            os.remove(file_path)
                            logger.info(f"Cleaned up old cookie file: {filename}")
                        except Exception as e:
                            logger.error(f"Failed to remove old cookie file {filename}: {e}")
                            
        except Exception as e:
            logger.error(f"Error during cookie cleanup: {e}")

# Global instance
user_cookies_manager = UserCookiesManager()
