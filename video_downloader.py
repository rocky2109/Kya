import os
import re
import time
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
import yt_dlp
from telethon import Button
from enum import Enum
import math
import glob

logger = logging.getLogger(__name__)

class VideoSource(Enum):
    YOUTUBE = "youtube"
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"
    TIKTOK = "tiktok"
    TWITTER = "twitter"
    OTHER = "other"

class VideoDownloader:
    """Class to handle video downloads using yt-dlp"""
    
    # URL patterns for supported platforms
    URL_PATTERNS = {
        VideoSource.YOUTUBE: r'(?:https?://)?(?:www\.)?(?:youtube\.com|youtu\.be)/(?:watch\?v=)?([^\s&]+)',
        VideoSource.FACEBOOK: r'(?:https?://)?(?:www\.)?facebook\.com/[^/]+/videos/(?:[^/]+/)?([^\s/]+)',
        VideoSource.INSTAGRAM: r'(?:https?://)?(?:www\.)?instagram\.com/(?:p|tv|reel)/([^\s/]+)',
        VideoSource.TIKTOK: r'(?:https?://)?(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/(?:@[^/]+/video/)?([^\s/]+)',
        VideoSource.TWITTER: r'(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/[^/]+/status/([^\s/]+)',
    }
    
    def __init__(self, download_dir: str, cookies_file: Optional[str] = None):
        self.download_dir = download_dir
        self.cookies_file = cookies_file
        if self.cookies_file and not os.path.exists(self.cookies_file):
            logger.warning(f"Cookies file not found at {self.cookies_file}. Some video downloads might fail.")
            # Create a placeholder cookies file to prevent errors
            try:
                os.makedirs(os.path.dirname(self.cookies_file), exist_ok=True)
                with open(self.cookies_file, 'w') as f:
                    f.write("# YouTube/Instagram cookies file\n")
                    f.write("# Add your cookies here in Netscape format\n")
                logger.info(f"Created placeholder cookies file at {self.cookies_file}")
            except Exception as e:
                logger.error(f"Failed to create placeholder cookies file: {e}")
                self.cookies_file = None
        
    def detect_source(self, url: str) -> Tuple[VideoSource, str]:
        """Detect the source platform and extract video ID from URL"""
        for source, pattern in self.URL_PATTERNS.items():
            match = re.search(pattern, url)
            if match:
                video_id = match.group(1)
                return source, video_id
        
        # If no specific pattern matches but yt-dlp might still support it
        return VideoSource.OTHER, url
    
    def is_playlist(self, url: str) -> bool:
        """Check if the URL is a playlist (YouTube-specific for now)"""
        return "playlist" in url or "list=" in url
        
    def format_size(self, bytes_size):
        """Format bytes to human-readable size with appropriate unit"""
        if bytes_size is None:
            return "Unknown size"
            
        if bytes_size >= 1_000_000_000:
            return f"{bytes_size / 1_000_000_000:.1f} GB"
        elif bytes_size >= 1_000_000:
            return f"{bytes_size / 1_000_000:.1f} MB"
        elif bytes_size >= 1_000:
            return f"{bytes_size / 1_000:.1f} KB"
        return f"{bytes_size} Bytes"
    
    async def get_video_info(self, url: str) -> Dict[str, Any]:
        try:
            logger.info(f"Processing URL: {url}")
            source, _ = self.detect_source(url)
            logger.info(f"Detected source: {source}")
            
            if not url.startswith(('http://', 'https://')):
                return {'error': 'Invalid URL. Please provide a valid http:// or https:// URL'}
            
            # Log the URL being processed
            logger.info(f"Processing URL: {url}")
            
            # Detect source to use appropriate options
            source, video_id = self.detect_source(url)
            
            # Enhanced yt-dlp options with updated parameters
            ydl_opts = {
                'ignoreerrors': True,
                'quiet': False,
                'no_warnings': False,
                'noplaylist': False,  # Allow playlist extraction
                'extract_flat': 'in_playlist' if self.is_playlist(url) else False,
                'skip_download': True,  # Just extract info first
                'format': 'best',  # Default format for info extraction
                'retries': 10,
                'fragment_retries': 15,
                'socket_timeout': 30,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
                'http_headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'DNT': '1',
                },
                'extractor_args': {
                    'youtube': {
                        'player_client': ['android', 'web'],  # Try different clients
                        'skip': ['hls', 'dash']  # Skip certain formats if they cause problems
                    }
                }
            }
            
            # Add cookies file if provided
            if self.cookies_file and os.path.exists(self.cookies_file):
                ydl_opts['cookiefile'] = self.cookies_file
                logger.info(f"Using cookies file: {self.cookies_file}")
            
            # Special handling for Instagram
            if source == VideoSource.INSTAGRAM:
                return await self.get_instagram_info(url)
            
            # Run yt-dlp in a separate thread to avoid blocking
            loop = asyncio.get_event_loop()
            
            def _extract_info():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    logger.info(f"Starting extraction for URL: {url}")
                    try:
                        result = ydl.extract_info(url, download=False)
                        logger.info(f"Extraction completed for URL: {url}")
                        return result
                    except yt_dlp.utils.DownloadError as e:
                        logger.error(f"yt-dlp extraction error: {str(e)}")
                        error_msg = str(e)
                        # Provide more descriptive errors
                        if "This video is available for Premium users only" in error_msg:
                            return {'error': 'This video requires YouTube Premium'}
                        elif "Sign in to confirm your age" in error_msg:
                            return {'error': 'Video is age restricted and requires login'}
                        elif "private video" in error_msg:
                            return {'error': 'Video is private and cannot be accessed'}
                        elif "has been removed" in error_msg:
                            return {'error': 'Video has been removed from platform'}
                        else:
                            return {'error': error_msg}
                    except Exception as e:
                        logger.error(f"Unexpected extraction error: {str(e)}")
                        return {'error': str(e)}
            
            info = await loop.run_in_executor(None, _extract_info)
            
            # Check if we got an error dictionary instead of actual info
            if isinstance(info, dict) and 'error' in info:
                return info
                
            if not info:
                logger.error(f"No info extracted for URL: {url} (source: {source})")
                return {'error': 'Failed to extract video info.'}
            
            # Process playlist info
            if self.is_playlist(url) or info.get('_type') == 'playlist':
                return self._process_playlist_info(info, url)
            
            # Process single video info
            return self._process_video_info(info, url, source)
            
        except Exception as e:
            logger.error(f"Error fetching video info: {e}", exc_info=True)
            return {'error': str(e)}
    
    def _process_video_info(self, info: Dict, url: str, source: VideoSource) -> Dict[str, Any]:
        """Process and format single video information"""
        # Extract available formats
        formats = []
        if 'formats' in info:
            # Filter out audio-only formats and create a clean format list
            video_formats = [f for f in info.get('formats', []) if f.get('vcodec') != 'none']
            
            # Group formats by resolution to avoid duplicates
            format_groups = {}
            for fmt in video_formats:
                height = fmt.get('height', 0)
                if height == 0:
                    continue
                
                # Create a key based on height for grouping
                key = f"{height}p"
                
                # If this resolution isn't in our groups or has better filesize/tbr, use it
                if key not in format_groups or (fmt.get('filesize') and fmt.get('filesize') > format_groups[key].get('filesize', 0)):
                    format_groups[key] = fmt
            
            # Convert groups to list and sort by height (descending)
            for height, fmt in sorted(format_groups.items(), key=lambda x: int(x[0].rstrip('p')), reverse=True):
                format_id = fmt.get('format_id', 'unknown')
                ext = fmt.get('ext', 'mp4')
                filesize = fmt.get('filesize') or fmt.get('filesize_approx')
                
                # Create a user-friendly description
                if height.rstrip('p') == '1080':
                    desc = f"🔹 Full HD ({height})"
                elif height.rstrip('p') == '720':
                    desc = f"🔹 HD ({height})"
                elif int(height.rstrip('p')) <= 480:
                    desc = f"🔹 SD ({height})"
                else:
                    desc = f"🔹 {height}"
                
                formats.append({
                    'format_id': format_id,
                    'desc': desc,
                    'height': int(height.rstrip('p')),
                    'filesize': filesize,
                    'ext': ext
                })
            
            # Add "best" option at the top if formats exist
            if formats:
                formats.insert(0, {
                    'format_id': 'best',
                    'desc': '✅ Best Quality',
                    'height': formats[0]['height'] if formats else 720,
                    'filesize': formats[0]['filesize'] if formats else 0,
                    'ext': formats[0]['ext'] if formats else 'mp4'
                })
                
                # Add "worst" option at the bottom for faster download
                formats.append({
                    'format_id': 'worst',
                    'desc': '⚡ Fastest (Lowest Quality)',
                    'height': formats[-1]['height'] if formats else 360,
                    'filesize': formats[-1]['filesize'] if formats else 0,
                    'ext': formats[-1]['ext'] if formats else 'mp4'
                })
        
        # If no formats were found, add default options
        if not formats:
            formats = [
                {
                    'format_id': 'best',
                    'desc': '✅ Best Quality',
                    'height': 720,
                    'filesize': 0,
                    'ext': 'mp4'
                },
                {
                    'format_id': 'worst',
                    'desc': '⚡ Fastest (Lowest Quality)',
                    'height': 360,
                    'filesize': 0,
                    'ext': 'mp4'
                }
            ]
        
        # Return formatted video info
        return {
            'title': info.get('title', 'Video'),
            'id': info.get('id', ''),
            'uploader': info.get('uploader', 'Unknown'),
            'duration': info.get('duration', 0),
            'formats': formats,
            'thumbnail': info.get('thumbnail'),
            'is_playlist': False,
            'webpage_url': url,
            'source': source.value
        }
    
    def _process_playlist_info(self, info: Dict, url: str) -> Dict[str, Any]:
        """Process and format playlist information"""
        # Extract entries
        entries = []
        if 'entries' in info:
            entries = list(info['entries'])
        
        # Return formatted playlist info
        return {
            'title': info.get('title', 'Playlist'),
            'id': info.get('id', ''),
            'uploader': info.get('uploader', 'Unknown'),
            'entries': entries,
            'is_playlist': True,
            'webpage_url': url,
            'playlist_count': len(entries)
        }
    
    async def download_video(self, url: str, format_id: str, output_path: str, progress_callback=None) -> Dict[str, Any]:
        """Download video in specified format with progress tracking"""
        ydl_opts = {}
        temp_files = []  # Track temporary files for cleanup
        try:
            # Create output directory
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Detect source to use appropriate options
            source, video_id = self.detect_source(url)
            
            # Get base path for identifying temp files
            base_dir = os.path.dirname(output_path)
            base_name = os.path.splitext(os.path.basename(output_path))[0]
            
            # Track potential temp files using the base name
            temp_pattern = os.path.join(base_dir, f"{base_name}.*")
            
            # Prepare yt-dlp options with improved error handling
            ydl_opts = {
                'ignoreerrors': False,
                'quiet': False,
                'no_warnings': False,
                'format': format_id,
                'outtmpl': output_path,
                'noplaylist': True,
                'merge_output_format': 'mp4',
                'retries': 10,
                'fragment_retries': 15,
                'socket_timeout': 30,
                'extractor_retries': 5,  # Add retries for extractor failures
                'file_access_retries': 5,  # Add retries for file access issues
                'http_chunk_size': 10485760,  # 10MB chunks for better recovery
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
                'http_headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'DNT': '1',
                },
                'postprocessors': [{
                    'key': 'FFmpegVideoConvertor',
                    'preferedformat': 'mp4'
                }]
            }
            
            # Add progress hook if callback provided
            if progress_callback: # progress_callback is async def _progress_callback(d)
                loop = asyncio.get_event_loop() # Ensure we get the main event loop
                def sync_hook(d_yt_dlp):
                    # The progress_callback in video_handler expects 'downloading' or 'finished'
                    if d_yt_dlp.get('status') in ['downloading', 'finished']:
                        asyncio.run_coroutine_threadsafe(progress_callback(d_yt_dlp), loop)
                ydl_opts['progress_hooks'] = [sync_hook]
            
            # Add cookies file if provided and exists
            if self.cookies_file and os.path.exists(self.cookies_file):
                ydl_opts['cookiefile'] = self.cookies_file
            
            # Special handling for Instagram
            if source == VideoSource.INSTAGRAM:
                ydl_opts['extractor_args'] = {
                    'instagram': {
                        'skip_hls': True,  # Skip HLS formats which might cause issues
                        'compatible_formats': True  # Use more compatible formats
                    }
                }
            
            # Special handling for YouTube
            if source == VideoSource.YOUTUBE:
                ydl_opts['extractor_args'] = {
                    'youtube': {
                        'player_client': ['android', 'web'],  # Try different clients
                        'skip': ['hls', 'dash'] if format_id != 'best' else []  # Skip certain formats if they cause problems
                    }
                }
            
            # Run yt-dlp in a separate thread
            loop = asyncio.get_event_loop()
            
            def _download_video():
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        return ydl.extract_info(url, download=True)
                except yt_dlp.utils.DownloadError as e:
                    error_msg = str(e)
                    if "This video is available for Premium users only" in error_msg:
                        raise ValueError("This video is only available for Premium users.")
                    elif "Sign in to confirm your age" in error_msg or "age-restricted" in error_msg:
                        raise ValueError("This video is age-restricted and requires login.")
                    elif "private video" in error_msg:
                        raise ValueError("This video is private and cannot be downloaded.")
                    elif "has been removed" in error_msg or "was removed" in error_msg:
                        raise ValueError("This video has been removed.")
                    elif "not available in your country" in error_msg:
                        raise ValueError("This video is not available in your country.")
                    else:
                        logger.error(f"[DOWNLOAD ERROR] {error_msg}")
                        raise
                except Exception as e:
                    logger.error(f"[UNEXPECTED ERROR] {str(e)}")
                    raise
            
            # Execute download in a separate thread
            try:
                info = await loop.run_in_executor(None, _download_video)
            except Exception as e:
                return {'success': False, 'error': str(e)}
            
            # Find potential temporary files before looking for final file
            for temp_file in glob.glob(temp_pattern):
                if os.path.exists(temp_file):
                    temp_files.append(temp_file)
            
            # Find the actual file path
            file_path = None
            if info and 'requested_downloads' in info and info['requested_downloads']:
                file_path = info['requested_downloads'][0].get('filepath', None)
            
            if not file_path:
                # Try to determine the file path from the template
                base_path = os.path.splitext(output_path)[0]
                for ext in ['mp4', 'mkv', 'webm', 'flv', 'avi']:
                    potential_path = f"{base_path}.{ext}"
                    if os.path.exists(potential_path):
                        file_path = potential_path
                        break
            
            if not file_path:
                file_path = output_path.replace('%(ext)s', 'mp4')  # Default to mp4
                
            # Check if file exists
            if not os.path.exists(file_path):
                for filename in os.listdir(os.path.dirname(output_path)):
                    if filename.startswith(os.path.basename(output_path.split('.')[0])):
                        file_path = os.path.join(os.path.dirname(output_path), filename)
                        break
            
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                # Remove temp files except the final one
                for temp_file in temp_files:
                    if temp_file != file_path and os.path.exists(temp_file):
                        try:
                            os.remove(temp_file)
                            logger.debug(f"Removed temp file: {temp_file}")
                        except Exception as e:
                            logger.warning(f"Failed to remove temp file {temp_file}: {e}")
                
                return {
                    'success': True,
                    'path': file_path,
                    'title': info.get('title', 'Video'),
                    'duration': info.get('duration', 0),
                    'filesize': os.path.getsize(file_path)
                }
            else:
                raise FileNotFoundError(f"Downloaded file not found at {file_path}")
            
        except Exception as e:
            logger.error(f"Download error for {url} (format {format_id}): {str(e)}")
            logger.debug(f"yt-dlp options used: {ydl_opts}")
            
            # Clean up any temp files on error
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                        logger.debug(f"Cleaned up temp file after error: {temp_file}")
                    except Exception as cleanup_error:
                        logger.warning(f"Failed to clean up temp file {temp_file}: {cleanup_error}")
            
            return {'success': False, 'error': f"Download failed: {str(e)}"}
    
    async def download_playlist(self, url: str, format_id: str, output_dir: str, progress_callback=None) -> Dict[str, Any]:
        """Download all videos in a playlist with progress reporting"""
        try:
        # Get playlist info
            try:
                logger.info(f"Starting playlist extraction for URL: {url}")
                playlist_info = await self.get_video_info(url)
            except Exception as e:
                logger.error(f"Playlist extraction failed: {e}")
                return {'success': False, 'error': f"Failed to extract playlist info: {str(e)}"}
        
            if 'error' in playlist_info:
                return {'success': False, 'error': playlist_info['error']}
        
            # Verify this is a playlist
            if not playlist_info.get('is_playlist', False):
                return {'success': False, 'error': 'Not a playlist URL'}
        
            # Create directory for playlist
            os.makedirs(output_dir, exist_ok=True)
        
            entries = playlist_info.get('entries', [])
            total_videos = len(entries)
        
            if total_videos == 0:
                logger.error(f"No videos found in playlist URL: {url}")
                return {'success': False, 'error': 'No videos found in playlist'}
        
            logger.info(f"Playlist: {playlist_info.get('title')} - Found {total_videos} videos")

            downloaded_videos = []
            failed_videos = []
            total_size = 0
        
            # Download each video in the playlist
            for i, entry in enumerate(entries):
                video_url = None
                video_title = f"Video {i+1}"

                try:
                    # Extract video URL and title
                    video_url = entry.get('url') or entry.get('webpage_url', '')
                    video_title = entry.get('title', f"Video {i+1}")

                    if not video_url:
                        logger.error(f"Missing URL for video {i+1} in playlist")
                        failed_videos.append({
                            'title': video_title,
                            'error': 'No valid URL found'
                        })
                        continue
            
                    logger.info(f"Processing playlist video {i+1}/{total_videos}: {video_title}")

                    # Create safe filename
                    safe_title = re.sub(r'[^\w\-.]', '_', video_title)
                    output_path = os.path.join(output_dir, f"{i+1:03d}_{safe_title}.%(ext)s")

                    # Report progress if callback provided
                    if progress_callback:
                        progress_callback(i+1, total_videos, video_title, 0, 'starting')  # Removed 'await'

                    # Fix YouTube URLs if needed
                    if not (video_url.startswith('http://') or video_url.startswith('https://')):
                        video_url = f"https://www.youtube.com/watch?v={video_url}"
                        logger.info(f"Fixed video URL to: {video_url}")

                    # Download the video
                    result = await self.download_video(video_url, format_id, output_path)

                    # Report completion status
                    if progress_callback:
                        progress_callback(
                            i+1, total_videos, video_title, 100,
                            'completed' if result.get('success', False) else 'failed'
                        )  # Removed 'await'
                
                    if result.get('success', False):
                        file_path = result.get('path')
                        if file_path and os.path.exists(file_path):
                            file_size = os.path.getsize(file_path)
                            logger.info(f"Successfully downloaded video {i+1}/{total_videos}: {video_title} ({self.format_size(file_size)})")

                            downloaded_videos.append({
                                'path': file_path,
                                'title': video_title,
                                'size': file_size
                            })

                            total_size += file_size
                        else:
                            logger.error(f"Downloaded file not found: {file_path}")
                            failed_videos.append({
                                'title': video_title,
                                'error': 'File not found after download'
                            })
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        logger.error(f"Failed to download video {i+1}/{total_videos}: {video_title} - {error_msg}")
                        failed_videos.append({
                            'title': video_title,
                            'error': error_msg
                        })

                except Exception as e:
                    logger.error(f"Unexpected error downloading video {i+1}: {str(e)}")
                    failed_videos.append({
                        'title': video_title,
                        'error': str(e)
                    })

            # Return results
            return {
                'success': len(downloaded_videos) > 0,
                'playlist_title': playlist_info.get('title', 'Playlist'),
                'downloaded_videos': downloaded_videos,
                'failed_videos': failed_videos,
                'total': total_videos,
                'total_size': total_size,
                'playlist_id': playlist_info.get('id', ''),
                'playlist_url': url
            }
    
        except Exception as e:
            logger.error(f"Error downloading playlist: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
    
    async def get_instagram_info(self, url: str) -> Dict[str, Any]:
        """Special handling for Instagram links which may need different options"""
        try:
            # Enhanced options specifically for Instagram
            ydl_opts = {
                'quiet': False,
                'no_warnings': False,
                'noplaylist': True,
                'retries': 10,
                'fragment_retries': 15,
                'skip_download': True,  # Just extract info
                'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
                'http_headers': {
                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'DNT': '1'
                },
                'extractor_args': {
                    'instagram': {
                        'skip_hls': True,  # Skip HLS formats which might cause issues
                        'compatible_formats': True  # Use more compatible formats
                    }
                }
            }
            
            # Add cookies file if provided
            if self.cookies_file and os.path.exists(self.cookies_file):
                ydl_opts['cookiefile'] = self.cookies_file
            
            # Log the URL being processed
            logger.info(f"Processing Instagram URL: {url}")
            
            loop = asyncio.get_event_loop()
            
            def _extract_info():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(url, download=False)
            
            info = await loop.run_in_executor(None, _extract_info)
            
            if not info:
                logger.error("No info returned from yt-dlp for Instagram")
                return {'error': 'Failed to extract video information'}
            
            # Process formats
            formats = []
            if 'formats' in info:
                # Filter out audio-only formats
                video_formats = [f for f in info.get('formats', []) if f.get('vcodec') != 'none']
                
                # Group formats by resolution to avoid duplicates
                format_groups = {}
                for fmt in video_formats:
                    height = fmt.get('height', 0)
                    if height == 0:
                        continue
                    
                    # Create a key based on height for grouping
                    key = f"{height}p"
                    
                    # If this resolution isn't in our groups or has better filesize/tbr, use it
                    if key not in format_groups or (fmt.get('filesize') and fmt.get('filesize') > format_groups[key].get('filesize', 0)):
                        format_groups[key] = fmt
                
                # Convert groups to list and sort by height (descending)
                for height, fmt in sorted(format_groups.items(), key=lambda x: int(x[0].rstrip('p')), reverse=True):
                    format_id = fmt.get('format_id', 'unknown')
                    ext = fmt.get('ext', 'mp4')
                    filesize = fmt.get('filesize') or fmt.get('filesize_approx')
                    
                    # Create a user-friendly description
                    if height.rstrip('p') == '1080':
                        desc = f"🔹 Full HD ({height})"
                    elif height.rstrip('p') == '720':
                        desc = f"🔹 HD ({height})"
                    elif int(height.rstrip('p')) <= 480:
                        desc = f"🔹 SD ({height})"
                    else:
                        desc = f"🔹 {height}"
                    
                    formats.append({
                        'format_id': format_id,
                        'desc': desc,
                        'height': int(height.rstrip('p')),
                        'filesize': filesize,
                        'ext': ext
                    })
            
            # If no formats were found, add default options
            if not formats:
                formats = [
                    {
                        'format_id': 'best',
                        'desc': '✅ Best Quality',
                        'height': 720,
                        'filesize': 0,
                        'ext': 'mp4'
                    },
                    {
                        'format_id': 'worst',
                        'desc': '⚡ Fastest (Lowest Quality)',
                        'height': 360,
                        'filesize': 0,
                        'ext': 'mp4'
                    }
                ]
            else:
                # Add "best" option at the top if formats exist
                formats.insert(0, {
                    'format_id': 'best',
                    'desc': '✅ Best Quality',
                    'height': formats[0]['height'] if formats else 720,
                    'filesize': formats[0]['filesize'] if formats else 0,
                    'ext': formats[0]['ext'] if formats else 'mp4'
                })
                
                # Add "worst" option at the bottom for faster download
                formats.append({
                    'format_id': 'worst',
                    'desc': '⚡ Fastest (Lowest Quality)',
                    'height': formats[-1]['height'] if formats else 360,
                    'filesize': formats[-1]['filesize'] if formats else 0,
                    'ext': formats[-1]['ext'] if formats else 'mp4'
                })
            
            # Return simplified info
            return {
                'title': info.get('title', 'Instagram Video'),
                'id': info.get('id', ''),
                'uploader': info.get('uploader', 'Unknown'),
                'formats': formats,
                'is_playlist': False,
                'webpage_url': url,
                'source': 'instagram',
                'duration': info.get('duration', 0)
            }
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Instagram extraction error: {error_msg}")
            
            # Check for common Instagram authentication errors
            if "login required" in error_msg.lower() or "rate-limit" in error_msg.lower() or "authentication" in error_msg.lower():
                return {'error': 'Instagram authentication required. Please provide cookies or try again later.'}
            elif "content is not available" in error_msg.lower():
                return {'error': 'Instagram content not available. It may be private or deleted.'}
            else:
                return {'error': f'Instagram download failed: {error_msg}'}
    
    async def compress_videos(self, video_paths: List[str], output_dir: str, quality: str = 'medium') -> Dict[str, Any]:
        """Compress multiple videos using ffmpeg"""
        try:
            if not video_paths:
                return {'success': False, 'error': 'No videos to compress'}
            
            os.makedirs(output_dir, exist_ok=True)
            
            # Define compression presets based on quality
            presets = {
                'high': '-preset slow -crf 22',
                'medium': '-preset medium -crf 26',
                'low': '-preset fast -crf 30'
            }
            
            preset = presets.get(quality, presets['medium'])
            
            compressed_files = []
            failed_files = []
            total_original_size = 0
            total_compressed_size = 0
            
            for i, video_path in enumerate(video_paths):
                try:
                    if not os.path.exists(video_path):
                        failed_files.append({
                            'path': video_path,
                            'error': 'File not found'
                        })
                        continue
                    
                    file_size = os.path.getsize(video_path)
                    total_original_size += file_size
                    
                    filename = os.path.basename(video_path)
                    output_path = os.path.join(output_dir, f"compressed_{filename}")
                    
                    # Skip if output file already exists
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    
                    # Prepare ffmpeg command
                    cmd = f"ffmpeg -i \"{video_path}\" {preset} -c:v libx264 -c:a aac \"{output_path}\" -y"
                    
                    # Run ffmpeg
                    process = await asyncio.create_subprocess_shell(
                        cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    
                    await process.communicate()
                    
                    if process.returncode == 0 and os.path.exists(output_path):
                        compressed_size = os.path.getsize(output_path)
                        total_compressed_size += compressed_size
                        
                        compressed_files.append({
                            'original_path': video_path,
                            'compressed_path': output_path,
                            'original_size': file_size,
                            'compressed_size': compressed_size,
                            'reduction': (file_size - compressed_size) / file_size * 100 if file_size > 0 else 0
                        })
                    else:
                        failed_files.append({
                            'path': video_path,
                            'error': 'Compression failed'
                        })
                
                except Exception as e:
                    logger.error(f"Error compressing video {video_path}: {e}")
                    failed_files.append({
                        'path': video_path,
                        'error': str(e)
                    })
            
            return {
                'success': len(compressed_files) > 0,
                'compressed_files': compressed_files,
                'failed_files': failed_files,
                'total_original_size': total_original_size,
                'total_compressed_size': total_compressed_size,
                'reduction_percent': (total_original_size - total_compressed_size) / total_original_size * 100 if total_original_size > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error in compress_videos: {e}")
            return {'success': False, 'error': str(e)}
