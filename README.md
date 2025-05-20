# Telegram Zakulika AIO Downloader Bot

A powerful Telegram bot that can download files from various sources including direct links, torrent files, magnet links, and online videos (YouTube, etc.), and send them directly to Telegram.

## Features

- **Multi-source downloading**:
  - Direct download links
  - Torrent files
  - Magnet links
  - Video URLs (YouTube, etc.)
  - Playlist URLs

- **Automatic file handling**:
  - Large file splitting
  - Progress tracking
  - File compression

- **User management**:
  - Rate limiting
  - Usage statistics
  - Admin controls

- **System monitoring**:
  - Disk space monitoring
  - Memory usage tracking
  - Stalled download cleanup

## Getting Started

### Prerequisites

- Python 3.8 or higher
- A Telegram Bot Token (from [@BotFather](https://t.me/BotFather))
- Telegram API credentials (API ID and API Hash from [my.telegram.org](https://my.telegram.org))

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/MMXV16/telegram-zakulika-AIO-downloader.git
   cd telegram-zakulika-AIO-downloader
   ```

2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Create and configure your settings:
   - Copy `config.sample.py` to `config.py`
   - Edit `config.py` with your credentials and settings
   ```bash
   cp config.sample.py config.py
   nano config.py  # or use any text editor
   ```

4. Create necessary directories:
   ```bash
   mkdir -p downloads/temp
   mkdir -p sessions
   ```

5. Start the bot:
   ```bash
   python main.py
   ```

## Hosting on an Ubuntu VPS

### Step 1: Set up the VPS

1. Update your system:
   ```bash
   sudo apt update && sudo apt upgrade -y
   ```

2. Install required dependencies:
   ```bash
   sudo apt install -y python3 python3-pip git screen
   ```

3. Install system dependencies for aria2, libtorrent, and ffmpeg:
   ```bash
   sudo apt install -y aria2 python3-libtorrent ffmpeg
   ```

### Step 2: Clone and set up the bot

1. Clone the repository:
   ```bash
   git clone https://github.com/MMXV16/telegram-zakulika-AIO-downloader.git
   cd telegram-zakulika-AIO-downloader
   ```

2. Install Python requirements:
   ```bash
   pip3 install -r requirements.txt
   ```

3. Configure the bot:
   ```bash
   cp config.sample.py config.py
   nano config.py
   ```
   
   Ensure you set the `DOWNLOAD_DIR` to a path with enough storage space.

### Step 3: Running the bot on the VPS

1. Using Screen (keeps bot running after disconnecting from SSH):
   ```bash
   screen -S zakulika_bot
   python3 main.py
   ```
   
   To detach from the screen session, press `Ctrl+A` then `D`.
   To reconnect later:
   ```bash
   screen -r zakulika_bot
   ```

2. Alternatively, set up a systemd service for auto-start:
   ```bash
   sudo nano /etc/systemd/system/zakulika_bot.service
   ```
   
   Add the following content:
   ```
   [Unit]
   Description=Zakulika AIO Downloader Bot
   After=network.target

   [Service]
   User=your_username
   WorkingDirectory=/path/to/telegram-zakulika-AIO-downloader
   ExecStart=/usr/bin/python3 /path/to/telegram-zakulika-AIO-downloader/main.py
   Restart=always
   RestartSec=5
   StandardOutput=syslog
   StandardError=syslog
   SyslogIdentifier=zakulika_bot

   [Install]
   WantedBy=multi-user.target
   ```
   
   Enable and start the service:
   ```bash
   sudo systemctl enable zakulika_bot.service
   sudo systemctl start zakulika_bot.service
   sudo systemctl status zakulika_bot.service
   ```

## Hosting on Heroku

### Step 1: Prepare for Heroku deployment

1. Create the following files in your project root:

   `requirements.txt` (if not already present):
   ```
   telethon
   aiohttp
   pysmartdl
   psutil
   yt-dlp
   python-libtorrent
   ```

   `runtime.txt`:
   ```
   python-3.10.12
   ```

   `Procfile`:
   ```
   worker: python main.py
   ```

2. Update your `config.py` to use environment variables:
   ```python
   import os

   API_ID = int(os.environ.get("API_ID"))
   API_HASH = os.environ.get("API_HASH")
   BOT_TOKEN = os.environ.get("BOT_TOKEN")

   # Use Heroku's provided directory structure
   DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/app/downloads/")
   TEMP_DIR = os.path.join(DOWNLOAD_DIR, "temp")
   
   # Other config settings...
   ```

### Step 2: Deploy to Heroku

1. Install Heroku CLI and login:
   ```bash
   curl https://cli-assets.heroku.com/install.sh | sh
   heroku login
   ```

2. Create a new Heroku app:
   ```bash
   heroku create your-app-name
   ```

3. Set up the required environment variables:
   ```bash
   heroku config:set API_ID=your_api_id
   heroku config:set API_HASH=your_api_hash
   heroku config:set BOT_TOKEN=your_bot_token
   ```

4. Deploy your app:
   ```bash
   git push heroku main
   ```

5. Scale worker dyno:
   ```bash
   heroku ps:scale worker=1
   ```

### Important Heroku Considerations

- Heroku has an ephemeral filesystem, meaning any files not committed to the repository are lost when the dyno restarts. Use external storage services for persistent data.
- Consider using add-ons like Heroku Redis or PostgreSQL for persistent storage.
- Free tier Heroku dynos sleep after 30 minutes of inactivity and have limited hours per month.
- Consider using a paid dyno for better performance and uptime.

## Command Reference

- `/start`: Start the bot and show welcome message
- `/help`: Show help message
- `/tasks`: List your active tasks
- `/status`: Check your usage stats
- `/cancel <task_id>`: Cancel a specific task

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This bot is designed for educational purposes and personal use only. Please ensure you comply with the terms of service of the platforms you download from and respect copyright laws.
