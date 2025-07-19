# Use Python 3.10 slim image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /tmp/downloads /tmp/sessions /tmp/torrent_bot_db

# Copy cookies file to expected location
COPY youtube_cookies.txt /tmp/youtube_cookies.txt

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DOWNLOAD_DIR=/tmp/downloads

# Expose port (not needed for worker dyno, but good practice)
EXPOSE 8080

# Run the bot
CMD ["python", "main.py"]
