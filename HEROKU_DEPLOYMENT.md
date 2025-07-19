# Heroku Deployment Guide

This guide will help you deploy the Telegram Zakulika AIO Downloader bot to Heroku.

## Prerequisites

1. **Heroku Account**: Sign up at [heroku.com](https://heroku.com)
2. **Heroku CLI**: Install from [devcenter.heroku.com/articles/heroku-cli](https://devcenter.heroku.com/articles/heroku-cli)
3. **Git**: Make sure Git is installed
4. **Telegram API Credentials**: Get these from [my.telegram.org/apps](https://my.telegram.org/apps)
5. **Bot Token**: Create a bot with [@BotFather](https://t.me/BotFather)

## Method 1: Deploy via Heroku CLI (Recommended)

### Step 1: Clone and Prepare
```bash
git clone https://github.com/MMXV16/telegram-zakulika-AIO-downloader.git
cd telegram-zakulika-AIO-downloader
```

### Step 2: Login to Heroku
```bash
heroku login
```

### Step 3: Create Heroku App
```bash
heroku create your-bot-name
```

### Step 4: Set Environment Variables
Replace the values with your actual credentials:

```bash
# Required variables
heroku config:set API_ID=your_api_id
heroku config:set API_HASH=your_api_hash
heroku config:set BOT_TOKEN=your_bot_token
heroku config:set ADMIN_USER_IDS=your_telegram_user_id

# Optional variables (with defaults)
heroku config:set MAX_CONCURRENT_DOWNLOADS=3
heroku config:set MAX_CONCURRENT_TORRENTS=2
heroku config:set MAX_TASKS_PER_USER=5
heroku config:set HOURLY_TASKS_LIMIT=10
heroku config:set DAILY_TASKS_LIMIT=30
heroku config:set DAILY_DATA_LIMIT_GB=40
heroku config:set WEEKLY_DATA_LIMIT_GB=100
heroku config:set COOLDOWN_MINUTES=5
```

### Step 5: Deploy
```bash
git add .
git commit -m "Deploy to Heroku"
git push heroku main
```

### Step 6: Scale the Worker
```bash
heroku ps:scale worker=1
```

## Method 2: Deploy via Heroku Dashboard

1. Go to [dashboard.heroku.com](https://dashboard.heroku.com)
2. Click "New" → "Create new app"
3. Choose app name and region
4. Go to "Deploy" tab
5. Connect your GitHub repository
6. Enable automatic deploys (optional)
7. Go to "Settings" tab
8. Click "Reveal Config Vars"
9. Add all the environment variables listed above
10. Go back to "Deploy" tab and click "Deploy Branch"

## Method 3: One-Click Deploy

Click this button to deploy directly:

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/MMXV16/telegram-zakulika-AIO-downloader)

## Getting Your Telegram Credentials

### API ID and Hash
1. Go to [my.telegram.org/apps](https://my.telegram.org/apps)
2. Login with your phone number
3. Create a new application
4. Copy the `API ID` and `API Hash`

### Bot Token
1. Start a chat with [@BotFather](https://t.me/BotFather)
2. Send `/newbot`
3. Follow the prompts to create your bot
4. Copy the bot token

### Your User ID
1. Start a chat with [@userinfobot](https://t.me/userinfobot)
2. Send `/start`
3. Copy your user ID

## Important Notes

### Heroku Limitations
- **Ephemeral Filesystem**: Files are deleted when the dyno restarts
- **Memory Limits**: Free dynos have 512MB RAM, paid dynos have more
- **Concurrent Downloads**: Limited by memory and CPU
- **No Persistent Storage**: Use external storage for permanent files

### Recommendations
- Use **Eco dynos** ($5/month) for better performance
- Consider **Basic** or **Standard dynos** for production use
- Monitor your dyno usage in the Heroku dashboard
- Set up logging to track bot activity

### Troubleshooting

#### Bot Not Starting
```bash
heroku logs --tail
```

#### Check Dyno Status
```bash
heroku ps
```

#### Restart the Bot
```bash
heroku restart
```

#### Scale Down/Up
```bash
heroku ps:scale worker=0  # Stop
heroku ps:scale worker=1  # Start
```

### Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `API_ID` | Yes | - | Telegram API ID |
| `API_HASH` | Yes | - | Telegram API Hash |
| `BOT_TOKEN` | Yes | - | Bot token from @BotFather |
| `ADMIN_USER_IDS` | Yes | - | Comma-separated admin user IDs |
| `MAX_CONCURRENT_DOWNLOADS` | No | 3 | Max concurrent downloads |
| `MAX_CONCURRENT_TORRENTS` | No | 2 | Max concurrent torrents |
| `MAX_TASKS_PER_USER` | No | 5 | Max tasks per user |
| `HOURLY_TASKS_LIMIT` | No | 10 | Tasks per hour per user |
| `DAILY_TASKS_LIMIT` | No | 30 | Tasks per day per user |
| `DAILY_DATA_LIMIT_GB` | No | 20 | Daily data limit in GB |
| `WEEKLY_DATA_LIMIT_GB` | No | 100 | Weekly data limit in GB |
| `COOLDOWN_MINUTES` | No | 5 | Cooldown between tasks |

## Security Considerations

1. **Keep credentials private**: Never commit API keys to public repositories
2. **Use environment variables**: Always use Heroku config vars for sensitive data
3. **Admin access**: Only add trusted users as admins
4. **Rate limiting**: Adjust limits based on your needs and Heroku plan

## Support

If you encounter issues:
1. Check the logs: `heroku logs --tail`
2. Verify environment variables are set correctly
3. Ensure your Telegram credentials are valid
4. Check the GitHub repository for updates

## Upgrading

To update your deployed bot:
```bash
git pull origin main
git push heroku main
```

The bot will automatically restart with the new code.
