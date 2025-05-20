import math

def format_size(bytes_val):
    if bytes_val is None or bytes_val < 0:
         return "0 Bytes" # Or "N/A" or raise error
    if bytes_val >= 1_000_000_000:
        return f"{bytes_val / 1_000_000_000:.2f} GB"
    elif bytes_val >= 1_000_000:
        return f"{bytes_val / 1_000_000:.2f} MB"
    elif bytes_val >= 1_000:
        return f"{bytes_val / 1_000:.2f} KB"
    return f"{bytes_val} Bytes"

def format_time(seconds):
    if seconds is None or seconds < 0 or math.isinf(seconds):
        return "N/A"
    try:
        seconds = int(seconds)
        hours, rem = divmod(seconds, 3600)
        minutes, sec = divmod(rem, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {sec}s"
        elif minutes > 0:
            return f"{minutes}m {sec}s"
        else:
            return f"{sec}s"
    except (ValueError, TypeError):
         return "N/A"
