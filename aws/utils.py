from datetime import datetime


def round_to_half_hour(dt: datetime):
    """Rounds a datetime object to the nearest half-hour"""
    minute = dt.minute
    if minute < 30:
        rounded_minute = 0
    else:
        rounded_minute = 30
    return dt.replace(minute=rounded_minute, second=0, microsecond=0)
