import time, datetime, pytz

def utc_now_ms():
    return int(datetime.datetime.now(tz=pytz.UTC).timestamp() * 1000)
