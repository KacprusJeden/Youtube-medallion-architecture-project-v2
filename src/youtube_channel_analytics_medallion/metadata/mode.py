from datetime import datetime
from youtube_channel_analytics_medallion.metadata import watermark as wtm
import calendar

def get_mode(date_of_data: str, automatically: bool = True, reload_all: bool = False):
    date_dt = datetime.strptime(date_of_data, "%Y-%m-%d").date() 

    if automatically:
        last_day = calendar.monthrange(date_dt.year, date_dt.month)[1]
        if wtm.is_watermark_empty() or date_dt.day == last_day:
            return "full"
        return "incremental"

    if reload_all:
        return "reload"
    if wtm.is_watermark_empty(date_of_data):
        return "incremental"
    return "reload_date"