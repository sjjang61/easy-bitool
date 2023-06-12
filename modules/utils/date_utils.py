from datetime import datetime
from datetime import timedelta
# from pytz import timezone
import math
from crontab import CronTab

MIN_UNIT = 60
HOUR_UNIT = 60 * MIN_UNIT
DAY_UNIT = 24 * HOUR_UNIT

def get_now(fmt = '%Y-%m-%d'):
    return date_to_str( datetime.now(), fmt )

def date_to_str( dt, fmt = '%Y-%m-%d' ):
    return dt.strftime( fmt )

def str_to_date( str, fmt = '%Y-%m-%d'):
    return datetime.strptime( str, fmt ).date()

def add_date( dt, day ):
    return dt + timedelta(days=day)

def add_hour( dt, hour ):
    return dt + timedelta(hours=hour)

def add_minute( dt, minute ):
    return dt + timedelta(minutes=minute)

def add_month( dt, month ):
    return dt + timedelta(months=month)

def add_year( dt, year ):
    return dt + timedelta(years=year)

def get_remain_time( cron_exp, unit = 'min'):
    """
    CronExpression 의 다음 잔여 시간을 조회
    :param cron_exp: 크론식 표현
        - Ex : 30 10 * * 3 ( 매주 수(3)요일 10:30분 / min, hour, dayofmonth, month, dayofweek)
        - dow : 일(0), 월(1), 화(2), 수(3), 목(4), 금(5), 토(6)
        - https://github.com/josiahcarlson/parse-crontab
    :param unit: min/hour/day
    :return: 잔여분, 잔여시간, 잔여날짜
    """
    try:
        crontab = CronTab(cron_exp)
    except Exception as e: # 예외가 발생했을 때 실행됨
        print(f'[Exception] cron_exp : "{cron_exp}", message : "{e}"' )
        return -1

    next_schedule_time = crontab.next(default_utc=False)

    if unit == 'min':
        return math.floor( next_schedule_time / MIN_UNIT )
    elif unit == 'hour':
        return math.floor( next_schedule_time / HOUR_UNIT )
    elif unit == 'day':
        return math.floor( next_schedule_time / DAY_UNIT )