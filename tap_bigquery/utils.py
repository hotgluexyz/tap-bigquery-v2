import logging, sys
import datetime


LOGGER = None


def get_logger(name="handoff"):
    global LOGGER
    if not LOGGER:
        logging.basicConfig(
            stream=sys.stdout,
            format="%(levelname)s - %(asctime)s - %(name)s - %(message)s",
            level=logging.INFO)
        LOGGER = logging.getLogger(name)
    return LOGGER

DATETIME_PARSE = "%Y-%m-%dT%H:%M:%SZ"
DATETIME_FMT = "%04Y-%m-%dT%H:%M:%S.%fZ"
DATETIME_FMT_SAFE = "%Y-%m-%dT%H:%M:%S.%fZ"

def strftime(dtime, format_str=DATETIME_FMT):
    if dtime.utcoffset() != datetime.timedelta(0):
        raise Exception("datetime must be pegged at UTC tzoneinfo")

    dt_str = None
    try:
        dt_str = dtime.strftime(format_str)
        if dt_str.startswith('4Y'):
            dt_str = dtime.strftime(DATETIME_FMT_SAFE)
    except ValueError:
        dt_str = dtime.strftime(DATETIME_FMT_SAFE)

    return dt_str
