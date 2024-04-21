"""Shared logic for main loop control.

This module provides support for running a function from a loop at fixed
intervals using monotonic time or on cron-like schedule using wall clock time.

The cron scheduler uses the same schedule format string that cron uses for
crontab entries, and will do its best to remain on schedule despite clock
adjustments.
"""

try:
    from croniter import croniter
    import dateutil.tz
    croniter_ok = True
except ImportError:
    croniter_ok = False
from datetime import datetime
import signal
import time

# Max time to sleep when using non-monotonic time. This helps protect against
# oversleeping as the result of large clock adjustments.
MAX_SLEEP = 3600.0


class Terminated(Exception):
    pass


def handle_sigterm(signum, frame):
    # Turn SIGTERM into an exception so main loop can clean up
    raise Terminated


def add_args(parser):
    group = parser.add_argument_group(title="Loop options")
    group.add_argument("-t", "--loop-interval", type=float, help="Run loop at interval, in seconds")
    group.add_argument("-c",
                       "--loop-cron",
                       help="Run loop on schedule defined by cron format expression")
    group.add_argument("-m",
                       "--cron-timezone",
                       help='Timezone name (IANA name or "UTC") to use for --loop-cron '
                       'schedule; default is system local time')


def check_args(opts, parser):
    if opts.loop_interval is not None and opts.loop_cron is not None:
        parser.error("At most one of --loop-interval and --loop-cron may be used")

    if opts.cron_timezone and not opts.loop_cron:
        parser.error("cron timezone specified, but not using cron scheduling")

    if opts.loop_cron is not None:
        if not croniter_ok:
            parser.error("croniter is not installed, --loop-cron requires it")
        if not croniter.is_valid(opts.loop_cron):
            parser.error("Invalid cron format")
        opts.timezone = dateutil.tz.gettz(opts.cron_timezone)
        if opts.timezone is None:
            if opts.cron_timezone is None:
                parser.error("Failed to get local timezone, may need to use --cron-timezone")
            else:
                parser.error("Invalid timezone name")

    if opts.loop_interval is None:
        opts.loop_interval = 0.0


def run_loop(opts, loop_body, *loop_args):
    signal.signal(signal.SIGTERM, handle_sigterm)

    rc = 0
    try:
        if opts.loop_interval <= 0.0 and not opts.loop_cron:
            rc = loop_body(*loop_args)
        elif opts.loop_cron:
            criter = croniter(opts.loop_cron, datetime.now(tz=opts.timezone))
            now = time.time()
            next_loop = criter.get_next(start_time=now)
            while True:
                while now < next_loop:
                    # This is to protect against clock getting set backwards
                    # by a large amount. Normally, it should do nothing:
                    next_loop = criter.get_next(start_time=now)
                    time.sleep(min(next_loop - now, MAX_SLEEP))
                    now = time.time()
                next_loop = criter.get_next(start_time=now)
                rc = loop_body(*loop_args)
                now = time.time()
        else:
            next_loop = time.monotonic()
            while True:
                rc = loop_body(*loop_args)
                now = time.monotonic()
                next_loop = max(next_loop + opts.loop_interval, now)
                time.sleep(next_loop - now)
    except (KeyboardInterrupt, Terminated):
        pass

    return rc
