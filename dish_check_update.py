#!/usr/bin/python3
"""Check whether there is a software update pending on a Starlink user terminal.

Optionally, reboot the dish to initiate install if there is an update pending.
"""

import argparse
try:
    from croniter import croniter
    import dateutil.tz
    croniter_ok = True
except ImportError:
    croniter_ok = False
from datetime import datetime
import logging
import sys
import time

import grpc

import starlink_grpc

# This is the enum value spacex.api.device.dish_pb2.SoftwareUpdateState.REBOOT_REQUIRED
REBOOT_REQUIRED = 6
MAX_SLEEP = 3600.0


def loop_body(opts, context):
    try:
        status = starlink_grpc.get_status(context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        logging.error("Failed getting dish status: %s", str(starlink_grpc.GrpcError(e)))
        return 1

    # There are at least 3 and maybe 4 redundant flags that indicate whether or
    # not a software update is pending. In order to be robust against future
    # changes in the protocol and/or implementation of it, this scripts checks
    # them all, while allowing for the possibility that some of them have been
    # obsoleted and thus no longer present in the reflected protocol classes.

    try:
        alert_flag = status.alerts.install_pending
    except (AttributeError, ValueError):
        alert_flag = None

    try:
        state_flag = status.software_update_state == REBOOT_REQUIRED
    except (AttributeError, ValueError):
        state_flag = None

    try:
        stats_flag = status.software_update_stats.software_update_state == REBOOT_REQUIRED
    except (AttributeError, ValueError):
        stats_flag = None

    try:
        ready_flag = status.swupdate_reboot_ready
    except (AttributeError, ValueError):
        ready_flag = None

    try:
        sw_version = status.device_info.software_version
    except (AttributeError, ValueError):
        sw_version = "UNKNOWN"

    if opts.verbose >= 2:
        print("Pending flags:", alert_flag, state_flag, stats_flag, ready_flag)

    # The swupdate_reboot_ready field does not appear to be in use, so may
    # mean something other than what it sounds like. Only use it if none of
    # the others are available.
    if alert_flag is None and state_flag is None and stats_flag is None:
        install_pending = bool(ready_flag)
    else:
        install_pending = alert_flag or state_flag or stats_flag

    if install_pending:
        print("Install pending, current version:", sw_version)
        if opts.install:
            print("Rebooting dish to initiate install")
            try:
                starlink_grpc.reboot(context)
            except starlink_grpc.GrpcError as e:
                logging.error("Failed reboot request: %s", str(e))
                return 1
    elif opts.verbose:
        print("No install pending, current version:", sw_version)

    return 0


def parse_args():
    parser = argparse.ArgumentParser(description="Check for Starlink user terminal software update")
    parser.add_argument(
        "-i",
        "--install",
        action="store_true",
        help="Initiate dish reboot to perform install if there is an update pending")
    parser.add_argument("-g",
                        "--target",
                        help="host:port of dish to query, default is the standard IP address "
                        "and port (192.168.100.1:9200)")
    parser.add_argument("-t", "--loop-interval", type=float, help="Run loop at interval in seconds")
    parser.add_argument("-c",
                        "--loop-cron",
                        help="Run loop on schedule defined by cron format expression")
    parser.add_argument("-m",
                        "--cron-timezone",
                        help='Timezone name (IANA name or "UTC") to use for --loop-cron '
                        'schedule; default is system local time')
    parser.add_argument("-v",
                        "--verbose",
                        action="count",
                        default=0,
                        help="Increase verbosity, may be used multiple times")
    opts = parser.parse_args()

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

    return opts


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    context = starlink_grpc.ChannelContext(target=opts.target)

    rc = 0
    try:
        if opts.loop_interval <= 0.0 and not opts.loop_cron:
            rc = loop_body(opts, context)
        elif opts.loop_cron:
            criter = croniter(opts.loop_cron, datetime.now(tz=opts.timezone))
            next_loop = criter.get_next()
            while True:
                now = time.time()
                while now < next_loop:
                    time.sleep(min(next_loop - now, MAX_SLEEP))
                    now = time.time()
                while next_loop < now:
                    next_loop = criter.get_next()
                rc = loop_body(opts, context)
        else:
            next_loop = time.monotonic()
            while True:
                rc = loop_body(opts, context)
                now = time.monotonic()
                next_loop = max(next_loop + opts.loop_interval, now)
                time.sleep(next_loop - now)
    except KeyboardInterrupt:
        pass
    finally:
        context.close()

    sys.exit(rc)


if __name__ == "__main__":
    main()
