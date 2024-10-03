#!/usr/bin/env python3
"""Check whether there is a software update pending on a Starlink user terminal.

Optionally, reboot the dish to initiate install if there is an update pending.
"""

import argparse
from datetime import datetime
import logging
import sys
import time

import grpc

import loop_util
import starlink_grpc

# This is the enum value spacex.api.device.dish_pb2.SoftwareUpdateState.REBOOT_REQUIRED
REBOOT_REQUIRED = 6
# This is the enum value spacex.api.device.dish_pb2.SoftwareUpdateState.DISABLED
UPDATE_DISABLED = 7


def loop_body(opts, context):
    now = time.time()

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
        state_dflag = status.software_update_state == UPDATE_DISABLED
    except (AttributeError, ValueError):
        state_flag = None
        state_dflag = None

    try:
        stats_flag = status.software_update_stats.software_update_state == REBOOT_REQUIRED
        stats_dflag = status.software_update_stats.software_update_state == UPDATE_DISABLED
    except (AttributeError, ValueError):
        stats_flag = None
        stats_dflag = None

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
        print("Disable flags:", state_dflag, stats_dflag)

    if state_dflag or stats_dflag:
        logging.warning("Software updates appear to be disabled")

    # The swupdate_reboot_ready field does not appear to be in use, so may
    # mean something other than what it sounds like. Only use it if none of
    # the others are available.
    if alert_flag is None and state_flag is None and stats_flag is None:
        install_pending = bool(ready_flag)
    else:
        install_pending = alert_flag or state_flag or stats_flag

    if opts.verbose:
        dtnow = datetime.fromtimestamp(now, tz=getattr(opts, "timezone", None))
        print(dtnow.replace(microsecond=0, tzinfo=None).isoformat(), "- ", end="")

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
    parser.add_argument("-v",
                        "--verbose",
                        action="count",
                        default=0,
                        help="Increase verbosity, may be used multiple times")
    loop_util.add_args(parser)
    opts = parser.parse_args()

    loop_util.check_args(opts, parser)

    return opts


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    context = starlink_grpc.ChannelContext(target=opts.target)

    try:
        rc = loop_util.run_loop(opts, loop_body, opts, context)
    finally:
        context.close()

    sys.exit(rc)


if __name__ == "__main__":
    main()
