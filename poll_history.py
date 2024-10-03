#!/usr/bin/env python3
"""A simple(?) example for using the starlink_grpc module.

This script shows an example of how to use the starlink_grpc module to
implement polling of status and/or history data.

By itself, it's not very useful unless you're trying to understand how the
status data correlates with certain aspects of the history data because all it
does is to dump both status and history data when it detects certain
conditions in the history data.
"""

from datetime import datetime
from datetime import timezone
import time

import starlink_grpc

INITIAL_SAMPLES = 20
LOOP_SLEEP_TIME = 4


def run_loop(context):
    samples = INITIAL_SAMPLES
    counter = None
    prev_triggered = False
    while True:
        try:
            # `starlink_grpc.status_data` returns a tuple of 3 dicts, but in case
            # the API changes to add more in the future, it's best to reference
            # them by index instead of direct assignment from the function call.
            groups = starlink_grpc.status_data(context=context)
            status = groups[0]

            # On the other hand, `starlink_grpc.history_bulk_data` will always
            # return 2 dicts, because that's all the data there is.
            general, bulk = starlink_grpc.history_bulk_data(samples, start=counter, context=context)
        except starlink_grpc.GrpcError:
            # Dish rebooting maybe, or LAN connectivity error. Just ignore it
            # and hope it goes away.
            pass
        else:
            # The following is what actually does stuff with the data. It should
            # be replaced with something more useful.

            # This computes a trigger detecting any packet loss (ping drop):
            #triggered = any(x > 0 for x in bulk["pop_ping_drop_rate"])
            # This computes a trigger detecting samples marked as obstructed:
            #triggered = any(bulk["obstructed"])
            # This computes a trigger detecting samples not marked as scheduled:
            triggered = not all(bulk["scheduled"])
            if triggered or prev_triggered:
                print("Triggered" if triggered else "Continued", "at:",
                      datetime.now(tz=timezone.utc))
                print("status:", status)
                print("history:", bulk)
                if not triggered:
                    print()

            prev_triggered = triggered
            # The following makes the next loop only pull the history samples that
            # are newer than the ones already examined.
            samples = -1
            counter = general["end_counter"]

        # And this is a not-very-robust way of implementing an interval loop.
        # Note that a 4 second loop will poll the history buffer pretty
        # frequently. Even though we only ask for new samples (which should
        # only be 4 of them), the grpc layer needs to pull the entire 12 hour
        # history buffer each time, only to discard most of it.
        time.sleep(LOOP_SLEEP_TIME)


def main():
    # This part is optional. The `starlink_grpc` functions can work without a
    # `starlink_grpc.ChannelContext` object passed in, but they will open a
    # new channel for each RPC call (so twice for each loop iteration) without
    # it.
    context = starlink_grpc.ChannelContext()

    try:
        run_loop(context)
    finally:
        context.close()


if __name__ == "__main__":
    main()
