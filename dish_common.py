"""Shared code among the dish_grpc_* commands

Note:

    This module is not intended to be generically useful or to export a stable
    interface. Rather, it should be considered an implementation detail of the
    other scripts, and will change as needed.

    For a module that exports an interface intended for general use, see
    starlink_grpc.
"""

import argparse
from datetime import datetime
from datetime import timezone
import logging
import re
import time

import starlink_grpc

BRACKETS_RE = re.compile(r"([^[]*)(\[((\d+),|)(\d*)\]|)$")
SAMPLES_DEFAULT = 3600
LOOP_TIME_DEFAULT = 0
STATUS_MODES = ["status", "obstruction_detail", "alert_detail"]
HISTORY_STATS_MODES = [
    "ping_drop", "ping_run_length", "ping_latency", "ping_loaded_latency", "usage"
]
UNGROUPED_MODES = []


def create_arg_parser(output_description, bulk_history=True):
    """Create an argparse parser and add the common command line options."""
    parser = argparse.ArgumentParser(
        description="Collect status and/or history data from a Starlink user terminal and " +
        output_description,
        epilog="Additional arguments can be read from a file by including @FILENAME as an "
        "option, where FILENAME is a path to a file that contains arguments, one per line.",
        fromfile_prefix_chars="@",
        add_help=False)

    # need to remember this for later
    parser.bulk_history = bulk_history

    group = parser.add_argument_group(title="General options")
    group.add_argument("-h", "--help", action="help", help="Be helpful")
    group.add_argument("-t",
                       "--loop-interval",
                       type=float,
                       default=float(LOOP_TIME_DEFAULT),
                       help="Loop interval in seconds or 0 for no loop, default: " +
                       str(LOOP_TIME_DEFAULT))
    group.add_argument("-v", "--verbose", action="store_true", help="Be verbose")

    group = parser.add_argument_group(title="History mode options")
    group.add_argument("-a",
                       "--all-samples",
                       action="store_const",
                       const=-1,
                       dest="samples",
                       help="Parse all valid samples")
    if bulk_history:
        sample_help = ("Number of data samples to parse; normally applies to first loop "
                       "iteration only, default: -1 in bulk mode, loop interval if loop interval "
                       "set, else " + str(SAMPLES_DEFAULT))
        no_counter_help = ("Don't track sample counter across loop iterations in non-bulk "
                           "modes; keep using samples option value instead")
    else:
        sample_help = ("Number of data samples to parse; normally applies to first loop "
                       "iteration only, default: loop interval, if set, else " +
                       str(SAMPLES_DEFAULT))
        no_counter_help = ("Don't track sample counter across loop iterations; keep using "
                           "samples option value instead")
    group.add_argument("-s", "--samples", type=int, help=sample_help)
    group.add_argument("-j", "--no-counter", action="store_true", help=no_counter_help)

    return parser


def run_arg_parser(parser, need_id=False, no_stdout_errors=False):
    """Run parse_args on a parser previously created with create_arg_parser

    Args:
        need_id (bool): A flag to set in options to indicate whether or not to
            set dish_id on the global state object; see get_data for more
            detail.
        no_stdout_errors (bool): A flag set in options to protect stdout from
            error messages, in case that's where the data output is going, so
            may be being redirected to a file.

    Returns:
        An argparse Namespace object with the parsed options set as attributes.
    """
    all_modes = STATUS_MODES + HISTORY_STATS_MODES + UNGROUPED_MODES
    if parser.bulk_history:
        all_modes.append("bulk_history")
    parser.add_argument("mode",
                        nargs="+",
                        choices=all_modes,
                        help="The data group to record, one or more of: " + ", ".join(all_modes),
                        metavar="mode")

    opts = parser.parse_args()

    # for convenience, set flags for whether any mode in a group is selected
    opts.satus_mode = bool(set(STATUS_MODES).intersection(opts.mode))
    opts.history_stats_mode = bool(set(HISTORY_STATS_MODES).intersection(opts.mode))
    opts.bulk_mode = "bulk_history" in opts.mode

    if opts.samples is None:
        opts.samples = -1 if opts.bulk_mode else int(
            opts.loop_interval) if opts.loop_interval >= 1.0 else SAMPLES_DEFAULT

    opts.no_stdout_errors = no_stdout_errors
    opts.need_id = need_id

    return opts


def conn_error(opts, msg, *args):
    """Indicate an error in an appropriate way."""
    # Connection errors that happen in an interval loop are not critical
    # failures, but are interesting enough to print in non-verbose mode.
    if opts.loop_interval > 0.0 and not opts.no_stdout_errors:
        print(msg % args)
    else:
        logging.error(msg, *args)


class GlobalState:
    """A class for keeping state across loop iterations."""
    def __init__(self):
        # counter for bulk_history:
        self.counter = None
        # counter for history stats:
        self.counter_stats = None
        self.timestamp = None
        self.dish_id = None
        self.context = starlink_grpc.ChannelContext()

    def shutdown(self):
        self.context.close()


def get_data(opts, gstate, add_item, add_sequence, add_bulk=None):
    """Fetch data from the dish, pull it apart and call back with the pieces.

    This function uses call backs to return the useful data. If need_id is set
    in opts, then it is guaranteed that dish_id will have been set in gstate
    prior to any of the call backs being invoked.

    Args:
        opts (object): The options object returned from run_arg_parser.
        gstate (GlobalState): An object for keeping track of state across
            multiple calls.
        add_item (function): Call back for non-sequence data, with prototype:

            add_item(name, value, category)
        add_sequence (function): Call back for sequence data, with prototype:

            add_sequence(name, value, category, start_index_label)
        add_bulk (function): Optional. Call back for bulk history data, with
            prototype:

            add_bulk(bulk_data, count, start_timestamp, start_counter)

    Returns:
        1 if there were any failures getting data from the dish, otherwise 0.
    """
    def add_data(data, category):
        for key, val in data.items():
            name, start, seq = BRACKETS_RE.match(key).group(1, 4, 5)
            if seq is None:
                add_item(name, val, category)
            else:
                add_sequence(name, val, category, int(start) if start else 0)

    if opts.satus_mode:
        try:
            groups = starlink_grpc.status_data(context=gstate.context)
            status_data, obstruct_detail, alert_detail = groups[0:3]
        except starlink_grpc.GrpcError as e:
            if "status" in opts.mode:
                if opts.need_id and gstate.dish_id is None:
                    conn_error(opts, "Dish unreachable and ID unknown, so not recording state")
                else:
                    if opts.verbose:
                        print("Dish unreachable")
                    if "status" in opts.mode:
                        add_item("state", "DISH_UNREACHABLE", "status")
                        return 0
            return 1
        if opts.need_id:
            gstate.dish_id = status_data["id"]
            del status_data["id"]
        if "status" in opts.mode:
            add_data(status_data, "status")
        if "obstruction_detail" in opts.mode:
            add_data(obstruct_detail, "status")
        if "alert_detail" in opts.mode:
            add_data(alert_detail, "status")
    elif opts.need_id and gstate.dish_id is None:
        try:
            gstate.dish_id = starlink_grpc.get_id(context=gstate.context)
        except starlink_grpc.GrpcError as e:
            conn_error(opts, "Failure getting dish ID: %s", str(e))
            return 1
        if opts.verbose:
            print("Using dish ID: " + gstate.dish_id)

    if opts.history_stats_mode:
        start = gstate.counter_stats
        parse_samples = opts.samples if start is None else -1
        try:
            groups = starlink_grpc.history_stats(parse_samples,
                                                 start=start,
                                                 verbose=opts.verbose,
                                                 context=gstate.context)
            general, ping, runlen, latency, loaded, usage = groups[0:6]
        except starlink_grpc.GrpcError as e:
            conn_error(opts, "Failure getting ping stats: %s", str(e))
            return 1
        add_data(general, "ping_stats")
        if "ping_drop" in opts.mode:
            add_data(ping, "ping_stats")
        if "ping_run_length" in opts.mode:
            add_data(runlen, "ping_stats")
        if "ping_latency" in opts.mode:
            add_data(latency, "ping_stats")
        if "ping_loaded_latency" in opts.mode:
            add_data(loaded, "ping_stats")
        if "usage" in opts.mode:
            add_data(usage, "usage")
        if not opts.no_counter:
            gstate.counter_stats = general["end_counter"]

    if opts.bulk_mode and add_bulk:
        return get_bulk_data(opts, gstate, add_bulk)

    return 0


def get_bulk_data(opts, gstate, add_bulk):
    """Fetch bulk data.  See `get_data` for details.

    This was split out in case bulk data needs to be handled separately, for
    example, if dish_id needs to be known before calling.
    """
    before = time.time()

    start = gstate.counter
    parse_samples = opts.samples if start is None else -1
    try:
        general, bulk = starlink_grpc.history_bulk_data(parse_samples,
                                                        start=start,
                                                        verbose=opts.verbose,
                                                        context=gstate.context)
    except starlink_grpc.GrpcError as e:
        conn_error(opts, "Failure getting history: %s", str(e))
        return 1

    after = time.time()
    parsed_samples = general["samples"]
    new_counter = general["end_counter"]
    timestamp = gstate.timestamp
    # check this first, so it doesn't report as lost time sync
    if gstate.counter is not None and new_counter != gstate.counter + parsed_samples:
        timestamp = None
    # Allow up to 2 seconds of time drift before forcibly re-syncing, since
    # +/- 1 second can happen just due to scheduler timing.
    if timestamp is not None and not before - 2.0 <= timestamp + parsed_samples <= after + 2.0:
        if opts.verbose:
            print("Lost sample time sync at: " +
                  str(datetime.fromtimestamp(timestamp + parsed_samples, tz=timezone.utc)))
        timestamp = None
    if timestamp is None:
        timestamp = int(before)
        if opts.verbose:
            print("Establishing new time base: {0} -> {1}".format(
                new_counter, datetime.fromtimestamp(timestamp, tz=timezone.utc)))
        timestamp -= parsed_samples

    add_bulk(bulk, parsed_samples, timestamp, new_counter - parsed_samples)

    gstate.counter = new_counter
    gstate.timestamp = timestamp + parsed_samples
