#!/usr/bin/python3
r"""Output Starlink user terminal data info in text format.

Expects input as from the following command:

    grpcurl -plaintext -d {\"get_history\":{}} 192.168.100.1:9200 SpaceX.API.Device.Device/Handle

This script examines the most recent samples from the history data and
prints several different metrics computed from them to stdout. By default,
it will print the results in CSV format.
"""

import argparse
from datetime import datetime
from datetime import timezone
import logging
import re
import sys
import time

import starlink_json

BRACKETS_RE = re.compile(r"([^[]*)(\[((\d+),|)(\d*)\]|)$")
SAMPLES_DEFAULT = 3600
HISTORY_STATS_MODES = [
    "ping_drop", "ping_run_length", "ping_latency", "ping_loaded_latency", "usage"
]
VERBOSE_FIELD_MAP = {
    # ping_drop fields
    "samples": "Parsed samples",
    "end_counter": "Sample counter",
    "total_ping_drop": "Total ping drop",
    "count_full_ping_drop": "Count of drop == 1",
    "count_obstructed": "Obstructed",
    "total_obstructed_ping_drop": "Obstructed ping drop",
    "count_full_obstructed_ping_drop": "Obstructed drop == 1",
    "count_unscheduled": "Unscheduled",
    "total_unscheduled_ping_drop": "Unscheduled ping drop",
    "count_full_unscheduled_ping_drop": "Unscheduled drop == 1",

    # ping_run_length fields
    "init_run_fragment": "Initial drop run fragment",
    "final_run_fragment": "Final drop run fragment",
    "run_seconds": "Per-second drop runs",
    "run_minutes": "Per-minute drop runs",

    # ping_latency fields
    "mean_all_ping_latency": "Mean RTT, drop < 1",
    "deciles_all_ping_latency": "RTT deciles, drop < 1",
    "mean_full_ping_latency": "Mean RTT, drop == 0",
    "deciles_full_ping_latency": "RTT deciles, drop == 0",
    "stdev_full_ping_latency": "RTT standard deviation, drop == 0",

    # ping_loaded_latency is still experimental, so leave those unexplained

    # usage fields
    "download_usage": "Bytes downloaded",
    "upload_usage": "Bytes uploaded",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Collect status and/or history data from a Starlink user terminal and "
        "print it to standard output in text format; by default, will print in CSV format",
        add_help=False)

    group = parser.add_argument_group(title="General options")
    group.add_argument("-f", "--filename", default="-", help="The file to parse, default: stdin")
    group.add_argument("-h", "--help", action="help", help="Be helpful")
    group.add_argument("-t",
                       "--timestamp",
                       help="UTC time history data was pulled, as YYYY-MM-DD_HH:MM:SS or as "
                       "seconds since Unix epoch, default: current time")
    group.add_argument("-v", "--verbose", action="store_true", help="Be verbose")

    group = parser.add_argument_group(title="History mode options")
    group.add_argument("-a",
                       "--all-samples",
                       action="store_const",
                       const=-1,
                       dest="samples",
                       help="Parse all valid samples")
    group.add_argument("-s",
                       "--samples",
                       type=int,
                       help="Number of data samples to parse, default: all in bulk mode, "
                       "else " + str(SAMPLES_DEFAULT))

    group = parser.add_argument_group(title="CSV output options")
    group.add_argument("-H",
                       "--print-header",
                       action="store_true",
                       help="Print CSV header instead of parsing data")

    all_modes = HISTORY_STATS_MODES + ["bulk_history"]
    parser.add_argument("mode",
                        nargs="+",
                        choices=all_modes,
                        help="The data group to record, one or more of: " + ", ".join(all_modes),
                        metavar="mode")

    opts = parser.parse_args()

    # for convenience, set flags for whether any mode in a group is selected
    opts.history_stats_mode = bool(set(HISTORY_STATS_MODES).intersection(opts.mode))
    opts.bulk_mode = "bulk_history" in opts.mode

    if opts.history_stats_mode and opts.bulk_mode:
        parser.error("bulk_history cannot be combined with other modes for CSV output")

    if opts.samples is None:
        opts.samples = -1 if opts.bulk_mode else SAMPLES_DEFAULT

    if opts.timestamp is None:
        opts.history_time = None
    else:
        try:
            opts.history_time = int(opts.timestamp)
        except ValueError:
            try:
                opts.history_time = int(
                    datetime.strptime(opts.timestamp, "%Y-%m-%d_%H:%M:%S").timestamp())
            except ValueError:
                parser.error("Could not parse timestamp")
        if opts.verbose:
            print("Using timestamp", datetime.fromtimestamp(opts.history_time, tz=timezone.utc))

    return opts


def print_header(opts):
    header = ["datetimestamp_utc"]

    def header_add(names):
        for name in names:
            name, start, end = BRACKETS_RE.match(name).group(1, 4, 5)
            if start:
                header.extend(name + "_" + str(x) for x in range(int(start), int(end)))
            elif end:
                header.extend(name + "_" + str(x) for x in range(int(end)))
            else:
                header.append(name)

    if opts.bulk_mode:
        general, bulk = starlink_json.history_bulk_field_names()
        header_add(general)
        header_add(bulk)

    if opts.history_stats_mode:
        groups = starlink_json.history_stats_field_names()
        general, ping, runlen, latency, loaded, usage = groups[0:6]
        header_add(general)
        if "ping_drop" in opts.mode:
            header_add(ping)
        if "ping_run_length" in opts.mode:
            header_add(runlen)
        if "ping_loaded_latency" in opts.mode:
            header_add(loaded)
        if "ping_latency" in opts.mode:
            header_add(latency)
        if "usage" in opts.mode:
            header_add(usage)

    print(",".join(header))
    return 0


def get_data(opts, add_item, add_sequence, add_bulk):
    def add_data(data):
        for key, val in data.items():
            name, seq = BRACKETS_RE.match(key).group(1, 5)
            if seq is None:
                add_item(name, val)
            else:
                add_sequence(name, val)

    if opts.history_stats_mode:
        try:
            groups = starlink_json.history_stats(opts.filename, opts.samples, verbose=opts.verbose)
        except starlink_json.JsonError as e:
            logging.error("Failure getting history stats: %s", str(e))
            return 1
        general, ping, runlen, latency, loaded, usage = groups[0:6]
        add_data(general)
        if "ping_drop" in opts.mode:
            add_data(ping)
        if "ping_run_length" in opts.mode:
            add_data(runlen)
        if "ping_latency" in opts.mode:
            add_data(latency)
        if "ping_loaded_latency" in opts.mode:
            add_data(loaded)
        if "usage" in opts.mode:
            add_data(usage)

    if opts.bulk_mode and add_bulk:
        timestamp = int(time.time()) if opts.history_time is None else opts.history_time
        try:
            general, bulk = starlink_json.history_bulk_data(opts.filename,
                                                            opts.samples,
                                                            verbose=opts.verbose)
        except starlink_json.JsonError as e:
            logging.error("Failure getting bulk history: %s", str(e))
            return 1
        parsed_samples = general["samples"]
        new_counter = general["end_counter"]
        if opts.verbose:
            print("Establishing time base: {0} -> {1}".format(
                new_counter, datetime.fromtimestamp(timestamp, tz=timezone.utc)))
        timestamp -= parsed_samples

        add_bulk(bulk, parsed_samples, timestamp, new_counter - parsed_samples)

    return 0


def loop_body(opts):
    if opts.verbose:
        csv_data = []
    else:
        history_time = int(time.time()) if opts.history_time is None else opts.history_time
        csv_data = [datetime.utcfromtimestamp(history_time).isoformat()]

    def cb_data_add_item(name, val):
        if opts.verbose:
            csv_data.append("{0:22} {1}".format(VERBOSE_FIELD_MAP.get(name, name) + ":", val))
        else:
            # special case for get_status failure: this will be the lone item added
            if name == "state" and val == "DISH_UNREACHABLE":
                csv_data.extend(["", "", "", val])
            else:
                csv_data.append(str(val))

    def cb_data_add_sequence(name, val):
        if opts.verbose:
            csv_data.append("{0:22} {1}".format(
                VERBOSE_FIELD_MAP.get(name, name) + ":", ", ".join(str(subval) for subval in val)))
        else:
            csv_data.extend(str(subval) for subval in val)

    def cb_add_bulk(bulk, count, timestamp, counter):
        if opts.verbose:
            print("Time range (UTC):      {0} -> {1}".format(
                datetime.utcfromtimestamp(timestamp).isoformat(),
                datetime.utcfromtimestamp(timestamp + count).isoformat()))
            for key, val in bulk.items():
                print("{0:22} {1}".format(key + ":", ", ".join(str(subval) for subval in val)))
        else:
            for i in range(count):
                timestamp += 1
                fields = [datetime.utcfromtimestamp(timestamp).isoformat()]
                fields.extend(["" if val[i] is None else str(val[i]) for val in bulk.values()])
                print(",".join(fields))

    rc = get_data(opts, cb_data_add_item, cb_data_add_sequence, cb_add_bulk)

    if opts.verbose:
        if csv_data:
            print("\n".join(csv_data))
    else:
        # skip if only timestamp
        if len(csv_data) > 1:
            print(",".join(csv_data))

    return rc


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    if opts.print_header:
        rc = print_header(opts)
        sys.exit(rc)

    # for consistency with dish_grpc_text, pretend there was a loop
    rc = loop_body(opts)

    sys.exit(rc)


if __name__ == '__main__':
    main()
