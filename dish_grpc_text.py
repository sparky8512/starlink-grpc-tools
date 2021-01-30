#!/usr/bin/python3
"""Output Starlink user terminal data info in text format.

This script pulls the current status info and/or metrics computed from the
history data and prints them to stdout either once or in a periodic loop.
By default, it will print the results in CSV format.
"""

from datetime import datetime
import logging
import sys
import time

import dish_common
import starlink_grpc

VERBOSE_FIELD_MAP = {
    # status fields (the remainder are either self-explanatory or I don't
    # know with confidence what they mean)
    "alerts": "Alerts bit field",

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
}


def parse_args():
    parser = dish_common.create_arg_parser(
        output_description=
        "print it to standard output in text format; by default, will print in CSV format")

    group = parser.add_argument_group(title="CSV output options")
    group.add_argument("-H",
                       "--print-header",
                       action="store_true",
                       help="Print CSV header instead of parsing data")

    opts = dish_common.run_arg_parser(parser, no_stdout_errors=True)

    if len(opts.mode) > 1 and "bulk_history" in opts.mode:
        parser.error("bulk_history cannot be combined with other modes for CSV output")

    return opts


def print_header(opts):
    header = ["datetimestamp_utc"]

    def header_add(names):
        for name in names:
            name, start, end = dish_common.BRACKETS_RE.match(name).group(1, 4, 5)
            if start:
                header.extend(name + "_" + str(x) for x in range(int(start), int(end)))
            elif end:
                header.extend(name + "_" + str(x) for x in range(int(end)))
            else:
                header.append(name)

    if opts.satus_mode:
        status_names, obstruct_names, alert_names = starlink_grpc.status_field_names()
        if "status" in opts.mode:
            header_add(status_names)
        if "obstruction_detail" in opts.mode:
            header_add(obstruct_names)
        if "alert_detail" in opts.mode:
            header_add(alert_names)

    if opts.bulk_mode:
        general, bulk = starlink_grpc.history_bulk_field_names()
        header_add(general)
        header_add(bulk)

    if opts.ping_mode:
        general, ping, runlen = starlink_grpc.history_ping_field_names()
        header_add(general)
        if "ping_drop" in opts.mode:
            header_add(ping)
        if "ping_run_length" in opts.mode:
            header_add(runlen)

    print(",".join(header))


def loop_body(opts, gstate):
    if opts.verbose:
        csv_data = []
    else:
        csv_data = [datetime.utcnow().replace(microsecond=0).isoformat()]

    def cb_data_add_item(name, val, category):
        if opts.verbose:
            csv_data.append("{0:22} {1}".format(VERBOSE_FIELD_MAP.get(name, name) + ":", val))
        else:
            # special case for get_status failure: this will be the lone item added
            if name == "state" and val == "DISH_UNREACHABLE":
                csv_data.extend(["", "", "", val])
            else:
                csv_data.append(str(val))

    def cb_data_add_sequence(name, val, category, start):
        if opts.verbose:
            csv_data.append("{0:22} {1}".format(
                VERBOSE_FIELD_MAP.get(name, name) + ":", ", ".join(str(subval) for subval in val)))
        else:
            csv_data.extend(str(subval) for subval in val)

    def cb_add_bulk(bulk, count, timestamp, counter):
        if opts.verbose:
            print("Time range (UTC):      {0} -> {1}".format(
                datetime.fromtimestamp(timestamp).isoformat(),
                datetime.fromtimestamp(timestamp + count).isoformat()))
            for key, val in bulk.items():
                print("{0:22} {1}".format(key + ":", ", ".join(str(subval) for subval in val)))
            if opts.loop_interval > 0.0:
                print()
        else:
            for i in range(count):
                timestamp += 1
                fields = [datetime.fromtimestamp(timestamp).isoformat()]
                fields.extend(["" if val[i] is None else str(val[i]) for val in bulk.values()])
                print(",".join(fields))

    rc = dish_common.get_data(opts,
                              gstate,
                              cb_data_add_item,
                              cb_data_add_sequence,
                              add_bulk=cb_add_bulk)

    if opts.verbose:
        if csv_data:
            print("\n".join(csv_data))
            if opts.loop_interval > 0.0:
                print()
    else:
        # skip if only timestamp
        if len(csv_data) > 1:
            print(",".join(csv_data))

    return rc


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    if opts.print_header:
        print_header(opts)
        sys.exit(0)

    gstate = dish_common.GlobalState()

    next_loop = time.monotonic()
    while True:
        rc = loop_body(opts, gstate)
        if opts.loop_interval > 0.0:
            now = time.monotonic()
            next_loop = max(next_loop + opts.loop_interval, now)
            time.sleep(next_loop - now)
        else:
            break

    sys.exit(rc)


if __name__ == '__main__':
    main()
