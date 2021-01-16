#!/usr/bin/python3
######################################################################
#
# Equivalent script to parseJsonHistory.py, except integrating the
# gRPC calls, instead of relying on separate invocation of grpcurl.
#
# This script examines the most recent samples from the history data
# and computes several different metrics related to packet loss. By
# default, it will print the results in CSV format.
#
######################################################################

import datetime
import getopt
import logging
import sys
import time

import starlink_grpc


def main():
    arg_error = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], "ahrs:t:vH")
    except getopt.GetoptError as err:
        print(str(err))
        arg_error = True

    # Default to 1 hour worth of data samples.
    samples_default = 3600
    samples = None
    print_usage = False
    verbose = False
    default_loop_time = 0
    loop_time = default_loop_time
    run_lengths = False
    print_header = False

    if not arg_error:
        if len(args) > 0:
            arg_error = True
        else:
            for opt, arg in opts:
                if opt == "-a":
                    samples = -1
                elif opt == "-h":
                    print_usage = True
                elif opt == "-r":
                    run_lengths = True
                elif opt == "-s":
                    samples = int(arg)
                elif opt == "-t":
                    loop_time = float(arg)
                elif opt == "-v":
                    verbose = True
                elif opt == "-H":
                    print_header = True

    if print_usage or arg_error:
        print("Usage: " + sys.argv[0] + " [options...]")
        print("Options:")
        print("    -a: Parse all valid samples")
        print("    -h: Be helpful")
        print("    -r: Include ping drop run length stats")
        print("    -s <num>: Number of data samples to parse, default: loop interval,")
        print("              if set, else " + str(samples_default))
        print("    -t <num>: Loop interval in seconds or 0 for no loop, default: " +
              str(default_loop_time))
        print("    -v: Be verbose")
        print("    -H: print CSV header instead of parsing history data")
        sys.exit(1 if arg_error else 0)

    if samples is None:
        samples = int(loop_time) if loop_time > 0 else samples_default

    logging.basicConfig(format="%(levelname)s: %(message)s")

    g_fields, pd_fields, rl_fields = starlink_grpc.history_ping_field_names()

    if print_header:
        header = ["datetimestamp_utc"]
        header.extend(g_fields)
        header.extend(pd_fields)
        if run_lengths:
            for field in rl_fields:
                if field.startswith("run_"):
                    header.extend(field + "_" + str(x) for x in range(1, 61))
                else:
                    header.append(field)
        print(",".join(header))
        sys.exit(0)

    def loop_body():
        timestamp = datetime.datetime.utcnow()

        try:
            g_stats, pd_stats, rl_stats = starlink_grpc.history_ping_stats(samples, verbose)
        except starlink_grpc.GrpcError as e:
            logging.error("Failure getting ping stats: %s", str(e))
            return 1

        if verbose:
            print("Parsed samples:        " + str(g_stats["samples"]))
            print("Total ping drop:       " + str(pd_stats["total_ping_drop"]))
            print("Count of drop == 1:    " + str(pd_stats["count_full_ping_drop"]))
            print("Obstructed:            " + str(pd_stats["count_obstructed"]))
            print("Obstructed ping drop:  " + str(pd_stats["total_obstructed_ping_drop"]))
            print("Obstructed drop == 1:  " + str(pd_stats["count_full_obstructed_ping_drop"]))
            print("Unscheduled:           " + str(pd_stats["count_unscheduled"]))
            print("Unscheduled ping drop: " + str(pd_stats["total_unscheduled_ping_drop"]))
            print("Unscheduled drop == 1: " + str(pd_stats["count_full_unscheduled_ping_drop"]))
            if run_lengths:
                print("Initial drop run fragment: " + str(rl_stats["init_run_fragment"]))
                print("Final drop run fragment: " + str(rl_stats["final_run_fragment"]))
                print("Per-second drop runs:  " +
                      ", ".join(str(x) for x in rl_stats["run_seconds"]))
                print("Per-minute drop runs:  " +
                      ", ".join(str(x) for x in rl_stats["run_minutes"]))
            if loop_time > 0:
                print()
        else:
            csv_data = [timestamp.replace(microsecond=0).isoformat()]
            csv_data.extend(str(g_stats[field]) for field in g_fields)
            csv_data.extend(str(pd_stats[field]) for field in pd_fields)
            if run_lengths:
                for field in rl_fields:
                    if field.startswith("run_"):
                        csv_data.extend(str(substat) for substat in rl_stats[field])
                    else:
                        csv_data.append(str(rl_stats[field]))
            print(",".join(csv_data))

        return 0

    next_loop = time.monotonic()
    while True:
        rc = loop_body()
        if loop_time > 0:
            now = time.monotonic()
            next_loop = max(next_loop + loop_time, now)
            time.sleep(next_loop - now)
        else:
            break

    sys.exit(rc)


if __name__ == '__main__':
    main()
