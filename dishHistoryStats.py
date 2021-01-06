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
import sys
import getopt

import starlink_grpc

arg_error = False

try:
    opts, args = getopt.getopt(sys.argv[1:], "ahrs:vH")
except getopt.GetoptError as err:
    print(str(err))
    arg_error = True

# Default to 1 hour worth of data samples.
samples_default = 3600
samples = samples_default
print_usage = False
verbose = False
parse_all = False
print_header = False
run_lengths = False

if not arg_error:
    if len(args) > 0:
        arg_error = True
    else:
        for opt, arg in opts:
            if opt == "-a":
                parse_all = True
            elif opt == "-h":
                print_usage = True
            elif opt == "-r":
                run_lengths = True
            elif opt == "-s":
                samples = int(arg)
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
    print("    -s <num>: Parse <num> data samples, default: " + str(samples_default))
    print("    -v: Be verbose")
    print("    -H: print CSV header instead of parsing file")
    sys.exit(1 if arg_error else 0)

fields, rl_fields = starlink_grpc.history_ping_field_names()

if print_header:
    header = ["datetimestamp_utc"]
    header.extend(fields)
    if run_lengths:
        for field in rl_fields:
            if field.startswith("run_"):
                header.extend(field + "_" + str(x) for x in range(1, 61))
            else:
                header.append(field)
    print(",".join(header))
    sys.exit(0)

timestamp = datetime.datetime.utcnow()

stats, rl_stats = starlink_grpc.history_ping_stats(-1 if parse_all else samples,
                                                   verbose)

if stats is None or rl_stats is None:
    # verbose output already happened, so just bail.
    sys.exit(1)

if verbose:
    print("Parsed samples:        " + str(stats["samples"]))
    print("Total ping drop:       " + str(stats["total_ping_drop"]))
    print("Count of drop == 1:    " + str(stats["count_full_ping_drop"]))
    print("Obstructed:            " + str(stats["count_obstructed"]))
    print("Obstructed ping drop:  " + str(stats["total_obstructed_ping_drop"]))
    print("Obstructed drop == 1:  " + str(stats["count_full_obstructed_ping_drop"]))
    print("Unscheduled:           " + str(stats["count_unscheduled"]))
    print("Unscheduled ping drop: " + str(stats["total_unscheduled_ping_drop"]))
    print("Unscheduled drop == 1: " + str(stats["count_full_unscheduled_ping_drop"]))
    if run_lengths:
        print("Initial drop run fragment: " + str(rl_stats["init_run_fragment"]))
        print("Final drop run fragment: " + str(rl_stats["final_run_fragment"]))
        print("Per-second drop runs:  " + ", ".join(str(x) for x in rl_stats["run_seconds"]))
        print("Per-minute drop runs:  " + ", ".join(str(x) for x in rl_stats["run_minutes"]))
else:
    csv_data = [timestamp.replace(microsecond=0).isoformat()]
    csv_data.extend(str(stats[field]) for field in fields)
    if run_lengths:
        for field in rl_fields:
            if field.startswith("run_"):
                csv_data.extend(str(substat) for substat in rl_stats[field])
            else:
                csv_data.append(str(rl_stats[field]))
    print(",".join(csv_data))
