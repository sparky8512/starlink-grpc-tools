#!/usr/bin/python3
######################################################################
#
# Write Starlink user terminal packet loss statistics to an InfluxDB
# database.
#
# This script examines the most recent samples from the history data,
# computes several different metrics related to packet loss, and
# writes those to the specified InfluxDB database.
#
######################################################################

import datetime
import sys
import getopt

from influxdb import InfluxDBClient

import starlink_grpc

arg_error = False

try:
    opts, args = getopt.getopt(sys.argv[1:], "ahn:p:rs:vD:P:R:U:")
except getopt.GetoptError as err:
    print(str(err))
    arg_error = True

# Default to 1 hour worth of data samples.
samples_default = 3600
samples = samples_default
print_usage = False
verbose = False
run_lengths = False
host_default = "localhost"
database_default = "dishstats"
icargs = {"host": host_default, "timeout": 5, "database": database_default}
rp = None

if not arg_error:
    if len(args) > 0:
        arg_error = True
    else:
        for opt, arg in opts:
            if opt == "-a":
                samples = -1
            elif opt == "-h":
                print_usage = True
            elif opt == "-n":
                icargs["host"] = arg
            elif opt == "-p":
                icargs["port"] = int(arg)
            elif opt == "-r":
                run_lengths = True
            elif opt == "-s":
                samples = int(arg)
            elif opt == "-v":
                verbose = True
            elif opt == "-D":
                icargs["database"] = arg
            elif opt == "-P":
                icargs["password"] = arg
            elif opt == "-R":
                rp = arg
            elif opt == "-U":
                icargs["username"] = arg

if "password" in icargs and "username" not in icargs:
    print("Password authentication requires username to be set")
    arg_error = True

if print_usage or arg_error:
    print("Usage: " + sys.argv[0] + " [options...]")
    print("Options:")
    print("    -a: Parse all valid samples")
    print("    -h: Be helpful")
    print("    -n <name>: Hostname of InfluxDB server, default: " + host_default)
    print("    -p <num>: Port number to use on InfluxDB server")
    print("    -r: Include ping drop run length stats")
    print("    -s <num>: Number of data samples to parse, default: " + str(samples_default))
    print("    -v: Be verbose")
    print("    -D <name>: Database name to use, default: " + database_default)
    print("    -P <word>: Set password for authentication")
    print("    -R <name>: Retention policy name to use")
    print("    -U <name>: Set username for authentication")
    sys.exit(1 if arg_error else 0)

dish_id = starlink_grpc.get_id()

if dish_id is None:
    if verbose:
        print("Unable to connect to Starlink user terminal")
    sys.exit(1)

timestamp = datetime.datetime.utcnow()

g_stats, pd_stats, rl_stats = starlink_grpc.history_ping_stats(samples, verbose)

if g_stats is None:
    # verbose output already happened, so just bail.
    sys.exit(1)

all_stats = g_stats.copy()
all_stats.update(pd_stats)
if run_lengths:
    for k, v in rl_stats.items():
        if k.startswith("run_"):
            for i, subv in enumerate(v, start=1):
                all_stats[k + "_" + str(i)] = subv
        else:
            all_stats[k] = v

points = [{
    "measurement": "spacex.starlink.user_terminal.ping_stats",
    "tags": {"id": dish_id},
    "time": timestamp,
    "fields": all_stats,
}]

influx_client = InfluxDBClient(**icargs)
try:
    influx_client.write_points(points, retention_policy=rp)
finally:
    influx_client.close()
