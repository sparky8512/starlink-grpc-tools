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

import getopt
import datetime
import logging
import os
import sys
import time
import warnings

from influxdb import InfluxDBClient

import starlink_grpc


def main():
    arg_error = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], "ahn:p:rs:t:vC:D:IP:R:SU:")
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
    host_default = "localhost"
    database_default = "starlinkstats"
    icargs = {"host": host_default, "timeout": 5, "database": database_default}
    rp = None
    flush_limit = 6

    # For each of these check they are both set and not empty string
    influxdb_host = os.environ.get("INFLUXDB_HOST")
    if influxdb_host:
        icargs["host"] = influxdb_host
    influxdb_port = os.environ.get("INFLUXDB_PORT")
    if influxdb_port:
        icargs["port"] = int(influxdb_port)
    influxdb_user = os.environ.get("INFLUXDB_USER")
    if influxdb_user:
        icargs["username"] = influxdb_user
    influxdb_pwd = os.environ.get("INFLUXDB_PWD")
    if influxdb_pwd:
        icargs["password"] = influxdb_pwd
    influxdb_db = os.environ.get("INFLUXDB_DB")
    if influxdb_db:
        icargs["database"] = influxdb_db
    influxdb_rp = os.environ.get("INFLUXDB_RP")
    if influxdb_rp:
        rp = influxdb_rp
    influxdb_ssl = os.environ.get("INFLUXDB_SSL")
    if influxdb_ssl:
        icargs["ssl"] = True
        if influxdb_ssl.lower() == "secure":
            icargs["verify_ssl"] = True
        elif influxdb_ssl.lower() == "insecure":
            icargs["verify_ssl"] = False
        else:
            icargs["verify_ssl"] = influxdb_ssl

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
                elif opt == "-t":
                    loop_time = float(arg)
                elif opt == "-v":
                    verbose = True
                elif opt == "-C":
                    icargs["ssl"] = True
                    icargs["verify_ssl"] = arg
                elif opt == "-D":
                    icargs["database"] = arg
                elif opt == "-I":
                    icargs["ssl"] = True
                    icargs["verify_ssl"] = False
                elif opt == "-P":
                    icargs["password"] = arg
                elif opt == "-R":
                    rp = arg
                elif opt == "-S":
                    icargs["ssl"] = True
                    icargs["verify_ssl"] = True
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
        print("    -s <num>: Number of data samples to parse, default: loop interval,")
        print("              if set, else " + str(samples_default))
        print("    -t <num>: Loop interval in seconds or 0 for no loop, default: " +
              str(default_loop_time))
        print("    -v: Be verbose")
        print("    -C <filename>: Enable SSL/TLS using specified CA cert to verify server")
        print("    -D <name>: Database name to use, default: " + database_default)
        print("    -I: Enable SSL/TLS but disable certificate verification (INSECURE!)")
        print("    -P <word>: Set password for authentication")
        print("    -R <name>: Retention policy name to use")
        print("    -S: Enable SSL/TLS using default CA cert")
        print("    -U <name>: Set username for authentication")
        sys.exit(1 if arg_error else 0)

    if samples is None:
        samples = int(loop_time) if loop_time > 0 else samples_default

    logging.basicConfig(format="%(levelname)s: %(message)s")

    class GlobalState:
        pass

    gstate = GlobalState()
    gstate.dish_id = None
    gstate.points = []

    def conn_error(msg, *args):
        # Connection errors that happen in an interval loop are not critical
        # failures, but are interesting enough to print in non-verbose mode.
        if loop_time > 0:
            print(msg % args)
        else:
            logging.error(msg, *args)

    def flush_points(client):
        try:
            client.write_points(gstate.points, retention_policy=rp)
            if verbose:
                print("Data points written: " + str(len(gstate.points)))
                gstate.points.clear()
        except Exception as e:
            conn_error("Failed writing to InfluxDB database: %s", str(e))
            return 1

        return 0

    def loop_body(client):
        if gstate.dish_id is None:
            try:
                gstate.dish_id = starlink_grpc.get_id()
                if verbose:
                    print("Using dish ID: " + gstate.dish_id)
            except starlink_grpc.GrpcError as e:
                conn_error("Failure getting dish ID: %s", str(e))
                return 1

        timestamp = datetime.datetime.utcnow()

        try:
            g_stats, pd_stats, rl_stats = starlink_grpc.history_ping_stats(samples, verbose)
        except starlink_grpc.GrpcError as e:
            conn_error("Failure getting ping stats: %s", str(e))
            return 1

        all_stats = g_stats.copy()
        all_stats.update(pd_stats)
        if run_lengths:
            for k, v in rl_stats.items():
                if k.startswith("run_"):
                    for i, subv in enumerate(v, start=1):
                        all_stats[k + "_" + str(i)] = subv
                else:
                    all_stats[k] = v

        gstate.points.append({
            "measurement": "spacex.starlink.user_terminal.ping_stats",
            "tags": {
                "id": gstate.dish_id
            },
            "time": timestamp,
            "fields": all_stats,
        })
        if verbose:
            print("Data points queued: " + str(len(gstate.points)))

        if len(gstate.points) >= flush_limit:
            return flush_points(client)

        return 0

    if "verify_ssl" in icargs and not icargs["verify_ssl"]:
        # user has explicitly said be insecure, so don't warn about it
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    influx_client = InfluxDBClient(**icargs)
    try:
        next_loop = time.monotonic()
        while True:
            rc = loop_body(influx_client)
            if loop_time > 0:
                now = time.monotonic()
                next_loop = max(next_loop + loop_time, now)
                time.sleep(next_loop - now)
            else:
                break
    finally:
        if gstate.points:
            rc = flush_points(influx_client)
        influx_client.close()

    sys.exit(rc)


if __name__ == '__main__':
    main()
