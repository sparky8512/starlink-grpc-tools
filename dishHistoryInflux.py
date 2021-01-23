#!/usr/bin/python3
######################################################################
#
# Write Starlink user terminal packet loss, latency, and usage data
# to an InfluxDB database.
#
# This script examines the most recent samples from the history data,
# and either writes them in whole, or computes several different
# metrics related to packet loss and writes those, to the specified
# InfluxDB database.
#
# NOTE: The Starlink user terminal does not include time values with
# its history or status data, so this script uses current system time
# to compute the timestamps it sends to InfluxDB. It is recommended
# to run this script on a host that has its system clock synced via
# NTP. Otherwise, the timestamps may get out of sync with real time.
#
######################################################################

import getopt
from datetime import datetime
from datetime import timezone
import logging
import os
import signal
import sys
import time
import warnings

from influxdb import InfluxDBClient

import starlink_grpc

BULK_MEASUREMENT = "spacex.starlink.user_terminal.history"
PING_MEASUREMENT = "spacex.starlink.user_terminal.ping_stats"
MAX_QUEUE_LENGTH = 864000


class Terminated(Exception):
    pass


def handle_sigterm(signum, frame):
    # Turn SIGTERM into an exception so main loop can clean up
    raise Terminated()


def main():
    arg_error = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], "abhkn:p:rs:t:vC:D:IP:R:SU:")
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
    bulk_mode = False
    bulk_skip_query = False
    run_lengths = False
    host_default = "localhost"
    database_default = "starlinkstats"
    icargs = {"host": host_default, "timeout": 5, "database": database_default}
    rp = None
    flush_limit = 6
    max_batch = 5000

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
                elif opt == "-b":
                    bulk_mode = True
                elif opt == "-h":
                    print_usage = True
                elif opt == "-k":
                    bulk_skip_query = True
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
        print("    -b: Bulk mode: write individual sample data instead of summary stats")
        print("    -h: Be helpful")
        print("    -k: Skip querying for prior sample write point in bulk mode")
        print("    -n <name>: Hostname of InfluxDB server, default: " + host_default)
        print("    -p <num>: Port number to use on InfluxDB server")
        print("    -r: Include ping drop run length stats")
        print("    -s <num>: Number of data samples to parse; in bulk mode, applies to first")
        print("              loop iteration only, default: -1 in bulk mode, loop interval if")
        print("              loop interval set, else " + str(samples_default))
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
        samples = -1 if bulk_mode else int(loop_time) if loop_time > 0 else samples_default

    logging.basicConfig(format="%(levelname)s: %(message)s")

    class GlobalState:
        pass

    gstate = GlobalState()
    gstate.dish_id = None
    gstate.points = []
    gstate.counter = None
    gstate.timestamp = None
    gstate.query_done = bulk_skip_query

    def conn_error(msg, *args):
        # Connection errors that happen in an interval loop are not critical
        # failures, but are interesting enough to print in non-verbose mode.
        if loop_time > 0:
            print(msg % args)
        else:
            logging.error(msg, *args)

    def flush_points(client):
        # Don't flush points to server if the counter query failed, since some
        # may be discarded later. Write would probably fail, too, anyway.
        if bulk_mode and not gstate.query_done:
            return 1

        try:
            while len(gstate.points) > max_batch:
                client.write_points(gstate.points[:max_batch],
                                    time_precision="s",
                                    retention_policy=rp)
                if verbose:
                    print("Data points written: " + str(max_batch))
                del gstate.points[:max_batch]
            if gstate.points:
                client.write_points(gstate.points, time_precision="s", retention_policy=rp)
                if verbose:
                    print("Data points written: " + str(len(gstate.points)))
                gstate.points.clear()
        except Exception as e:
            conn_error("Failed writing to InfluxDB database: %s", str(e))
            # If failures persist, don't just use infinite memory. Max queue
            # is currently 10 days of bulk data, so something is very wrong
            # if it's ever exceeded.
            if len(gstate.points) > MAX_QUEUE_LENGTH:
                logging.error("Max write queue exceeded, discarding data.")
                del gstate.points[:-MAX_QUEUE_LENGTH]
            return 1

        return 0

    def query_counter(client, now, len_points):
        try:
            # fetch the latest point where counter field was recorded
            result = client.query("SELECT counter FROM \"{0}\" "
                                  "WHERE time>={1}s AND time<{2}s AND id=$id "
                                  "ORDER by time DESC LIMIT 1;".format(
                                      BULK_MEASUREMENT, now - len_points, now),
                                  bind_params={"id": gstate.dish_id},
                                  epoch="s")
            rpoints = list(result.get_points())
            if rpoints:
                counter = rpoints[0].get("counter", None)
                timestamp = rpoints[0].get("time", 0)
                if counter and timestamp:
                    return int(counter), int(timestamp)
        except TypeError as e:
            # bind_params was added in influxdb-python v5.2.3. That would be
            # easy enough to work around, but older versions had other problems
            # with query(), so just skip this functionality.
            logging.error(
                "Failed running query, probably due to influxdb-python version too old. "
                "Skipping resumption from prior counter value. Reported error was: %s", str(e))

        return None, 0

    def process_bulk_data(client):
        before = time.time()

        start = gstate.counter
        parse_samples = samples if start is None else -1
        general, bulk = starlink_grpc.history_bulk_data(parse_samples, start=start, verbose=verbose)

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
            if verbose:
                print("Lost sample time sync at: " +
                      str(datetime.fromtimestamp(timestamp + parsed_samples, tz=timezone.utc)))
            timestamp = None
        if timestamp is None:
            timestamp = int(before)
            if verbose and gstate.query_done:
                print("Establishing new time base: {0} -> {1}".format(
                    new_counter, datetime.fromtimestamp(timestamp, tz=timezone.utc)))
            timestamp -= parsed_samples

        for i in range(parsed_samples):
            timestamp += 1
            gstate.points.append({
                "measurement": BULK_MEASUREMENT,
                "tags": {
                    "id": gstate.dish_id
                },
                "time": timestamp,
                "fields": {k: v[i] for k, v in bulk.items() if v[i] is not None},
            })

        # save off counter value for script restart
        if parsed_samples:
            gstate.points[-1]["fields"]["counter"] = new_counter

        gstate.counter = new_counter
        gstate.timestamp = timestamp

        # This is here and not before the points being processed because if the
        # query previously failed, there will be points that were processed in
        # a prior loop. This avoids having to handle that as a special case.
        if not gstate.query_done:
            try:
                db_counter, db_timestamp = query_counter(client, timestamp, len(gstate.points))
            except Exception as e:
                # could be temporary outage, so try again next time
                conn_error("Failed querying InfluxDB for prior count: %s", str(e))
                return
            gstate.query_done = True
            start_counter = new_counter - len(gstate.points)
            if db_counter and start_counter <= db_counter < new_counter:
                del gstate.points[:db_counter - start_counter]
                if before - 2.0 <= db_timestamp + len(gstate.points) <= after + 2.0:
                    if verbose:
                        print("Using existing time base: {0} -> {1}".format(
                            db_counter, datetime.fromtimestamp(db_timestamp, tz=timezone.utc)))
                    for point in gstate.points:
                        db_timestamp += 1
                        point["time"] = db_timestamp
                    gstate.timestamp = db_timestamp
                    return
            if verbose:
                print("Establishing new time base: {0} -> {1}".format(
                    new_counter, datetime.fromtimestamp(timestamp, tz=timezone.utc)))

    def process_ping_stats():
        timestamp = time.time()

        general, pd_stats, rl_stats = starlink_grpc.history_ping_stats(samples, verbose)

        all_stats = general.copy()
        all_stats.update(pd_stats)
        if run_lengths:
            for k, v in rl_stats.items():
                if k.startswith("run_"):
                    for i, subv in enumerate(v, start=1):
                        all_stats[k + "_" + str(i)] = subv
                else:
                    all_stats[k] = v

        gstate.points.append({
            "measurement": PING_MEASUREMENT,
            "tags": {
                "id": gstate.dish_id
            },
            "time": int(timestamp),
            "fields": all_stats,
        })

    def loop_body(client):
        if gstate.dish_id is None:
            try:
                gstate.dish_id = starlink_grpc.get_id()
                if verbose:
                    print("Using dish ID: " + gstate.dish_id)
            except starlink_grpc.GrpcError as e:
                conn_error("Failure getting dish ID: %s", str(e))
                return 1

        if bulk_mode:
            try:
                process_bulk_data(client)
            except starlink_grpc.GrpcError as e:
                conn_error("Failure getting history: %s", str(e))
                return 1
        else:
            try:
                process_ping_stats()
            except starlink_grpc.GrpcError as e:
                conn_error("Failure getting ping stats: %s", str(e))
                return 1

        if verbose:
            print("Data points queued: " + str(len(gstate.points)))

        if len(gstate.points) >= flush_limit:
            return flush_points(client)

        return 0

    if "verify_ssl" in icargs and not icargs["verify_ssl"]:
        # user has explicitly said be insecure, so don't warn about it
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        # attempt to hack around breakage between influxdb-python client and 2.0 server:
        influx_client = InfluxDBClient(**icargs, headers={"Accept": "application/json"})
    except TypeError:
        # ...unless influxdb-python package version is too old
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
    except Terminated:
        pass
    finally:
        if gstate.points:
            rc = flush_points(influx_client)
        influx_client.close()

    sys.exit(rc)


if __name__ == '__main__':
    main()
