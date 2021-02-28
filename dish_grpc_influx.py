#!/usr/bin/python3
"""Write Starlink user terminal data to an InfluxDB database.

This script pulls the current status info and/or metrics computed from the
history data and writes them to the specified InfluxDB database either once
or in a periodic loop.

Data will be written into the requested database with the following
measurement / series names:

: spacex.starlink.user_terminal.status : Current status data
: spacex.starlink.user_terminal.history : Bulk history data
: spacex.starlink.user_terminal.ping_stats : Ping history statistics
: spacex.starlink.user_terminal.usage : Usage history statistics

NOTE: The Starlink user terminal does not include time values with its
history or status data, so this script uses current system time to compute
the timestamps it sends to InfluxDB. It is recommended to run this script on
a host that has its system clock synced via NTP. Otherwise, the timestamps
may get out of sync with real time.
"""

from datetime import datetime
from datetime import timezone
import logging
import os
import signal
import sys
import time
import warnings

from influxdb import InfluxDBClient

import dish_common

HOST_DEFAULT = "localhost"
DATABASE_DEFAULT = "starlinkstats"
BULK_MEASUREMENT = "spacex.starlink.user_terminal.history"
FLUSH_LIMIT = 6
MAX_BATCH = 5000
MAX_QUEUE_LENGTH = 864000


class Terminated(Exception):
    pass


def handle_sigterm(signum, frame):
    # Turn SIGTERM into an exception so main loop can clean up
    raise Terminated


def parse_args():
    parser = dish_common.create_arg_parser(output_description="write it to an InfluxDB database")

    group = parser.add_argument_group(title="InfluxDB database options")
    group.add_argument("-n",
                       "--hostname",
                       default=HOST_DEFAULT,
                       dest="host",
                       help="Hostname of MQTT broker, default: " + HOST_DEFAULT)
    group.add_argument("-p", "--port", type=int, help="Port number to use on MQTT broker")
    group.add_argument("-P", "--password", help="Set password for username/password authentication")
    group.add_argument("-U", "--username", help="Set username for authentication")
    group.add_argument("-D",
                       "--database",
                       default=DATABASE_DEFAULT,
                       help="Database name to use, default: " + DATABASE_DEFAULT)
    group.add_argument("-R", "--retention-policy", help="Retention policy name to use")
    group.add_argument("-k",
                       "--skip-query",
                       action="store_true",
                       help="Skip querying for prior sample write point in bulk mode")
    group.add_argument("-C",
                       "--ca-cert",
                       dest="verify_ssl",
                       help="Enable SSL/TLS using specified CA cert to verify broker",
                       metavar="FILENAME")
    group.add_argument("-I",
                       "--insecure",
                       action="store_false",
                       dest="verify_ssl",
                       help="Enable SSL/TLS but disable certificate verification (INSECURE!)")
    group.add_argument("-S",
                       "--secure",
                       action="store_true",
                       dest="verify_ssl",
                       help="Enable SSL/TLS using default CA cert")

    env_map = (
        ("INFLUXDB_HOST", "host"),
        ("INFLUXDB_PORT", "port"),
        ("INFLUXDB_USER", "username"),
        ("INFLUXDB_PWD", "password"),
        ("INFLUXDB_DB", "database"),
        ("INFLUXDB_RP", "retention-policy"),
        ("INFLUXDB_SSL", "verify_ssl"),
    )
    env_defaults = {}
    for var, opt in env_map:
        # check both set and not empty string
        val = os.environ.get(var)
        if val:
            if var == "INFLUXDB_SSL" and val == "secure":
                env_defaults[opt] = True
            elif var == "INFLUXDB_SSL" and val == "insecure":
                env_defaults[opt] = False
            else:
                env_defaults[opt] = val
    parser.set_defaults(**env_defaults)

    opts = dish_common.run_arg_parser(parser, need_id=True)

    if opts.username is None and opts.password is not None:
        parser.error("Password authentication requires username to be set")

    opts.icargs = {"timeout": 5}
    for key in ["port", "host", "password", "username", "database", "verify_ssl"]:
        val = getattr(opts, key)
        if val is not None:
            opts.icargs[key] = val

    if opts.verify_ssl is not None:
        opts.icargs["ssl"] = True

    return opts


def flush_points(opts, gstate):
    try:
        while len(gstate.points) > MAX_BATCH:
            gstate.influx_client.write_points(gstate.points[:MAX_BATCH],
                                              time_precision="s",
                                              retention_policy=opts.retention_policy)
            if opts.verbose:
                print("Data points written: " + str(MAX_BATCH))
            del gstate.points[:MAX_BATCH]
        if gstate.points:
            gstate.influx_client.write_points(gstate.points,
                                              time_precision="s",
                                              retention_policy=opts.retention_policy)
            if opts.verbose:
                print("Data points written: " + str(len(gstate.points)))
            gstate.points.clear()
    except Exception as e:
        dish_common.conn_error(opts, "Failed writing to InfluxDB database: %s", str(e))
        # If failures persist, don't just use infinite memory. Max queue
        # is currently 10 days of bulk data, so something is very wrong
        # if it's ever exceeded.
        if len(gstate.points) > MAX_QUEUE_LENGTH:
            logging.error("Max write queue exceeded, discarding data.")
            del gstate.points[:-MAX_QUEUE_LENGTH]
        return 1

    return 0


def query_counter(gstate, start, end):
    try:
        # fetch the latest point where counter field was recorded
        result = gstate.influx_client.query("SELECT counter FROM \"{0}\" "
                                            "WHERE time>={1}s AND time<{2}s AND id=$id "
                                            "ORDER by time DESC LIMIT 1;".format(
                                                BULK_MEASUREMENT, start, end),
                                            bind_params={"id": gstate.dish_id},
                                            epoch="s")
        points = list(result.get_points())
        if points:
            counter = points[0].get("counter", None)
            timestamp = points[0].get("time", 0)
            if counter and timestamp:
                return int(counter), int(timestamp)
    except TypeError as e:
        # bind_params was added in influxdb-python v5.2.3. That would be easy
        # enough to work around, but older versions had other problems with
        # query(), so just skip this functionality.
        logging.error(
            "Failed running query, probably due to influxdb-python version too old. "
            "Skipping resumption from prior counter value. Reported error was: %s", str(e))

    return None, 0


def sync_timebase(opts, gstate):
    try:
        db_counter, db_timestamp = query_counter(gstate, gstate.start_timestamp, gstate.timestamp)
    except Exception as e:
        # could be temporary outage, so try again next time
        dish_common.conn_error(opts, "Failed querying InfluxDB for prior count: %s", str(e))
        return
    gstate.timebase_synced = True

    if db_counter and gstate.start_counter <= db_counter:
        del gstate.deferred_points[:db_counter - gstate.start_counter]
        if gstate.deferred_points:
            delta_timestamp = db_timestamp - (gstate.deferred_points[0]["time"] - 1)
            # to prevent +/- 1 second timestamp drift when the script restarts,
            # if time base is within 2 seconds of that of the last sample in
            # the database, correct back to that time base
            if delta_timestamp == 0:
                if opts.verbose:
                    print("Exactly synced with database time base")
            elif -2 <= delta_timestamp <= 2:
                if opts.verbose:
                    print("Replacing with existing time base: {0} -> {1}".format(
                        db_counter, datetime.fromtimestamp(db_timestamp, tz=timezone.utc)))
                for point in gstate.deferred_points:
                    db_timestamp += 1
                    if point["time"] + delta_timestamp == db_timestamp:
                        point["time"] = db_timestamp
                    else:
                        # lost time sync when recording data, leave the rest
                        break
                else:
                    gstate.timestamp = db_timestamp
            else:
                if opts.verbose:
                    print("Database time base out of sync by {0} seconds".format(delta_timestamp))

    gstate.points.extend(gstate.deferred_points)
    gstate.deferred_points.clear()


def loop_body(opts, gstate):
    fields = {"status": {}, "ping_stats": {}, "usage": {}}

    def cb_add_item(key, val, category):
        fields[category][key] = val

    def cb_add_sequence(key, val, category, start):
        for i, subval in enumerate(val, start=start):
            fields[category]["{0}_{1}".format(key, i)] = subval

    def cb_add_bulk(bulk, count, timestamp, counter):
        if gstate.start_timestamp is None:
            gstate.start_timestamp = timestamp
            gstate.start_counter = counter
        points = gstate.points if gstate.timebase_synced else gstate.deferred_points
        for i in range(count):
            timestamp += 1
            points.append({
                "measurement": BULK_MEASUREMENT,
                "tags": {
                    "id": gstate.dish_id
                },
                "time": timestamp,
                "fields": {key: val[i] for key, val in bulk.items() if val[i] is not None},
            })
        if points:
            # save off counter value for script restart
            points[-1]["fields"]["counter"] = counter + count

    now = time.time()
    rc = dish_common.get_data(opts, gstate, cb_add_item, cb_add_sequence, add_bulk=cb_add_bulk)
    if rc:
        return rc

    for category in fields:
        if fields[category]:
            gstate.points.append({
                "measurement": "spacex.starlink.user_terminal." + category,
                "tags": {
                    "id": gstate.dish_id
                },
                "time": int(now),
                "fields": fields[category],
            })

    # This is here and not before the points being processed because if the
    # query previously failed, there will be points that were processed in
    # a prior loop. This avoids having to handle that as a special case.
    if opts.bulk_mode and not gstate.timebase_synced:
        sync_timebase(opts, gstate)

    if opts.verbose:
        print("Data points queued: " + str(len(gstate.points)))

    if len(gstate.points) >= FLUSH_LIMIT:
        return flush_points(opts, gstate)

    return 0


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    gstate = dish_common.GlobalState(target=opts.target)
    gstate.points = []
    gstate.deferred_points = []
    gstate.timebase_synced = opts.skip_query
    gstate.start_timestamp = None
    gstate.start_counter = None

    if "verify_ssl" in opts.icargs and not opts.icargs["verify_ssl"]:
        # user has explicitly said be insecure, so don't warn about it
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        # attempt to hack around breakage between influxdb-python client and 2.0 server:
        gstate.influx_client = InfluxDBClient(**opts.icargs, headers={"Accept": "application/json"})
    except TypeError:
        # ...unless influxdb-python package version is too old
        gstate.influx_client = InfluxDBClient(**opts.icargs)

    try:
        next_loop = time.monotonic()
        while True:
            rc = loop_body(opts, gstate)
            if opts.loop_interval > 0.0:
                now = time.monotonic()
                next_loop = max(next_loop + opts.loop_interval, now)
                time.sleep(next_loop - now)
            else:
                break
    except Terminated:
        pass
    finally:
        if gstate.points:
            rc = flush_points(opts, gstate)
        gstate.influx_client.close()
        gstate.shutdown()

    sys.exit(rc)


if __name__ == '__main__':
    main()
