#!/usr/bin/env python3
"""Write Starlink user terminal data to an InfluxDB 2.x database.

This script pulls the current status info and/or metrics computed from the
history data and writes them to the specified InfluxDB 2.x database either once
or in a periodic loop.

Data will be written into the requested database with the following
measurement / series names:

: spacex.starlink.user_terminal.status : Current status data
: spacex.starlink.user_terminal.history : Bulk history data
: spacex.starlink.user_terminal.ping_stats : Ping history statistics
: spacex.starlink.user_terminal.usage : Usage history statistics
: spacex.starlink.user_terminal.power : Power history statistics

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

from influxdb_client import InfluxDBClient, WriteOptions, WritePrecision

import dish_common

URL_DEFAULT = "http://localhost:8086"
BUCKET_DEFAULT = "starlinkstats"
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
    parser = dish_common.create_arg_parser(
        output_description="write it to an InfluxDB 2.x database")

    group = parser.add_argument_group(title="InfluxDB 2.x database options")
    group.add_argument("-u",
                       "--url",
                       default=URL_DEFAULT,
                       dest="url",
                       help="URL of the InfluxDB 2.x server, default: " + URL_DEFAULT)
    group.add_argument("-T", "--token", help="Token to access the bucket")
    group.add_argument("-B",
                       "--bucket",
                       default=BUCKET_DEFAULT,
                       help="Bucket name to use, default: " + BUCKET_DEFAULT)
    group.add_argument("-O", "--org", help="Organisation name")
    group.add_argument("-k",
                       "--skip-query",
                       action="store_true",
                       help="Skip querying for prior sample write point in bulk mode")
    group.add_argument("-C",
                       "--ca-cert",
                       dest="ssl_ca_cert",
                       help="Use specified CA cert to verify HTTPS server",
                       metavar="FILENAME")
    group.add_argument("-I",
                       "--insecure",
                       action="store_false",
                       dest="verify_ssl",
                       help="Disable certificate verification of HTTPS server (INSECURE!)")

    env_map = (
        ("INFLUXDB_URL", "url"),
        ("INFLUXDB_TOKEN", "token"),
        ("INFLUXDB_Bucket", "bucket"),
        ("INFLUXDB_ORG", "org"),
        ("INFLUXDB_SSL", "verify_ssl"),
    )
    env_defaults = {}
    for var, opt in env_map:
        # check both set and not empty string
        val = os.environ.get(var)
        if val:
            if var == "INFLUXDB_SSL":
                if val == "insecure":
                    env_defaults[opt] = False
                elif val == "secure":
                    env_defaults[opt] = True
                else:
                    env_defaults["ssl_ca_cert"] = val
            else:
                env_defaults[opt] = val
    parser.set_defaults(**env_defaults)

    opts = dish_common.run_arg_parser(parser, need_id=True)

    opts.icargs = {}
    for key in ["url", "token", "bucket", "org", "verify_ssl", "ssl_ca_cert"]:
        val = getattr(opts, key)
        if val is not None:
            opts.icargs[key] = val

    if (not opts.verify_ssl
            or opts.ssl_ca_cert is not None) and not opts.url.lower().startswith("https:"):
        parser.error("SSL options only apply to HTTPS URLs")

    return opts


def flush_points(opts, gstate):
    try:
        write_api = gstate.influx_client.write_api(
            write_options=WriteOptions(batch_size=len(gstate.points),
                                       flush_interval=10_000,
                                       jitter_interval=2_000,
                                       retry_interval=5_000,
                                       max_retries=5,
                                       max_retry_delay=30_000,
                                       exponential_base=2))
        while len(gstate.points) > MAX_BATCH:
            write_api.write(record=gstate.points[:MAX_BATCH],
                            write_precision=WritePrecision.S,
                            bucket=opts.bucket)
            if opts.verbose:
                print("Data points written: " + str(MAX_BATCH))
            del gstate.points[:MAX_BATCH]

        if gstate.points:
            write_api.write(record=gstate.points,
                            write_precision=WritePrecision.S,
                            bucket=opts.bucket)
            if opts.verbose:
                print("Data points written: " + str(len(gstate.points)))
            gstate.points.clear()
        write_api.flush()
        write_api.close()
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


def query_counter(opts, gstate, start, end):
    query_api = gstate.influx_client.query_api()
    result = query_api.query('''
    from(bucket: "{0}")
        |> range(start: {1}, stop: {2})
        |> filter(fn: (r) => r["_measurement"] == "{3}")
        |> filter(fn: (r) => r["_field"] == "counter")
        |> last()
        |> yield(name: "last")
        '''.format(opts.bucket, str(start), str(end), BULK_MEASUREMENT))
    if result:
        counter = result[0].records[0]["_value"]
        timestamp = result[0].records[0]["_time"].timestamp()
        if counter and timestamp:
            return int(counter), int(timestamp)

    return None, 0


def sync_timebase(opts, gstate):
    try:
        db_counter, db_timestamp = query_counter(opts, gstate, gstate.start_timestamp,
                                                 gstate.timestamp)
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


def loop_body(opts, gstate, shutdown=False):
    fields = {"status": {}, "ping_stats": {}, "usage": {}, "power": {}}

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

    rc, status_ts, hist_ts = dish_common.get_data(opts,
                                                  gstate,
                                                  cb_add_item,
                                                  cb_add_sequence,
                                                  add_bulk=cb_add_bulk,
                                                  flush_history=shutdown)
    if rc:
        return rc

    for category, cat_fields in fields.items():
        if cat_fields:
            timestamp = status_ts if category == "status" else hist_ts
            gstate.points.append({
                "measurement": "spacex.starlink.user_terminal." + category,
                "tags": {
                    "id": gstate.dish_id
                },
                "time": timestamp,
                "fields": cat_fields,
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
    gstate.influx_client = InfluxDBClient(**opts.icargs)

    rc = 0
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
    except (KeyboardInterrupt, Terminated):
        pass
    finally:
        loop_body(opts, gstate, shutdown=True)
        if gstate.points:
            rc = flush_points(opts, gstate)
        gstate.influx_client.close()
        gstate.shutdown()

    sys.exit(rc)


if __name__ == "__main__":
    main()
