#!/usr/bin/python3
"""Write Starlink user terminal data to a sqlite database.

This script pulls the current status info and/or metrics computed from the
history data and writes them to the specified sqlite database either once or
in a periodic loop.

Requested data will be written into the following tables:

: status : Current status data
: history : Bulk history data
: ping_stats : Ping history statistics
: usage : Usage history statistics

Array data is currently written to the database as text strings of comma-
separated values, which may not be the best method for some use cases. If you
find yourself wishing they were handled better, please open a feature request
at https://github.com/sparky8512/starlink-grpc-tools/issues explaining the use
case and how you would rather see it. This only affects a few fields, since
most of the useful data is not in arrays.

Note that using this script to record the alert_detail group mode will tend to
trip schema-related errors when new alert types are added to the dish
software. The error message will include something like "table status has no
column named alert_foo", where "foo" is the newly added alert type. To work
around this rare occurrence, you can pass the -f option to force a schema
update. Alternatively, instead of using the alert_detail mode, you can use the
alerts bitmask in the status group.

NOTE: The Starlink user terminal does not include time values with its
history or status data, so this script uses current system time to compute
the timestamps it writes into the database. It is recommended to run this
script on a host that has its system clock synced via NTP. Otherwise, the
timestamps may get out of sync with real time.
"""

from datetime import datetime
from datetime import timezone
from itertools import repeat
import logging
import signal
import sqlite3
import sys
import time

import dish_common
import starlink_grpc

SCHEMA_VERSION = 2


class Terminated(Exception):
    pass


def handle_sigterm(signum, frame):
    # Turn SIGTERM into an exception so main loop can clean up
    raise Terminated


def parse_args():
    parser = dish_common.create_arg_parser(output_description="write it to a sqlite database")

    parser.add_argument("database", help="Database file to use")

    group = parser.add_argument_group(title="sqlite database options")
    group.add_argument("-f",
                       "--force",
                       action="store_true",
                       help="Force schema conversion, even if it results in downgrade; may "
                       "result in discarded data")
    group.add_argument("-k",
                       "--skip-query",
                       action="store_true",
                       help="Skip querying for prior sample write point in bulk mode")

    opts = dish_common.run_arg_parser(parser, need_id=True)

    opts.skip_query |= opts.no_counter

    return opts


def query_counter(opts, gstate, column, table):
    now = time.time()
    cur = gstate.sql_conn.cursor()
    cur.execute(
        'SELECT "time", "{0}" FROM "{1}" WHERE "time"<? AND "id"=? '
        'ORDER BY "time" DESC LIMIT 1'.format(column, table), (now, gstate.dish_id))
    row = cur.fetchone()
    cur.close()

    if row and row[0] and row[1]:
        if opts.verbose:
            print("Existing time base: {0} -> {1}".format(
                row[1], datetime.fromtimestamp(row[0], tz=timezone.utc)))
        return row
    else:
        return 0, None


def loop_body(opts, gstate):
    tables = {"status": {}, "ping_stats": {}, "usage": {}}
    hist_cols = ["time", "id"]
    hist_rows = []

    def cb_add_item(key, val, category):
        tables[category][key] = val

    def cb_add_sequence(key, val, category, start):
        tables[category][key] = ",".join(str(subv) if subv is not None else "" for subv in val)

    def cb_add_bulk(bulk, count, timestamp, counter):
        if len(hist_cols) == 2:
            hist_cols.extend(bulk.keys())
            hist_cols.append("counter")
        for i in range(count):
            timestamp += 1
            counter += 1
            row = [timestamp, gstate.dish_id]
            row.extend(val[i] for val in bulk.values())
            row.append(counter)
            hist_rows.append(row)

    now = int(time.time())
    rc = dish_common.get_status_data(opts, gstate, cb_add_item, cb_add_sequence)

    if opts.history_stats_mode and not rc:
        if gstate.counter_stats is None and not opts.skip_query and opts.samples < 0:
            _, gstate.counter_stats = query_counter(opts, gstate, "end_counter", "ping_stats")
        rc = dish_common.get_history_stats(opts, gstate, cb_add_item, cb_add_sequence)

    if opts.bulk_mode and not rc:
        if gstate.counter is None and not opts.skip_query and opts.bulk_samples < 0:
            gstate.timestamp, gstate.counter = query_counter(opts, gstate, "counter", "history")
        rc = dish_common.get_bulk_data(opts, gstate, cb_add_bulk)

    rows_written = 0

    try:
        cur = gstate.sql_conn.cursor()
        for category, fields in tables.items():
            if fields:
                sql = 'INSERT OR REPLACE INTO "{0}" ("time","id",{1}) VALUES ({2})'.format(
                    category, ",".join('"' + x + '"' for x in fields),
                    ",".join(repeat("?",
                                    len(fields) + 2)))
                values = [now, gstate.dish_id]
                values.extend(fields.values())
                cur.execute(sql, values)
                rows_written += 1

        if hist_rows:
            sql = 'INSERT OR REPLACE INTO "history" ({0}) VALUES({1})'.format(
                ",".join('"' + x + '"' for x in hist_cols), ",".join(repeat("?", len(hist_cols))))
            cur.executemany(sql, hist_rows)
            rows_written += len(hist_rows)

        cur.close()
        gstate.sql_conn.commit()
    except sqlite3.OperationalError as e:
        # these are not necessarily fatal, but also not much can do about
        logging.error("Unexpected error from database, discarding data: %s", e)
        rc = 1
    else:
        if opts.verbose:
            print("Rows written to db:", rows_written)

    return rc


def ensure_schema(opts, conn, context):
    cur = conn.cursor()
    cur.execute("PRAGMA user_version")
    version = cur.fetchone()
    if version and version[0] == SCHEMA_VERSION and not opts.force:
        cur.close()
        return 0

    try:
        if not version or not version[0]:
            if opts.verbose:
                print("Initializing new database")
            create_tables(conn, context, "")
        elif version[0] > SCHEMA_VERSION and not opts.force:
            logging.error("Cowardly refusing to downgrade from schema version %s", version[0])
            return 1
        else:
            print("Converting from schema version:", version[0])
            convert_tables(conn, context)
        cur.execute("PRAGMA user_version={0}".format(SCHEMA_VERSION))
        conn.commit()
        return 0
    except starlink_grpc.GrpcError as e:
        dish_common.conn_error(opts, "Failure reflecting status fields: %s", str(e))
        return 1
    finally:
        cur.close()


def create_tables(conn, context, suffix):
    tables = {}
    name_groups = starlink_grpc.status_field_names(context=context)
    type_groups = starlink_grpc.status_field_types(context=context)
    tables["status"] = zip(name_groups, type_groups)

    name_groups = starlink_grpc.history_stats_field_names()
    type_groups = starlink_grpc.history_stats_field_types()
    tables["ping_stats"] = zip(name_groups[0:5], type_groups[0:5])
    tables["usage"] = ((name_groups[5], type_groups[5]),)

    name_groups = starlink_grpc.history_bulk_field_names()
    type_groups = starlink_grpc.history_bulk_field_types()
    tables["history"] = ((name_groups[1], type_groups[1]), (["counter"], [int]))

    def sql_type(type_class):
        if issubclass(type_class, float):
            return "REAL"
        if issubclass(type_class, bool):
            # advisory only, stores as int:
            return "BOOLEAN"
        if issubclass(type_class, int):
            return "INTEGER"
        if issubclass(type_class, str):
            return "TEXT"
        raise TypeError

    column_info = {}
    cur = conn.cursor()
    for table, group_pairs in tables.items():
        column_names = ["time", "id"]
        columns = ['"time" INTEGER NOT NULL', '"id" TEXT NOT NULL']
        for name_group, type_group in group_pairs:
            for name_item, type_item in zip(name_group, type_group):
                name_item = dish_common.BRACKETS_RE.match(name_item).group(1)
                if name_item != "id":
                    columns.append('"{0}" {1}'.format(name_item, sql_type(type_item)))
                    column_names.append(name_item)
        cur.execute('DROP TABLE IF EXISTS "{0}{1}"'.format(table, suffix))
        sql = 'CREATE TABLE "{0}{1}" ({2}, PRIMARY KEY("time","id"))'.format(
            table, suffix, ", ".join(columns))
        cur.execute(sql)
        column_info[table] = column_names
    cur.close()

    return column_info


def convert_tables(conn, context):
    new_column_info = create_tables(conn, context, "_new")
    conn.row_factory = sqlite3.Row
    old_cur = conn.cursor()
    new_cur = conn.cursor()
    for table, new_columns in new_column_info.items():
        old_cur.execute('SELECT * FROM "{0}"'.format(table))
        old_columns = set(x[0] for x in old_cur.description)
        new_columns = tuple(x for x in new_columns if x in old_columns)
        sql = 'INSERT OR REPLACE INTO "{0}_new" ({1}) VALUES ({2})'.format(
            table, ",".join('"' + x + '"' for x in new_columns),
            ",".join(repeat("?", len(new_columns))))
        new_cur.executemany(sql, (tuple(row[col] for col in new_columns) for row in old_cur))
        new_cur.execute('DROP TABLE "{0}"'.format(table))
        new_cur.execute('ALTER TABLE {0}_new RENAME TO {0}'.format(table))
    old_cur.close()
    new_cur.close()
    conn.row_factory = None


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    gstate = dish_common.GlobalState(target=opts.target)
    gstate.points = []
    gstate.deferred_points = []

    signal.signal(signal.SIGTERM, handle_sigterm)
    gstate.sql_conn = sqlite3.connect(opts.database)

    rc = 0
    try:
        rc = ensure_schema(opts, gstate.sql_conn, gstate.context)
        if rc:
            sys.exit(rc)
        next_loop = time.monotonic()
        while True:
            rc = loop_body(opts, gstate)
            if opts.loop_interval > 0.0:
                now = time.monotonic()
                next_loop = max(next_loop + opts.loop_interval, now)
                time.sleep(next_loop - now)
            else:
                break
    except sqlite3.Error as e:
        logging.error("Database error: %s", e)
        rc = 1
    except Terminated:
        pass
    finally:
        gstate.sql_conn.close()
        gstate.shutdown()

    sys.exit(rc)


if __name__ == '__main__':
    main()
