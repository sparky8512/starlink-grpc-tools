#!/usr/bin/python3
"""Publish Starlink user terminal data to a MQTT broker.

This script pulls the current status info and/or metrics computed from the
history data and publishes them to the specified MQTT broker either once or
in a periodic loop.

Data will be published to the following topic names:

: starlink/dish_status/*id_value*/*field_name* : Current status data
: starlink/dish_ping_stats/*id_value*/*field_name* : Ping history statistics
: starlink/dish_usage/*id_value*/*field_name* : Usage history statistics

Where *id_value* is the *id* value from the dish status information.
"""

import logging
import sys
import time

try:
    import ssl
    ssl_ok = True
except ImportError:
    ssl_ok = False

import paho.mqtt.publish

import dish_common

HOST_DEFAULT = "localhost"


def parse_args():
    parser = dish_common.create_arg_parser(output_description="publish it to a MQTT broker",
                                           bulk_history=False)

    group = parser.add_argument_group(title="MQTT broker options")
    group.add_argument("-n",
                       "--hostname",
                       default=HOST_DEFAULT,
                       help="Hostname of MQTT broker, default: " + HOST_DEFAULT)
    group.add_argument("-p", "--port", type=int, help="Port number to use on MQTT broker")
    group.add_argument("-P", "--password", help="Set password for username/password authentication")
    group.add_argument("-U", "--username", help="Set username for authentication")
    if ssl_ok:

        def wrap_ca_arg(arg):
            return {"ca_certs": arg}

        group.add_argument("-C",
                           "--ca-cert",
                           type=wrap_ca_arg,
                           dest="tls",
                           help="Enable SSL/TLS using specified CA cert to verify broker",
                           metavar="FILENAME")
        group.add_argument("-I",
                           "--insecure",
                           action="store_const",
                           const={"cert_reqs": ssl.CERT_NONE},
                           dest="tls",
                           help="Enable SSL/TLS but disable certificate verification (INSECURE!)")
        group.add_argument("-S",
                           "--secure",
                           action="store_const",
                           const={},
                           dest="tls",
                           help="Enable SSL/TLS using default CA cert")
    else:
        parser.epilog += "\nSSL support options not available due to missing ssl module"

    opts = dish_common.run_arg_parser(parser, need_id=True)

    if opts.username is None and opts.password is not None:
        parser.error("Password authentication requires username to be set")

    opts.mqargs = {}
    for key in ["hostname", "port", "tls"]:
        val = getattr(opts, key)
        if val is not None:
            opts.mqargs[key] = val

    if opts.username is not None:
        opts.mqargs["auth"] = {"username": opts.username}
        if opts.password is not None:
            opts.mqargs["auth"]["password"] = opts.password

    return opts


def loop_body(opts, gstate):
    msgs = []

    def cb_add_item(key, val, category):
        msgs.append(("starlink/dish_{0}/{1}/{2}".format(category, gstate.dish_id,
                                                        key), val, 0, False))

    def cb_add_sequence(key, val, category, _):
        msgs.append(
            ("starlink/dish_{0}/{1}/{2}".format(category, gstate.dish_id,
                                                key), ",".join(str(x) for x in val), 0, False))

    rc = dish_common.get_data(opts, gstate, cb_add_item, cb_add_sequence)

    if msgs:
        try:
            paho.mqtt.publish.multiple(msgs, client_id=gstate.dish_id, **opts.mqargs)
            if opts.verbose:
                print("Successfully published to MQTT broker")
        except Exception as e:
            dish_common.conn_error(opts, "Failed publishing to MQTT broker: %s", str(e))
            rc = 1

    return rc


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    gstate = dish_common.GlobalState(target=opts.target)

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
    finally:
        gstate.shutdown()

    sys.exit(rc)


if __name__ == '__main__':
    main()
