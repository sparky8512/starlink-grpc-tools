#!/usr/bin/python3
######################################################################
#
# Publish Starlink user terminal packet loss statistics to a MQTT
# broker.
#
# This script examines the most recent samples from the history data,
# computes several different metrics related to packet loss, and
# publishes those to the specified MQTT broker.
#
######################################################################

import getopt
import logging
import sys
import time

try:
    import ssl
    ssl_ok = True
except ImportError:
    ssl_ok = False

import paho.mqtt.publish

import starlink_grpc


def main():
    arg_error = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], "ahn:p:rs:t:vC:ISP:U:")
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
    mqargs = {"hostname": host_default}
    username = None
    password = None

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
                    mqargs["hostname"] = arg
                elif opt == "-p":
                    mqargs["port"] = int(arg)
                elif opt == "-r":
                    run_lengths = True
                elif opt == "-s":
                    samples = int(arg)
                elif opt == "-t":
                    loop_time = float(arg)
                elif opt == "-v":
                    verbose = True
                elif opt == "-C":
                    mqargs["tls"] = {"ca_certs": arg}
                elif opt == "-I":
                    if ssl_ok:
                        mqargs["tls"] = {"cert_reqs": ssl.CERT_NONE}
                    else:
                        print("No SSL support found")
                        sys.exit(1)
                elif opt == "-P":
                    password = arg
                elif opt == "-S":
                    mqargs["tls"] = {}
                elif opt == "-U":
                    username = arg

    if username is None and password is not None:
        print("Password authentication requires username to be set")
        arg_error = True

    if print_usage or arg_error:
        print("Usage: " + sys.argv[0] + " [options...]")
        print("Options:")
        print("    -a: Parse all valid samples")
        print("    -h: Be helpful")
        print("    -n <name>: Hostname of MQTT broker, default: " + host_default)
        print("    -p <num>: Port number to use on MQTT broker")
        print("    -r: Include ping drop run length stats")
        print("    -s <num>: Number of data samples to parse, default: loop interval,")
        print("              if set, else " + str(samples_default))
        print("    -t <num>: Loop interval in seconds or 0 for no loop, default: " +
              str(default_loop_time))
        print("    -v: Be verbose")
        print("    -C <filename>: Enable SSL/TLS using specified CA cert to verify broker")
        print("    -I: Enable SSL/TLS but disable certificate verification (INSECURE!)")
        print("    -P: Set password for username/password authentication")
        print("    -S: Enable SSL/TLS using default CA cert")
        print("    -U: Set username for authentication")
        sys.exit(1 if arg_error else 0)

    if samples is None:
        samples = int(loop_time) if loop_time > 0 else samples_default

    if username is not None:
        mqargs["auth"] = {"username": username}
        if password is not None:
            mqargs["auth"]["password"] = password

    logging.basicConfig(format="%(levelname)s: %(message)s")

    class GlobalState:
        pass

    gstate = GlobalState()
    gstate.dish_id = None

    def conn_error(msg, *args):
        # Connection errors that happen in an interval loop are not critical
        # failures, but are interesting enough to print in non-verbose mode.
        if loop_time > 0:
            print(msg % args)
        else:
            logging.error(msg, *args)

    def loop_body():
        if gstate.dish_id is None:
            try:
                gstate.dish_id = starlink_grpc.get_id()
                if verbose:
                    print("Using dish ID: " + gstate.dish_id)
            except starlink_grpc.GrpcError as e:
                conn_error("Failure getting dish ID: %s", str(e))
                return 1

        try:
            g_stats, pd_stats, rl_stats = starlink_grpc.history_ping_stats(samples, verbose)
        except starlink_grpc.GrpcError as e:
            conn_error("Failure getting ping stats: %s", str(e))
            return 1

        topic_prefix = "starlink/dish_ping_stats/" + gstate.dish_id + "/"
        msgs = [(topic_prefix + k, v, 0, False) for k, v in g_stats.items()]
        msgs.extend([(topic_prefix + k, v, 0, False) for k, v in pd_stats.items()])
        if run_lengths:
            for k, v in rl_stats.items():
                if k.startswith("run_"):
                    msgs.append((topic_prefix + k, ",".join(str(x) for x in v), 0, False))
                else:
                    msgs.append((topic_prefix + k, v, 0, False))

        try:
            paho.mqtt.publish.multiple(msgs, client_id=gstate.dish_id, **mqargs)
            if verbose:
                print("Successfully published to MQTT broker")
        except Exception as e:
            conn_error("Failed publishing to MQTT broker: %s", str(e))
            return 1

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
