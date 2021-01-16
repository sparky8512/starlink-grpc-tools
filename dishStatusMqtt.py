#!/usr/bin/python3
######################################################################
#
# Publish Starlink user terminal status info to a MQTT broker.
#
# This script pulls the current status and publishes it to the
# specified MQTT broker either once or in a periodic loop.
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

import grpc
import paho.mqtt.publish

import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc


def main():
    arg_error = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hn:p:t:vC:ISP:U:")
    except getopt.GetoptError as err:
        print(str(err))
        arg_error = True

    print_usage = False
    verbose = False
    default_loop_time = 0
    loop_time = default_loop_time
    host_default = "localhost"
    mqargs = {"hostname": host_default}
    username = None
    password = None

    if not arg_error:
        if len(args) > 0:
            arg_error = True
        else:
            for opt, arg in opts:
                if opt == "-h":
                    print_usage = True
                elif opt == "-n":
                    mqargs["hostname"] = arg
                elif opt == "-p":
                    mqargs["port"] = int(arg)
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
        print("    -h: Be helpful")
        print("    -n <name>: Hostname of MQTT broker, default: " + host_default)
        print("    -p <num>: Port number to use on MQTT broker")
        print("    -t <num>: Loop interval in seconds or 0 for no loop, default: " +
              str(default_loop_time))
        print("    -v: Be verbose")
        print("    -C <filename>: Enable SSL/TLS using specified CA cert to verify broker")
        print("    -I: Enable SSL/TLS but disable certificate verification (INSECURE!)")
        print("    -P: Set password for username/password authentication")
        print("    -S: Enable SSL/TLS using default CA cert")
        print("    -U: Set username for authentication")
        sys.exit(1 if arg_error else 0)

    if username is not None:
        mqargs["auth"] = {"username": username}
        if password is not None:
            mqargs["auth"]["password"] = password

    logging.basicConfig(format="%(levelname)s: %(message)s")

    class GlobalState:
        pass

    gstate = GlobalState()
    gstate.dish_id = None

    def conn_error(msg):
        # Connection errors that happen in an interval loop are not critical
        # failures, but are interesting enough to print in non-verbose mode.
        if loop_time > 0:
            print(msg)
        else:
            logging.error(msg)

    def loop_body():
        try:
            with grpc.insecure_channel("192.168.100.1:9200") as channel:
                stub = spacex.api.device.device_pb2_grpc.DeviceStub(channel)
                response = stub.Handle(spacex.api.device.device_pb2.Request(get_status={}))

            status = response.dish_get_status

            # More alerts may be added in future, so rather than list them individually,
            # build a bit field based on field numbers of the DishAlerts message.
            alert_bits = 0
            for alert in status.alerts.ListFields():
                alert_bits |= (1 if alert[1] else 0) << (alert[0].number - 1)

            gstate.dish_id = status.device_info.id
            topic_prefix = "starlink/dish_status/" + gstate.dish_id + "/"
            msgs = [
                (topic_prefix + "hardware_version", status.device_info.hardware_version, 0, False),
                (topic_prefix + "software_version", status.device_info.software_version, 0, False),
                (topic_prefix + "state", spacex.api.device.dish_pb2.DishState.Name(status.state), 0, False),
                (topic_prefix + "uptime", status.device_state.uptime_s, 0, False),
                (topic_prefix + "snr", status.snr, 0, False),
                (topic_prefix + "seconds_to_first_nonempty_slot", status.seconds_to_first_nonempty_slot, 0, False),
                (topic_prefix + "pop_ping_drop_rate", status.pop_ping_drop_rate, 0, False),
                (topic_prefix + "downlink_throughput_bps", status.downlink_throughput_bps, 0, False),
                (topic_prefix + "uplink_throughput_bps", status.uplink_throughput_bps, 0, False),
                (topic_prefix + "pop_ping_latency_ms", status.pop_ping_latency_ms, 0, False),
                (topic_prefix + "alerts", alert_bits, 0, False),
                (topic_prefix + "fraction_obstructed", status.obstruction_stats.fraction_obstructed, 0, False),
                (topic_prefix + "currently_obstructed", status.obstruction_stats.currently_obstructed, 0, False),
                # While the field name for this one implies it covers 24 hours, the
                # empirical evidence suggests it only covers 12 hours. It also resets
                # on dish reboot, so may not cover that whole period. Rather than try
                # to convey that complexity in the topic label, just be a bit vague:
                (topic_prefix + "seconds_obstructed", status.obstruction_stats.last_24h_obstructed_s, 0, False),
                (topic_prefix + "wedges_fraction_obstructed", ",".join(str(x) for x in status.obstruction_stats.wedge_abs_fraction_obstructed), 0, False),
            ]
        except grpc.RpcError:
            if gstate.dish_id is None:
                conn_error("Dish unreachable and ID unknown, so not recording state")
                return 1
            if verbose:
                print("Dish unreachable")
            topic_prefix = "starlink/dish_status/" + gstate.dish_id + "/"
            msgs = [(topic_prefix + "state", "DISH_UNREACHABLE", 0, False)]

        try:
            paho.mqtt.publish.multiple(msgs, client_id=gstate.dish_id, **mqargs)
            if verbose:
                print("Successfully published to MQTT broker")
        except Exception as e:
            conn_error("Failed publishing to MQTT broker: " + str(e))
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
