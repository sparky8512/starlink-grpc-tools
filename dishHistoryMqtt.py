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

import sys
import getopt
import logging

try:
    import ssl
    ssl_ok = True
except ImportError:
    ssl_ok = False

import paho.mqtt.publish

import starlink_grpc

arg_error = False

try:
    opts, args = getopt.getopt(sys.argv[1:], "ahn:p:rs:vC:ISP:U:")
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
    print("    -s <num>: Number of data samples to parse, default: " + str(samples_default))
    print("    -v: Be verbose")
    print("    -C <filename>: Enable SSL/TLS using specified CA cert to verify broker")
    print("    -I: Enable SSL/TLS but disable certificate verification (INSECURE!)")
    print("    -P: Set password for username/password authentication")
    print("    -S: Enable SSL/TLS using default CA cert")
    print("    -U: Set username for authentication")
    sys.exit(1 if arg_error else 0)

logging.basicConfig(format="%(levelname)s: %(message)s")

try:
    dish_id = starlink_grpc.get_id()
except starlink_grpc.GrpcError as e:
    logging.error("Failure getting dish ID: " + str(e))
    sys.exit(1)

try:
    g_stats, pd_stats, rl_stats = starlink_grpc.history_ping_stats(samples, verbose)
except starlink_grpc.GrpcError as e:
    logging.error("Failure getting ping stats: " + str(e))
    sys.exit(1)

topic_prefix = "starlink/dish_ping_stats/" + dish_id + "/"
msgs = [(topic_prefix + k, v, 0, False) for k, v in g_stats.items()]
msgs.extend([(topic_prefix + k, v, 0, False) for k, v in pd_stats.items()])
if run_lengths:
    for k, v in rl_stats.items():
        if k.startswith("run_"):
            msgs.append((topic_prefix + k, ",".join(str(x) for x in v), 0, False))
        else:
            msgs.append((topic_prefix + k, v, 0, False))

if username is not None:
    mqargs["auth"] = {"username": username}
    if password is not None:
        mqargs["auth"]["password"] = password

try:
    paho.mqtt.publish.multiple(msgs, client_id=dish_id, **mqargs)
except Exception as e:
    logging.error("Failed publishing to MQTT broker: " + str(e))
    sys.exit(1)
