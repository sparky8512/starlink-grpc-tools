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

import paho.mqtt.publish

import starlink_grpc

arg_error = False

try:
    opts, args = getopt.getopt(sys.argv[1:], "ahn:p:rs:vU:P:")
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
host = host_default
port = None
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
                host = arg
            elif opt == "-p":
                port = int(arg)
            elif opt == "-r":
                run_lengths = True
            elif opt == "-s":
                samples = int(arg)
            elif opt == "-v":
                verbose = True
            elif opt == "-P":
                password = arg
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
    print("    -P: Set password for username/password authentication")
    print("    -U: Set username for authentication")
    sys.exit(1 if arg_error else 0)

dish_id = starlink_grpc.get_id()

if dish_id is None:
    if verbose:
        print("Unable to connect to Starlink user terminal")
    sys.exit(1)

g_stats, pd_stats, rl_stats = starlink_grpc.history_ping_stats(samples, verbose)

if g_stats is None:
    # verbose output already happened, so just bail.
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

optargs = {}
if username is not None:
    auth = {"username": username}
    if password is not None:
        auth["password"] = password
    optargs["auth"] = auth
if port is not None:
    optargs["port"] = port
paho.mqtt.publish.multiple(msgs, hostname=host, client_id=dish_id, **optargs)
