#!/usr/bin/python3
######################################################################
#
# Output get_status info in CSV format.
#
# This script pulls the current status once and prints to stdout.
#
######################################################################

import datetime
import sys
import getopt
import logging

import grpc

import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc

arg_error = False

try:
    opts, args = getopt.getopt(sys.argv[1:], "hH")
except getopt.GetoptError as err:
    print(str(err))
    arg_error = True

print_usage = False
print_header = False

if not arg_error:
    if len(args) > 0:
        arg_error = True
    else:
        for opt, arg in opts:
            if opt == "-h":
                print_usage = True
            elif opt == "-H":
                print_header = True

if print_usage or arg_error:
    print("Usage: " + sys.argv[0] + " [options...]")
    print("Options:")
    print("    -h: Be helpful")
    print("    -H: print CSV header instead of parsing file")
    sys.exit(1 if arg_error else 0)

logging.basicConfig(format="%(levelname)s: %(message)s")

if print_header:
    header = [
        "datetimestamp_utc",
        "hardware_version",
        "software_version",
        "state",
        "uptime",
        "snr",
        "seconds_to_first_nonempty_slot",
        "pop_ping_drop_rate",
        "downlink_throughput_bps",
        "uplink_throughput_bps",
        "pop_ping_latency_ms",
        "alerts",
        "fraction_obstructed",
        "currently_obstructed",
        "seconds_obstructed"
    ]
    header.extend("wedges_fraction_obstructed_" + str(x) for x in range(12))
    print(",".join(header))
    sys.exit(0)

try:
    with grpc.insecure_channel("192.168.100.1:9200") as channel:
        stub = spacex.api.device.device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(spacex.api.device.device_pb2.Request(get_status={}))
except grpc.RpcError:
    logging.error("Failed getting status info")
    sys.exit(1)

timestamp = datetime.datetime.utcnow()

status = response.dish_get_status

# More alerts may be added in future, so rather than list them individually,
# build a bit field based on field numbers of the DishAlerts message.
alert_bits = 0
for alert in status.alerts.ListFields():
    alert_bits |= (1 if alert[1] else 0) << (alert[0].number - 1)

csv_data = [
    timestamp.replace(microsecond=0).isoformat(),
    status.device_info.id,
    status.device_info.hardware_version,
    status.device_info.software_version,
    spacex.api.device.dish_pb2.DishState.Name(status.state)
]
csv_data.extend(str(x) for x in [
    status.device_state.uptime_s,
    status.snr,
    status.seconds_to_first_nonempty_slot,
    status.pop_ping_drop_rate,
    status.downlink_throughput_bps,
    status.uplink_throughput_bps,
    status.pop_ping_latency_ms,
    alert_bits,
    status.obstruction_stats.fraction_obstructed,
    status.obstruction_stats.currently_obstructed,
    status.obstruction_stats.last_24h_obstructed_s
])
csv_data.extend(str(x) for x in status.obstruction_stats.wedge_abs_fraction_obstructed)
print(",".join(csv_data))
