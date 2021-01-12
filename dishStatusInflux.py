#!/usr/bin/python3
######################################################################
#
# Write Starlink user terminal status info to an InfluxDB database.
#
# This script will poll current status and write it to the specified
# InfluxDB database either once or in a periodic loop.
#
######################################################################

import time
import os
import sys
import getopt

import warnings
from influxdb import InfluxDBClient
from influxdb import SeriesHelper

import grpc

import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc

arg_error = False

try:
    opts, args = getopt.getopt(sys.argv[1:], "hn:p:t:vC:D:IP:R:SU:")
except getopt.GetoptError as err:
    print(str(err))
    arg_error = True

print_usage = False
verbose = False
host_default = "localhost"
database_default = "starlinkstats"
icargs = {"host": host_default, "timeout": 5, "database": database_default}
rp = None
default_sleep_time = 30
sleep_time = default_sleep_time

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
            if opt == "-h":
                print_usage = True
            elif opt == "-n":
                icargs["host"] = arg
            elif opt == "-p":
                icargs["port"] = int(arg)
            elif opt == "-t":
                sleep_time = int(arg)
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
    print("    -h: Be helpful")
    print("    -n <name>: Hostname of InfluxDB server, default: " + host_default)
    print("    -p <num>: Port number to use on InfluxDB server")
    print("    -t <num>: Loop interval in seconds or 0 for no loop, default: " +
          str(default_sleep_time))
    print("    -v: Be verbose")
    print("    -C <filename>: Enable SSL/TLS using specified CA cert to verify server")
    print("    -D <name>: Database name to use, default: " + database_default)
    print("    -I: Enable SSL/TLS but disable certificate verification (INSECURE!)")
    print("    -P <word>: Set password for authentication")
    print("    -R <name>: Retention policy name to use")
    print("    -S: Enable SSL/TLS using default CA cert")
    print("    -U <name>: Set username for authentication")
    sys.exit(1 if arg_error else 0)

class DeviceStatusSeries(SeriesHelper):
    class Meta:
        series_name = "spacex.starlink.user_terminal.status"
        fields = [
            "hardware_version",
            "software_version",
            "state",
            "alert_motors_stuck",
            "alert_thermal_throttle",
            "alert_thermal_shutdown",
            "alert_unexpected_location",
            "snr",
            "seconds_to_first_nonempty_slot",
            "pop_ping_drop_rate",
            "downlink_throughput_bps",
            "uplink_throughput_bps",
            "pop_ping_latency_ms",
            "currently_obstructed",
            "fraction_obstructed"]
        tags = ["id"]
        retention_policy = rp

if "verify_ssl" in icargs and not icargs["verify_ssl"]:
    # user has explicitly said be insecure, so don't warn about it
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

influx_client = InfluxDBClient(**icargs)

try:
    dish_channel = None
    last_id = None
    last_failed = False

    pending = 0
    count = 0
    while True:
        try:
            if dish_channel is None:
                dish_channel = grpc.insecure_channel("192.168.100.1:9200")
            stub = spacex.api.device.device_pb2_grpc.DeviceStub(dish_channel)
            response = stub.Handle(spacex.api.device.device_pb2.Request(get_status={}))
            status = response.dish_get_status
            DeviceStatusSeries(
                id=status.device_info.id,
                hardware_version=status.device_info.hardware_version,
                software_version=status.device_info.software_version,
                state=spacex.api.device.dish_pb2.DishState.Name(status.state),
                alert_motors_stuck=status.alerts.motors_stuck,
                alert_thermal_throttle=status.alerts.thermal_throttle,
                alert_thermal_shutdown=status.alerts.thermal_shutdown,
                alert_unexpected_location=status.alerts.unexpected_location,
                snr=status.snr,
                seconds_to_first_nonempty_slot=status.seconds_to_first_nonempty_slot,
                pop_ping_drop_rate=status.pop_ping_drop_rate,
                downlink_throughput_bps=status.downlink_throughput_bps,
                uplink_throughput_bps=status.uplink_throughput_bps,
                pop_ping_latency_ms=status.pop_ping_latency_ms,
                currently_obstructed=status.obstruction_stats.currently_obstructed,
                fraction_obstructed=status.obstruction_stats.fraction_obstructed)
            last_id = status.device_info.id
            last_failed = False
        except grpc.RpcError:
            if dish_channel is not None:
                dish_channel.close()
                dish_channel = None
            if last_failed:
                if last_id is not None:
                    DeviceStatusSeries(id=last_id, state="DISH_UNREACHABLE")
            else:
                # Retry once, because the connection may have been lost while
                # we were sleeping
                last_failed = True
                continue
        pending = pending + 1
        if verbose:
            print("Samples: " + str(pending))
        count = count + 1
        if count > 5:
            try:
                DeviceStatusSeries.commit(influx_client)
                if verbose:
                    print("Wrote " + str(pending))
                pending = 0
            except Exception as e:
                print("Failed to write: " + str(e))
            count = 0
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            break
finally:
    # Flush on error/exit
    try:
        DeviceStatusSeries.commit(influx_client)
        if verbose:
            print("Wrote " + str(pending))
        rc = 0
    except Exception as e:
        print("Failed to write: " + str(e))
        rc = 1
    influx_client.close()
    if dish_channel is not None:
        dish_channel.close()
    sys.exit(rc)
