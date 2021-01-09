#!/usr/bin/python3
######################################################################
#
# Write get_status info to an InfluxDB database.
#
# This script will poll current status and write it to
# the specified InfluxDB database.
#
######################################################################
import os
import grpc
import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc

from influxdb import InfluxDBClient
from influxdb import SeriesHelper

influxdb_host = os.environ.get("INFLUXDB_HOST")
influxdb_port = os.environ.get("INFLUXDB_PORT")
influxdb_user = os.environ.get("INFLUXDB_USER")
influxdb_pwd = os.environ.get("INFLUXDB_PWD")
influxdb_db = os.environ.get("INFLUXDB_DB")

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

influx_client = InfluxDBClient(host=influxdb_host, port=influxdb_port, username=influxdb_user, password=influxdb_pwd, database=influxdb_db, ssl=False, retries=1, timeout=15)

dish_channel = None
last_id = None
last_failed = False

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
    try:
        DeviceStatusSeries.commit(influx_client)
    except Exception as e:
        print("Failed to write: " + str(e))
    break
