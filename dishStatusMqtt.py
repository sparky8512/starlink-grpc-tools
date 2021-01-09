#!/usr/bin/python3
######################################################################
#
# Publish Starlink user terminal status info to a MQTT broker.
#
# This script pulls the current status once and publishes it to the
# specified MQTT broker.
#
######################################################################
import paho.mqtt.publish

import grpc

import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc

with grpc.insecure_channel("192.168.100.1:9200") as channel:
    stub = spacex.api.device.device_pb2_grpc.DeviceStub(channel)
    response = stub.Handle(spacex.api.device.device_pb2.Request(get_status={}))

status = response.dish_get_status

# More alerts may be added in future, so rather than list them individually,
# build a bit field based on field numbers of the DishAlerts message.
alert_bits = 0
for alert in status.alerts.ListFields():
    alert_bits |= (1 if alert[1] else 0) << (alert[0].number - 1)

topic_prefix = "starlink/dish_status/" + status.device_info.id + "/"
msgs = [(topic_prefix + "hardware_version", status.device_info.hardware_version, 0, False),
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
        (topic_prefix + "wedges_fraction_obstructed", ",".join(str(x) for x in status.obstruction_stats.wedge_abs_fraction_obstructed), 0, False)]

paho.mqtt.publish.multiple(msgs, hostname="localhost", client_id=status.device_info.id)
