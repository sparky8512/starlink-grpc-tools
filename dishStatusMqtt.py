#!/usr/bin/python3
import paho.mqtt.publish

import grpc

import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc

with grpc.insecure_channel('192.168.100.1:9200') as channel:
    stub = spacex.api.device.device_pb2_grpc.DeviceStub(channel)
    response = stub.Handle(spacex.api.device.device_pb2.Request(get_status={}))

status = response.dish_get_status

# More alerts may be added in future, so rather than list them individually,
# build a bit field based on field numbers of the DishAlerts message.
alert_bits = 0
for alert in status.alerts.ListFields():
    alert_bits |= (1 if alert[1] else 0) << (alert[0].number - 1)

topicPrefix = "starlink/dish_status/" + status.device_info.id + "/"
msgs = [(topicPrefix + "hardware_version", status.device_info.hardware_version, 0, False),
        (topicPrefix + "software_version", status.device_info.software_version, 0, False),
        (topicPrefix + "state", spacex.api.device.dish_pb2.DishState.Name(status.state), 0, False),
        (topicPrefix + "uptime", status.device_state.uptime_s, 0, False),
        (topicPrefix + "snr", status.snr, 0, False),
        (topicPrefix + "seconds_to_first_nonempty_slot", status.seconds_to_first_nonempty_slot, 0, False),
        (topicPrefix + "pop_ping_drop_rate", status.pop_ping_drop_rate, 0, False),
        (topicPrefix + "downlink_throughput_bps", status.downlink_throughput_bps, 0, False),
        (topicPrefix + "uplink_throughput_bps", status.uplink_throughput_bps, 0, False),
        (topicPrefix + "pop_ping_latency_ms", status.pop_ping_latency_ms, 0, False),
        (topicPrefix + "alerts", alert_bits, 0, False),
        (topicPrefix + "fraction_obstructed", status.obstruction_stats.fraction_obstructed, 0, False),
        (topicPrefix + "currently_obstructed", status.obstruction_stats.currently_obstructed, 0, False),
        # While the field name for this one implies it covers 24 hours, the
        # empirical evidence suggests it only covers 12 hours. It also resets
        # on dish reboot, so may not cover that whole period. Rather than try
        # to convey that complexity in the topic label, just be a bit vague:
        (topicPrefix + "seconds_obstructed", status.obstruction_stats.last_24h_obstructed_s, 0, False),
        (topicPrefix + "wedges_fraction_obstructed", ",".join(str(x) for x in status.obstruction_stats.wedge_abs_fraction_obstructed), 0, False)]

paho.mqtt.publish.multiple(msgs, hostname="localhost", client_id=status.device_info.id)
