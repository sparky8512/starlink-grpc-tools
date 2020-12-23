#!/usr/bin/python3
######################################################################
#
# Simple example of how to poll the get_status request directly using
# grpc calls.
#
######################################################################
import grpc

import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc

with grpc.insecure_channel('192.168.100.1:9200') as channel:
    stub = spacex.api.device.device_pb2_grpc.DeviceStub(channel)
    response = stub.Handle(spacex.api.device.device_pb2.Request(get_status={}))

print(response)
