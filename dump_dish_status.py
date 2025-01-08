#!/usr/bin/env python3
"""Simple example of get_status request using grpc call directly."""

import sys

import grpc

try:
    from spacex.api.device import device_pb2
    from spacex.api.device import device_pb2_grpc
except ModuleNotFoundError:
    print("This script requires the generated gRPC protocol modules. See README file for details.",
          file=sys.stderr)
    sys.exit(1)

# Note that if you remove the 'with' clause here, you need to separately
# call channel.close() when you're done with the gRPC connection.
with grpc.insecure_channel("192.168.100.1:9200") as channel:
    stub = device_pb2_grpc.DeviceStub(channel)
    response = stub.Handle(device_pb2.Request(get_status={}), timeout=10)

# Dump everything
print(response)

# Just the software version
print("Software version:", response.dish_get_status.device_info.software_version)

# Check if connected
print("Not connected" if response.dish_get_status.HasField("outage") else "Connected")
