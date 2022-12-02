#!/usr/bin/python3
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

def wrap_ser(self, **kwargs):
    ret = device_pb2.Request.SerializeToString(self, **kwargs)
    print("request:", ret)
    return ret

def wrap_des(s):
    print("response:", s)
    #return "Hello"
    return device_pb2.Response.FromString(s)

# Note that if you remove the 'with' clause here, you need to separately
# call channel.close() when you're done with the gRPC connection.
with grpc.insecure_channel("192.168.100.1:9200") as channel:
    method = channel.unary_unary('/SpaceX.API.Device.Device/Handle',
            request_serializer=wrap_ser,
            response_deserializer=None
            #response_deserializer=wrap_des
            )
    response = method(device_pb2.Request(get_status={}), timeout=10)

# Dump everything
print(response)

# Just the software version
print("Software version:", response.dish_get_status.device_info.software_version)

# Check if connected
print("Not connected" if response.dish_get_status.HasField("outage") else "Connected")
