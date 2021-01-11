#!/bin/sh

printenv >> /etc/environment
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
grpcurl -plaintext -protoset-out dish.protoset 192.168.100.1:9200 describe SpaceX.API.Device.Device
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/device.proto
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/common/status/status.proto
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/command.proto
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/common.proto
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/dish.proto
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/wifi.proto
python3 -m grpc_tools.protoc --descriptor_set_in=dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/wifi_config.proto
/usr/local/bin/python3 $1
