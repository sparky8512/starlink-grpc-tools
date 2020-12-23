# starlink-grpc-tools
This repository has a handful of tools for interacting with the [gRPC](https://grpc.io/) service implemented on the Starlink user terminal (AKA "the dish").

For more information on what Starlink is, see [starlink.com](https://www.starlink.com/) and/or the [r/Starlink subreddit](https://www.reddit.com/r/Starlink/).

## Prerequisites

`parseJsonHistory.py` operates on a JSON format data representation of the protocol buffer messages, such as that output by [gRPCurl](https://github.com/fullstorydev/grpcurl). The command lines below assume `grpcurl` is installed in the runtime PATH. If that's not the case, just substitute in the full path to the command.

All the tools that pull data from the dish expect to be able to reach it at the dish's fixed IP address of 192.168.100.1, as do the Starlink [Android app](https://play.google.com/store/apps/details?id=com.starlink.mobile) and [iOS app](https://apps.apple.com/us/app/starlink/id1537177988). When using a router other than the one included with the Starlink installation kit, this usually requires some additional router configuration to make it work. That configuration is beyond the scope of this document, but if the Starlink app doesn't work on your home network, then neither will these scripts. That being said, you do not need the Starlink app installed to make use of these scripts.

The scripts that don't use `grpcurl` to pull data require the `grpcio` Python package at runtime and generating the necessary gRPC protocol code requires the `grpcio-tools` package. Information about how to install both can be found at https://grpc.io/docs/languages/python/quickstart/

## Usage

For `parseJsonHistory.py`, the easiest way to use it is to pipe the `grpcurl` command directly into it. For example:
```
grpcurl -plaintext -d {\"get_history\":{}} 192.168.100.1:9200 SpaceX.API.Device.Device/Handle | python parseJsonHistory.py
```
For more usage options, run:
```
python parseJsonHistory.py -h
```

When used as-is, `parseJsonHistory.py` will summarize packet loss information from the data the dish records. There's other bits of data in there, though, so that script could be used as a starting point or example of how to iterate through it. Most of the data displayed in the Statistics page of the Starlink app appears to come from this same `get_history` gRPC response. See the file `get_history_notes.txt` for some ramblings on how to interpret it.

The other scripts can do the gRPC communication directly, but they require some generated code to support the specific gRPC protocol messages used. These would normally be generated from .proto files that specify those messages, but to date (2020-Dec), SpaceX has not publicly released such files. The gRPC service running on the dish appears to have [server reflection](https://github.com/grpc/grpc/blob/master/doc/server-reflection.md) enabled, though. `grpcurl` can use that to extract a protoset file, and the `protoc` compiler can use that to make the necessary generated code:
```
grpcurl -plaintext -protoset-out dish.protoset 192.168.100.1:9200 describe SpaceX.API.Device.Device
mkdir src
cd src
python3 -m grpc_tools.protoc --descriptor_set_in=../dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/device.proto
python3 -m grpc_tools.protoc --descriptor_set_in=../dish.protoset --python_out=. --grpc_python_out=. spacex/api/common/status/status.proto
python3 -m grpc_tools.protoc --descriptor_set_in=../dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/command.proto
python3 -m grpc_tools.protoc --descriptor_set_in=../dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/common.proto
python3 -m grpc_tools.protoc --descriptor_set_in=../dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/dish.proto
python3 -m grpc_tools.protoc --descriptor_set_in=../dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/wifi.proto
python3 -m grpc_tools.protoc --descriptor_set_in=../dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/wifi_config.proto
```
Then move the resulting files to where the Python scripts can find them, such as in the same directory as the scripts themselves.

Once those are available, the `dishHistoryStats.py` script can be used in place of the `grpcurl | parseJsonHistory.py` pipeline, with most of the same command line options.

To collect and record summary stats every hour, you can put something like the following in your user crontab:
```
00 * * * * [ -e ~/dishStats.csv ] || ~/bin/dishHistoryStats.py -H >~/dishStats.csv; ~/bin/dishHistoryStats.py >>~/dishStats.csv
```

`dishDumpStatus.py` is even simpler. Just run it as:
```
python3 dishDumpStatus.py
```
and revel in copious amounts of dish status information. OK, maybe it's not as impressive as all that. This one is really just meant to be a starting point for real functionality to be added to it. The information this script pulls is mostly what appears related to the dish in the Debug Data section of the Starlink app.

## To Be Done (Maybe)

There are `reboot` and `dish_stow` requests in the Device protocol, too, so it should be trivial to write a command that initiates dish reboot and stow operations. These are easy enough to do with `grpcurl`, though, as there is no need to parse through the response data. For that matter, they're easy enough to do with the Starlink app.

## Other Tidbits

The Starlink Android app actually uses port 9201 instead of 9200. Both appear to expose the same gRPC service, but the one on port 9201 uses an HTTP/1.1 wrapper, whereas the one on port 9200 uses HTTP/2.0, which is what most gRPC tools expect.

The Starlink router also exposes a gRPC service, on ports 9000 (HTTP/2.0) and 9001 (HTTP/1.1).
