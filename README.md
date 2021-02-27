# starlink-grpc-tools
This repository has a handful of tools for interacting with the [gRPC](https://grpc.io/) service implemented on the Starlink user terminal (AKA "the dish").

For more information on what Starlink is, see [starlink.com](https://www.starlink.com/) and/or the [r/Starlink subreddit](https://www.reddit.com/r/Starlink/).

## Prerequisites

Most of the scripts here are [Python](https://www.python.org/) scripts. To use them, you will either need Python installed on your system or you can use the Docker image. If you use the Docker image, you can skip the rest of the prerequisites other than making sure the dish IP is reachable and Docker itself. For Linux systems, the python package from your distribution should be fine, as long as it is Python 3.

All the tools that pull data from the dish expect to be able to reach it at the dish's fixed IP address of 192.168.100.1, as do the Starlink [Android app](https://play.google.com/store/apps/details?id=com.starlink.mobile), [iOS app](https://apps.apple.com/us/app/starlink/id1537177988), and the browser app you can run directly from http://192.168.100.1. When using a router other than the one included with the Starlink installation kit, this usually requires some additional router configuration to make it work. That configuration is beyond the scope of this document, but if the Starlink app doesn't work on your home network, then neither will these scripts. That being said, you do not need the Starlink app installed to make use of these scripts. See [here](https://github.com/starlink-community/knowledge-base/wiki#using-your-own-router) for more detail on this.

Running the scripts within a [Docker](https://www.docker.com/) container requires Docker to be installed. Information about how to install that can be found at https://docs.docker.com/engine/install/

`dish_json_text.py` operates on a JSON format data representation of the protocol buffer messages, such as that output by [gRPCurl](https://github.com/fullstorydev/grpcurl). The command lines below assume `grpcurl` is installed in the runtime PATH. If that's not the case, just substitute in the full path to the command.

### Required Python modules

If you don't care about the details or minimizing your package requirements, you can skip the rest of this section and just do this to install latest versions of a superset of required modules:
```shell script
pip install --upgrade -r requirements.txt
```

The scripts that don't use `grpcurl` to pull data require the `grpcio` Python package at runtime and the optional step of generating the gRPC protocol module code requires the `grpcio-tools` package. Information about how to install both can be found at https://grpc.io/docs/languages/python/quickstart/. If you skip generation of the gRPC protocol modules, the scripts will instead require the `yagrc` Python package. Information about how to install that is at https://github.com/sparky8512/yagrc.

The scripts that use [MQTT](https://mqtt.org/) for output require the `paho-mqtt` Python package. Information about how to install that can be found at https://www.eclipse.org/paho/index.php?page=clients/python/index.php

The scripts that use [InfluxDB](https://www.influxdata.com/products/influxdb/) for output require the `influxdb` Python package. Information about how to install that can be found at https://github.com/influxdata/influxdb-python. Note that this is the (slightly) older version of the InfluxDB client Python module, not the InfluxDB 2.0 client. It can still be made to work with an InfluxDB 2.0 server, but doing so requires using `influx v1` [CLI commands](https://docs.influxdata.com/influxdb/v2.0/reference/cli/influx/v1/) on the server to map the 1.x username, password, and database names to their 2.0 equivalents.

Note that the Python package versions available from various Linux distributions (ie: installed via `apt-get` or similar) tend to run a bit behind those available to install via `pip`. While the distro packages should work OK as long as they aren't extremely old, they may not work as well as the later versions.

### Generating the gRPC protocol modules

This step is no longer required, as the grpc scripts can now get the protocol module classes at run time via reflection, but generating the protocol modules will improve script startup time, and it would be a good idea to at least stash away the protoset file emitted by `grpcurl` in case SpaceX ever turns off server reflection in the dish software.

The grpc scripts require some generated code to support the specific gRPC protocol messages used. These would normally be generated from .proto files that specify those messages, but to date (2020-Dec), SpaceX has not publicly released such files. The gRPC service running on the dish appears to have [server reflection](https://github.com/grpc/grpc/blob/master/doc/server-reflection.md) enabled, though. `grpcurl` can use that to extract a protoset file, and the `protoc` compiler can use that to make the necessary generated code:
```shell script
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
python3 -m grpc_tools.protoc --descriptor_set_in=../dish.protoset --python_out=. --grpc_python_out=. spacex/api/device/transceiver.proto
```
Then move the resulting files to where the Python scripts can find them in the import path, such as in the same directory as the scripts themselves.

## Usage

Of the 3 groups below, the grpc scripts are really the only ones being actively developed. The others are mostly by way of example of what could be done with the underlying data.

### The grpc scripts

This set of scripts includes `dish_grpc_text.py`, `dish_grpc_influx.py`, `dish_grpc_sqlite.py`, and `dish_grpc_mqtt.py`. They mostly support the same functionality, but write their output in different ways. `dish_grpc_text.py` writes data to standard output, `dish_grpc_influx.py` sends it to an InfluxDB server, `dish_grpc_sqlite.py` writes it a sqlite database, and `dish_grpc_mqtt.py` sends it to a MQTT broker.

All 4 scripts support processing status data and/or history data in various modes. The status data is mostly what appears related to the dish in the Debug Data section of the Starlink app, whereas most of the data displayed in the Statistics page of the Starlink app comes from the history data. Specific status or history data groups can be selected by including their mode names on the command line. Run the scripts with `-h` command line option to get a list of available modes. See the documentation at the top of `starlink_grpc.py` for detail on what each of the fields means within each mode group.

For example, all the currently available status groups can be output by doing:
```shell script
python3 dish_grpc_text.py status obstruction_detail alert_detail
```

By default, `dish_grpc_text.py` (and `dish_json_text.py`, described below) will output in CSV format. You can use the `-v` option to instead output in a (slightly) more human-readable format.

By default, all of these scripts will pull data once, send it off to the specified data backend, and then exit. They can instead be made to run in a periodic loop by passing a `-t` option to specify loop interval, in seconds. For example, to capture status information to a InfluxDB server every 30 seconds, you could do something like this:
```shell script
python3 dish_grpc_influx.py -t 30 [... probably other args to specify server options ...] status
```

Some of the scripts (currently only the InfluxDB one) also support specifying options through environment variables. See details in the scripts for the environment variables that map to options.

#### Bulk history data collection

`dish_grpc_influx.py`, `dish_grpc_sqlite.py`, and `dish_grpc_text.py` also support a bulk history mode that collects and writes the full second-by-second data instead of summary stats. To select bulk mode, use `bulk_history` for the mode argument. You'll probably also want to use the `-t` option to have it run in a loop.

### The JSON parser script

`dish_json_text.py` is similar to `dish_grpc_text.py`, but it takes JSON format input from a file instead of pulling it directly from the dish via grpc call. It also does not support the status info modes, because those are easy enough to interpret directly from the JSON data. The easiest way to use it is to pipe the `grpcurl` command directly into it. For example:
```shell script
grpcurl -plaintext -d {\"get_history\":{}} 192.168.100.1:9200 SpaceX.API.Device.Device/Handle | python3 dish_json_text.py ping_drop
```
For more usage options, run:
```shell script
python3 dish_json_text.py -h
```

The one bit of functionality this script has over the grpc scripts is that it supports capturing the grpcurl output to a file and reading from that, which may be useful if you're collecting data in one place but analyzing it in another. Otherwise, it's probably better to use `dish_grpc_text.py`, described above.

### Other scripts

`dump_dish_status.py` is a simple example of how to use the grpc modules (the ones generated by protoc, not `starlink_grpc`) directly. Just run it as:
```shell script
python3 dump_dish_status.py
```
and revel in copious amounts of dish status information. OK, maybe it's not as impressive as all that. This one is really just meant to be a starting point for real functionality to be added to it.

`poll_history.py` is another silly example, but this one illustrates how to periodically poll the status and/or bulk history data using the `starlink_grpc` module's API. It's not really useful by itself, but if you really want to, you can run it as:
```shell script
python3 poll_history.py
```
Possibly more simple examples to come, as the other scripts have started getting a bit complicated.

## Docker for InfluxDB ( & MQTT under development )

Initialization of the container can be performed with the following command:

```shell script
docker run -d -t --name='starlink-grpc-tools' -e INFLUXDB_HOST={InfluxDB Hostname} \
    -e INFLUXDB_PORT={Port, 8086 usually} \
    -e INFLUXDB_USER={Optional, InfluxDB Username} \
    -e INFLUXDB_PWD={Optional, InfluxDB Password} \
    -e INFLUXDB_DB={Pre-created DB name, starlinkstats works well} \
    neurocis/starlink-grpc-tools dish_grpc_influx.py -v status alert_detail
```

The `-t` option to `docker run` will prevent Python from buffering the script's standard output and can be omitted if you don't care about seeing the verbose output in the container logs as soon as it is printed.

The `dish_grpc_influx.py -v status alert_detail` is optional and omitting it will run same but not verbose, or you can replace it with one of the other scripts if you wish to run that instead, or use other command line options. There is also a `GrafanaDashboard - Starlink Statistics.json` which can be imported to get some charts like:

![image](https://user-images.githubusercontent.com/945191/104257179-ae570000-5431-11eb-986e-3fedd04bfcfb.png)

You'll probably want to run with the `-t` option to `dish_grpc_influx.py` to collect status information periodically for this to be meaningful.

## To Be Done (Maybe)

There are `reboot` and `dish_stow` requests in the Device protocol, too, so it should be trivial to write a command that initiates dish reboot and stow operations. These are easy enough to do with `grpcurl`, though, as there is no need to parse through the response data. For that matter, they're easy enough to do with the Starlink app.

No further data collection functionality is planned at this time. If there's something you'd like to see added, please feel free to open a feature request issue. Bear in mind, though, that functionality will be limited to that which the Starlink gRPC services support. In general, those services are limited to what is required by the Starlink app, so unless the app has some related feature, it is unlikely the gRPC services will be sufficient to implement it in these tools.

## Other Tidbits

The Starlink Android app actually uses port 9201 instead of 9200. Both appear to expose the same gRPC service, but the one on port 9201 uses [gRPC-Web](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md), which can use HTTP/1.1, whereas the one on port 9200 uses HTTP/2, which is what most gRPC tools expect.

The Starlink router also exposes a gRPC service, on ports 9000 (HTTP/2.0) and 9001 (HTTP/1.1).

The file `get_history_notes.txt` has my original ramblings on how to interpret the history buffer data (with the JSON format naming). It may be of interest if you want to pull the `get_history` grpc data directly and don't want to dig through the convoluted logic in the `starlink_grpc` module.

## Related Projects

[ChuckTSI's Better Than Nothing Web Interface](https://github.com/ChuckTSI/BetterThanNothingWebInterface) uses grpcurl and PHP to provide a spiffy web UI for some of the same data this project works on.

[starlink-cli](https://github.com/starlink-community/starlink-cli) is another command line tool for interacting with the Starlink gRPC services, including the one on the Starlink router, in case Go is more your thing.
