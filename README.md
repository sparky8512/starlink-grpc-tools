# starlink-grpc-tools
This repository has a handful of tools for interacting with the [gRPC](https://grpc.io/) service implemented on the Starlink user terminal (AKA "the dish").

For more information on what Starlink is, see [starlink.com](https://www.starlink.com/) and/or the [r/Starlink subreddit](https://www.reddit.com/r/Starlink/).

## Prerequisites / Installation

Most of the scripts here are [Python](https://www.python.org/) scripts. To use them, you will either need Python installed on your system or you can use the Docker image. If you use the Docker image, you can skip the rest of the prerequisites other than making sure the dish IP is reachable and Docker itself. For Linux systems, the python package from your distribution should be fine, as long as it is Python 3, version 3.7 or later.

All the tools that pull data from the dish expect to be able to reach it at the dish's fixed IP address of 192.168.100.1, as do the Starlink [Android app](https://play.google.com/store/apps/details?id=com.starlink.mobile), [iOS app](https://apps.apple.com/us/app/starlink/id1537177988), and the browser app you can run directly from http://192.168.100.1. When using a router other than the one included with the Starlink installation kit, this usually requires some additional router configuration to make it work. That configuration is beyond the scope of this document, but if the Starlink app doesn't work on your home network, then neither will these scripts. That being said, you do not need the Starlink app installed to make use of these scripts. See [here](https://github.com/starlink-community/knowledge-base/wiki#using-your-own-router) for more detail on this.

Running the scripts within a [Docker](https://www.docker.com/) container requires Docker to be installed. Information about how to install that can be found at https://docs.docker.com/engine/install/. See below for how to pull the starlink-grpc-tools container image.

### Required Python modules (for non-Docker usage)

The scripts require a number of Python modules to be present in your local Python environment. It is recommended to [create and use a virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#create-and-use-virtual-environments) (venv) for this purpose, but usually not required. However, some OS distribution's Python installations may require the use of venv when running at `root`/`Administrator` user. If you don't want to deal with that, either install as a different user, or add the `--user` option after the word `install` in the following command, but be aware that will only make it available to that user.

The easiest way to get the required modules is to run the following command, which will install latest versions of a superset of the required modules:
```shell script
pip install --upgrade -r requirements.txt
```

If you really care about the details here or wish to minimize your package requirements, you can find more detail about which specific modules are required for what usage in [this Wiki article](https://github.com/sparky8512/starlink-grpc-tools/wiki/Python-Module-Dependencies).

### Generating the gRPC protocol modules (for non-Docker usage)

This step is no longer required, nor is it particularly recommended, so the details have been moved to [this Wiki article](https://github.com/sparky8512/starlink-grpc-tools/wiki/gRPC-Protocol-Modules).

### Enabling access to location data

This step is only required if you want to use the `location` data group with the grpc scripts. Note that it will allow any device on your local (home) network to access the physical location (GPS) data for your dish. If you are not comfortable with that, then do not enable it.

Access to location data must be enabled per dish and currently (2022-Sep), this can only be done using the Starlink mobile app, version 2022.09.0 or later. It cannot be done using the browser app. To enable access, you must be logged in to your Starlink account. You can log in by pressing the user icon in the upper left corner of the main screen of the app. Once logged in, from the main screen, select SETTINGS, then select ADVANCED, then select DEBUG DATA. Scroll down and you should see a toggle switch for "allow access on local network" in a section labelled STARLINK LOCATION, which should be off by default. Turn that switch on to enable access or off to disable it. This may move in the future, and there is no guarantee the ability to enable this feature will remain in the app.

Note that the Starlink mobile app can be pretty finicky and painfully slow. It's best to wait for the screens to load completely before going on to the next one.

## Usage

Of the 3 groups below, the grpc scripts are really the only ones being actively developed. The others are mostly by way of example of what could be done with the underlying data.

### The grpc scripts

This set of scripts includes `dish_grpc_text.py`, `dish_grpc_influx.py`, `dish_grpc_influx2.py`, `dish_grpc_sqlite.py`, `dish_grpc_mqtt.py`, and `dish_grpc_prometheus.py`. They mostly support the same functionality, but write their output in different ways. `dish_grpc_text.py` writes data to standard output, `dish_grpc_influx.py` and `dish_grpc_influx2.py` send it to an InfluxDB 1.x and 2.x server, respectively, `dish_grpc_sqlite.py` writes it to a sqlite database, and `dish_grpc_mqtt.py` sends it to a MQTT broker. `dish_grpc_prometheus.py` does not write anywhere but will listen for HTTP requests and
return data in a format Prometheus can scrape.

All these scripts support processing status data and/or history data in various modes. The status data is mostly what appears related to the dish in the Debug Data section of the Starlink app, whereas most of the data displayed in the Statistics page of the Starlink app comes from the history data. Specific status or history data groups can be selected by including their mode names on the command line. Run the scripts with `-h` command line option to get a list of available modes. See the documentation at the top of `starlink_grpc.py` for detail on what each of the fields means within each mode group.

For example, data from all the currently available status groups can be output by doing:
```shell script
python3 dish_grpc_text.py status obstruction_detail alert_detail
```

By default, `dish_grpc_text.py` will output in CSV format. You can use the `-v` option to instead output in a (slightly) more human-readable format.

By default, most of these scripts will pull data once, send it off to the specified data backend, and then exit. They can instead be made to run in a periodic loop by passing a `-t` option to specify loop interval, in seconds. For example, to capture status information to a InfluxDB server every 30 seconds, you could do something like this:
```shell script
python3 dish_grpc_influx.py -t 30 [... probably other args to specify server options ...] status
```

The exception to this is `dish_grpc_prometheus.py`, for which the timing interval is determined by whatever is polling the HTTP page it exports.

Some of the scripts (currently only the InfluxDB and MQTT ones) also support specifying options through environment variables. See details in the scripts for the environment variables that map to options.

#### Bulk history data collection

`dish_grpc_influx.py`, `dish_grpc_influx2.py`, `dish_grpc_sqlite.py`, and `dish_grpc_text.py` also support a bulk history mode that collects and writes the full second-by-second data instead of summary stats. To select bulk mode, use `bulk_history` for the mode argument. You'll probably also want to use the `-t` option to have it run in a loop.

#### Polling interval

A recent (as of 2021-Aug) change in the dish firmware appears to have reduced the amount of history data returned from the most recent 12 hours to the most recent 15 minutes, so if you are using the `-t` option to poll either bulk history or history-based statistics, you should choose an interval less than 900 seconds; otherwise, you will not capture all the data.

Computing history statistics (one or more of groups `ping_drop`, `ping_run_length`, `ping_latency`, `ping_loaded_latency`, and `usage`) across periods longer than the 15 minute history buffer may be done by combining the `-t` and `-o` options. The history data will be polled at the interval specified by the `-t` option, but it will be aggregated the number of times specified by the `-o` option and statistics will be computed against the aggregated data which will be a period of the `-t` option value times the `-o` option value. For example, the following:
```shell script
python3 dish_grpc_text.py -t 60 -o 60 ping_drop 
```
will poll history data once per minute, but compute statistics only once per hour. This also reduces data loss due to a dish reboot, since the `-o` option will aggregate across reboots, too.

#### The obstruction map script

`dish_obstruction_map.py` is a little different in that it doesn't write to a database, but rather writes PNG images to the local filesystem. To get a single image of the current obstruction map using the default colors, you can do the following:
```shell script
python3 dish_obstruction_map.py obstructions.png
```
or to run in a loop writing a sequence of images once per hour, you can do the following:
```shell script
python3 dish_obstruction_map.py -t 3600 obstructions_%s.png
```

Run it with the `-h` command line option for full usage details, including control of the map colors and color modes.

#### Reboot, stow, sleep, and GPS control

`dish_control.py` is a simple stand alone script that can issue reboot, stow, or unstow commands to the dish:
```shell script
python3 dish_control.py reboot
python3 dish_control.py stow
python3 dish_control.py unstow
```
These operations can also be done using `grpcurl`, thus avoiding the need to use Python or install the required Python module dependencies. See [here](https://github.com/sparky8512/starlink-grpc-tools/wiki/Useful-grpcurl-commands) for specific `grpcurl` commands for these operations.

`dish_control.py` can also show, set, or disable the sleep mode schedule. You can get usage instructions for that by doing:
```shell script
python3 dish_control.py set_sleep -h
```

It can also tell the dish whether or not to use GPS for position data. You can get usage instructions for that by doing:
```shell script
python3 dish_control.py set_gps -h
```
**NOTE**: This has no impact on whether or not the location data can be polled (see [above section](#enabling-access-to-location-data)). It only instructs the dish whether or not to use GPS for its own purposes. If this is not meaningful to you, then you should probably not mess with this setting. Note also that this setting is not preserved across dish reboot, at which point it will reset to the default of GPS enabled.

Finally, all the commands supported by this script can be run periodically, either by using the `-t` option most of the other scripts support, or the `-c` option to use the cron-like scheduler described in the [next section](#firmware-update-checking-and-triggering).

#### Firmware update checking and triggering

`dish_check_update.py` checks for pending dish software updates, and can optionally trigger the update by rebooting the dish if it detects one. This can be useful if your dish normally does its automatic software install reboots at a time you don't want. To simplify this use case, this script supports a cron-like scheduling option, in addition to the `-t` periodic interval loop scheduling that most of the other scripts support.

To use the cron-like scheduler, add the `-c` command line option to specify the schedule, using the same string format cron uses for its crontab entries (`minute` `hour` `day_of_month` `month` `day_of_week`). By default, it will use system local timezone, including DST adjustment. To use a different timezone, use the `-m` option. For example, to check and trigger updates at 2:30am local time daily:
```shell script
python3 dish_check_update.py -c "30 2 * * *" --install
```
or same for specific timezone:
```shell script
python3 dish_check_update.py -c "30 2 * * *" -m "America/Los_Angeles" --install
```
or to immediately check without triggering install, you can do:
```shell script
python3 dish_check_update.py -v
```

Run with the `-h` command line option for full usage details. For more information on the cron schedule string format or timezone names, see the [croniter](https://github.com/kiorky/croniter) or [dateutil](https://github.com/dateutil/dateutil) project documentation, respectively.

### The JSON parser script

`dish_json_text.py` operates on a JSON format data representation of the protocol buffer messages, such as that output by [gRPCurl](https://github.com/fullstorydev/grpcurl). The command lines below assume `grpcurl` is installed in the runtime PATH. If that's not the case, just substitute in the full path to the command.

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

`dump_dish_status.py` is a simple example of how to use the grpc modules (the ones generated by protoc, not `starlink_grpc`) directly. This script does require the [generated gRPC protocol modules](https://github.com/sparky8512/starlink-grpc-tools/wiki/gRPC-Protocol-Modules), contrary to the above recommendation against generating them. Once those are in place, just run as:
```shell script
python3 dump_dish_status.py
```
and revel in copious amounts of dish status information. OK, maybe it's not as impressive as all that. This one is really just meant to be a starting point for real functionality to be added to it. For a (relatively) simple example of using reflection to avoid the requirement to generate protocol modules, see `dish_control.py`.

`poll_history.py` is another silly example, but this one illustrates how to periodically poll the status and/or bulk history data using the `starlink_grpc` module's API. It's not really useful by itself, but if you really want to, you can run it as:
```shell script
python3 poll_history.py
```
Possibly more simple examples to come, as the other scripts have started getting a bit complicated.

`extract_protoset.py` can be used in place of `grpcurl` for recording the dish protocol information. See [the related Wiki article](https://github.com/sparky8512/starlink-grpc-tools/wiki/gRPC-Protocol-Modules) for more details.

## Running with Docker

The supported docker image for this project is the one hosted in the [GitHub Packages repository](https://github.com/sparky8512/starlink-grpc-tools/pkgs/container/starlink-grpc-tools). This is a multi-arch image built for `linux/amd64` (x64_64) and `linux/arm64` (aarch64) docker platforms.

You can get the "latest" image with the following command:
```shell script
docker pull ghcr.io/sparky8512/starlink-grpc-tools
```
This will pull the image tagged as "latest", which will be the latest image generated that has at least been sanity-tested to not be completely broken. There should also be images for all recent tagged releases of this project. See the package repository for a full list of tagged images.

You can run it with the following:
```shell script
docker run --name=starlink-grpc-tools ghcr.io/sparky8512/starlink-grpc-tools <script_name>.py <script args...>
```
For example, the following will print current status info and then exit:
```shell script
docker run --name=starlink-grpc-tools ghcr.io/sparky8512/starlink-grpc-tools dish_grpc_text.py -v status alert_detail
```
Of course, you can change the name to whatever you want instead, and use other docker run options, as appropriate.

The default command is `dish_grpc_influx.py status alert_detail`, which is only useful if you have an InfluxDB server running somewhere and pass in environment variables with the appropriate user and database info, such as:
```shell script
docker run --name=starlink-grpc-tools -e INFLUXDB_HOST={InfluxDB Hostname} \
    -e INFLUXDB_PORT={Port, 8086 usually} \
    -e INFLUXDB_USER={Optional, InfluxDB Username} \
    -e INFLUXDB_PWD={Optional, InfluxDB Password} \
    -e INFLUXDB_DB={Pre-created DB name, starlinkstats works well} \
    ghcr.io/sparky8512/starlink-grpc-tools
```

When running in the background, you will probably want to specify a `-t` script option, to run in a loop, otherwise it will exit right away and leave an inactive container. For example:
```shell script
docker run -d -t --name=starlink-grpc-tools -e INFLUXDB_HOST={InfluxDB Hostname} \
    -e INFLUXDB_PORT={Port, 8086 usually} \
    -e INFLUXDB_USER={Optional, InfluxDB Username} \
    -e INFLUXDB_PWD={Optional, InfluxDB Password} \
    -e INFLUXDB_DB={Pre-created DB name, starlinkstats works well} \
    ghcr.io/sparky8512/starlink-grpc-tools -v -t 60 status alert_detail
```
The `-t` option to `docker run` will prevent Python from buffering the script's standard output and can be omitted if you don't care about seeing the verbose output in the container logs as soon as it is printed.

## Running with SystemD

To run e.g. the `dish_grpc_influx2` script via SystemD the following steps are an option.
Commands here should work for debian / ubuntu based distribution

```shell
sudo apt instlall python3-venv
cd /opt/
sudo mkdir starlink-grpc-tools
sudo chown <your non-root user>
git clone <git url>
cd starlink-grpc-tools
python3 -m venv venv
source venv/bin/activate.sh
pip3 install -r requirements.txt
sudo cp systemd/starlink-influx2.service /etc/systemd/system/starlink-influx2.service
sudo <your favorite editor> /etc/systemd/system/starlink-influx2.service
# Set influx url, token, bucket and org
sudo systemctl enable starlink-influx2
sudo systemctl start starlink-influx2
```

## Dashboards

Several users have built dashboards for displaying data collected by the scripts in this project. Information on those can be found in [this Wiki article](https://github.com/sparky8512/starlink-grpc-tools/wiki/Dashboards). If you have one you would like to add, please feel free to edit the Wiki page to do so.

Note that feeding an InfluxDB dashboard will likely need the `-t` script option to `dish_grpc_influx.py` in order to collect status and/or history information periodically.

## To Be Done (Maybe, but Probably Not)

The [Wiki for this GitHub project](https://github.com/sparky8512/starlink-grpc-tools/wiki) has a little more information, and was originally planned to have more detail on some aspects of the history data, but that's mostly been obsoleted by changes to the gRPC service. It still may be updated some day with more use case examples or other information. In the mean time, it is configured as editable by anyone with a GitHub login, so if you have relevant content you believe to be useful, feel free to add it.

No further data collection functionality is planned at this time. If there's something you'd like to see added, please feel free to open a feature request issue. Bear in mind, though, that functionality will be limited to that which the Starlink gRPC services support. In general, those services are limited to what is required by the Starlink app, so unless the app has some related feature, it is unlikely the gRPC services will be sufficient to implement it in these tools.

## Related Projects

[ChuckTSI's Better Than Nothing Web Interface](https://github.com/ChuckTSI/BetterThanNothingWebInterface) uses grpcurl and PHP to provide a spiffy web UI for some of the same data this project works on.

[starlink-cli](https://github.com/starlink-community/starlink-cli) is another command line tool for interacting with the Starlink gRPC services, including the one on the Starlink router, in case Go is more your thing.
