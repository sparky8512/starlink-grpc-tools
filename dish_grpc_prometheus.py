#!/usr/bin/env python3
"""Prometheus exporter for Starlink user terminal data info.

This script pulls the current status info and/or metrics computed from the
history data and makes it available via HTTP in the format Prometheus expects.
"""

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import logging
import signal
import sys
import threading

import dish_common


class Terminated(Exception):
    pass


def handle_sigterm(signum, frame):
    # Turn SIGTERM into an exception so main loop can clean up
    raise Terminated


class MetricInfo:
    unit = ""
    kind = "gauge"
    help = ""

    def __init__(self, unit=None, kind=None, help=None) -> None:
        if unit:
            self.unit = f"_{unit}"
        if kind:
            self.kind = kind
        if help:
            self.help = help
        pass


METRICS_INFO = {
    "status_uptime": MetricInfo(unit="seconds", kind="counter"),
    "status_longitude": MetricInfo(),
    "status_latitude": MetricInfo(),
    "status_altitude": MetricInfo(),
    "status_gps_enabled": MetricInfo(),
    "status_gps_ready": MetricInfo(),
    "status_gps_sats": MetricInfo(),
    "status_seconds_to_first_nonempty_slot": MetricInfo(),
    "status_pop_ping_drop_rate": MetricInfo(),
    "status_downlink_throughput_bps": MetricInfo(),
    "status_uplink_throughput_bps": MetricInfo(),
    "status_pop_ping_latency_ms": MetricInfo(),
    "status_alerts": MetricInfo(),
    "status_fraction_obstructed": MetricInfo(),
    "status_currently_obstructed": MetricInfo(),
    "status_seconds_obstructed": MetricInfo(),
    "status_obstruction_duration": MetricInfo(),
    "status_obstruction_interval": MetricInfo(),
    "status_direction_azimuth": MetricInfo(),
    "status_direction_elevation": MetricInfo(),
    "status_is_snr_above_noise_floor": MetricInfo(),
    "status_alert_motors_stuck": MetricInfo(),
    "status_alert_thermal_throttle": MetricInfo(),
    "status_alert_thermal_shutdown": MetricInfo(),
    "status_alert_mast_not_near_vertical": MetricInfo(),
    "status_alert_unexpected_location": MetricInfo(),
    "status_alert_slow_ethernet_speeds": MetricInfo(),
    "status_alert_roaming": MetricInfo(),
    "status_alert_install_pending": MetricInfo(),
    "status_alert_is_heating": MetricInfo(),
    "status_alert_power_supply_thermal_throttle": MetricInfo(),
    "status_alert_slow_ethernet_speeds_100": MetricInfo(),
    "status_alert_is_power_save_idle": MetricInfo(),
    "status_alert_moving_while_not_mobile": MetricInfo(),
    "status_alert_moving_too_fast_for_policy": MetricInfo(),
    "status_alert_dbf_telem_stale": MetricInfo(),
    "status_alert_low_motor_current": MetricInfo(),
    "status_alert_obstruction_map_reset": MetricInfo(),
    "status_alert_lower_signal_than_predicted": MetricInfo(),
    "ping_stats_samples": MetricInfo(kind="counter"),
    "ping_stats_end_counter": MetricInfo(kind="counter"),
    "usage_download_usage": MetricInfo(unit="bytes", kind="counter"),
    "usage_upload_usage": MetricInfo(unit="bytes", kind="counter"),
    "power_latest_power": MetricInfo(),
    "power_mean_power": MetricInfo(),
    "power_min_power": MetricInfo(),
    "power_max_power": MetricInfo(),
    "power_total_energy": MetricInfo(),
}

STATE_VALUES = [
    "UNKNOWN",
    "CONNECTED",
    "BOOTING",
    "SEARCHING",
    "STOWED",
    "THERMAL_SHUTDOWN",
    "NO_SATS",
    "OBSTRUCTED",
    "NO_DOWNLINK",
    "NO_PINGS",
    "DISH_UNREACHABLE",
]


class Metric:
    name = ""
    timestamp = ""
    kind = None
    help = None
    values = None

    def __init__(self, name, timestamp, kind="gauge", help="", values=None):
        self.name = name
        self.timestamp = timestamp
        self.kind = kind
        self.help = help
        if values:
            self.values = values
        else:
            self.values = []
        pass

    def __str__(self):
        if not self.values:
            return ""

        lines = []
        lines.append(f"# HELP {self.name} {self.help}")
        lines.append(f"# TYPE {self.name} {self.kind}")
        for value in self.values:
            lines.append(f"{self.name}{value} {self.timestamp*1000}")
        lines.append("")
        return str.join("\n", lines)


class MetricValue:
    value = 0
    labels = None

    def __init__(self, value, labels=None) -> None:
        self.value = value
        self.labels = labels

    def __str__(self):
        label_str = ""
        if self.labels:
            label_str = ("{" + str.join(",", [f'{v[0]}="{v[1]}"'
                                              for v in self.labels.items()]) + "}")
        return f"{label_str} {self.value}"


def parse_args():
    parser = dish_common.create_arg_parser(output_description="Prometheus exporter",
                                           bulk_history=False)

    group = parser.add_argument_group(title="HTTP server options")
    group.add_argument("--address", default="0.0.0.0", help="IP address to listen on")
    group.add_argument("--port", default=8080, type=int, help="Port to listen on")

    return dish_common.run_arg_parser(parser, modes=["status", "alert_detail", "usage", "location", "power"])


def prometheus_export(opts, gstate):
    raw_data = {}

    def data_add_item(name, value, category):
        raw_data[category + "_" + name] = value
        pass

    def data_add_sequencem(name, value, category, start):
        raise NotImplementedError("Did not expect sequence data")

    with gstate.lock:
        rc, status_ts, hist_ts = dish_common.get_data(opts, gstate, data_add_item,
                                                      data_add_sequencem)

    metrics = []

    # snr is not supported by starlink any more but still returned by the grpc
    # service for backwards compatibility
    if "status_snr" in raw_data:
        del raw_data["status_snr"]

    metrics.append(
        Metric(
            name="starlink_status_state",
            timestamp=status_ts,
            values=[
                MetricValue(
                    value=int(raw_data["status_state"] == state_value),
                    labels={"state": state_value},
                ) for state_value in STATE_VALUES
            ],
        ))
    del raw_data["status_state"]

    info_metrics = ["status_id", "status_hardware_version", "status_software_version"]
    metrics_not_found = []
    metrics_not_found.extend([x for x in info_metrics if x not in raw_data])

    if len(metrics_not_found) < len(info_metrics):
        metrics.append(
            Metric(
                name="starlink_info",
                timestamp=status_ts,
                values=[
                    MetricValue(
                        value=1,
                        labels={
                            x.replace("status_", ""): raw_data.pop(x) for x in info_metrics
                            if x in raw_data
                        },
                    )
                ],
            ))

    for name, metric_info in METRICS_INFO.items():
        if name in raw_data:
            metrics.append(
                Metric(
                    name=f"starlink_{name}{metric_info.unit}",
                    timestamp=status_ts,
                    kind=metric_info.kind,
                    values=[MetricValue(value=float(raw_data.pop(name) or 0))],
                ))
        else:
            metrics_not_found.append(name)

    metrics.append(
        Metric(
            name="starlink_exporter_unprocessed_metrics",
            timestamp=status_ts,
            values=[MetricValue(value=1, labels={"metric": name}) for name in raw_data],
        ))

    metrics.append(
        Metric(
            name="starlink_exporter_missing_metrics",
            timestamp=status_ts,
            values=[MetricValue(
                value=1,
                labels={"metric": name},
            ) for name in metrics_not_found],
        ))

    return str.join("\n", [str(metric) for metric in metrics])


class MetricsRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.partition("?")[0]
        if path.lower() == "/favicon.ico":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        opts = self.server.opts
        gstate = self.server.gstate

        content = prometheus_export(opts, gstate)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-Length", len(content))
        self.end_headers()
        self.wfile.write(content.encode())


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s", stream=sys.stderr)

    gstate = dish_common.GlobalState(target=opts.target)
    gstate.lock = threading.Lock()

    httpd = ThreadingHTTPServer((opts.address, opts.port), MetricsRequestHandler)
    httpd.daemon_threads = False
    httpd.opts = opts
    httpd.gstate = gstate

    signal.signal(signal.SIGTERM, handle_sigterm)

    print("HTTP listening on port", opts.port)
    try:
        httpd.serve_forever()
    except (KeyboardInterrupt, Terminated):
        pass
    finally:
        httpd.server_close()
        httpd.gstate.shutdown()

    sys.exit()


if __name__ == "__main__":
    main()
