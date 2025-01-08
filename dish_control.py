#!/usr/bin/env python3
"""Manipulate operating state of a Starlink user terminal."""

import argparse
import logging
import sys

import grpc
from yagrc import reflector as yagrc_reflector

import loop_util


def parse_args():
    parser = argparse.ArgumentParser(description="Starlink user terminal state control")
    parser.add_argument("-e",
                        "--target",
                        default="192.168.100.1:9200",
                        help="host:port of dish to query, default is the standard IP address "
                        "and port (192.168.100.1:9200)")
    subs = parser.add_subparsers(dest="command", required=True)
    subs.add_parser("reboot", help="Reboot the user terminal")
    subs.add_parser("stow", help="Set user terminal to stow position")
    subs.add_parser("unstow", help="Restore user terminal from stow position")
    sleep_parser = subs.add_parser(
        "set_sleep",
        help="Show, set, or disable power save configuration",
        description="Run without arguments to show current configuration")
    sleep_parser.add_argument("start",
                              nargs="?",
                              type=int,
                              help="Start time in minutes past midnight UTC")
    sleep_parser.add_argument("duration",
                              nargs="?",
                              type=int,
                              help="Duration in minutes, or 0 to disable")
    gps_parser = subs.add_parser(
        "set_gps",
        help="Enable, disable, or show usage of GPS for position data",
        description="Run without arguments to show current configuration")
    gps_parser.add_argument("--enable",
                            action=argparse.BooleanOptionalAction,
                            help="Enable/disable use of GPS for position data")
    loop_util.add_args(parser)

    opts = parser.parse_args()

    if opts.command == "set_sleep" and opts.start is not None:
        if opts.duration is None:
            sleep_parser.error("Must specify duration if start time is specified")
        if opts.start < 0 or opts.start >= 1440:
            sleep_parser.error("Invalid start time, must be >= 0 and < 1440")
        if opts.duration < 0 or opts.duration > 1440:
            sleep_parser.error("Invalid duration, must be >= 0 and <= 1440")
    loop_util.check_args(opts, parser)

    return opts


def loop_body(opts):
    reflector = yagrc_reflector.GrpcReflectionClient()
    try:
        with grpc.insecure_channel(opts.target) as channel:
            reflector.load_protocols(channel, symbols=["SpaceX.API.Device.Device"])
            stub = reflector.service_stub_class("SpaceX.API.Device.Device")(channel)
            request_class = reflector.message_class("SpaceX.API.Device.Request")
            if opts.command == "reboot":
                request = request_class(reboot={})
            elif opts.command == "stow":
                request = request_class(dish_stow={})
            elif opts.command == "unstow":
                request = request_class(dish_stow={"unstow": True})
            elif opts.command == "set_sleep":
                if opts.start is None and opts.duration is None:
                    request = request_class(dish_get_config={})
                else:
                    if opts.duration:
                        request = request_class(
                            dish_power_save={
                                "power_save_start_minutes": opts.start,
                                "power_save_duration_minutes": opts.duration,
                                "enable_power_save": True
                            })
                    else:
                        # duration of 0 not allowed, even when disabled
                        request = request_class(dish_power_save={
                            "power_save_duration_minutes": 1,
                            "enable_power_save": False
                        })
            elif opts.command == "set_gps":
                if opts.enable is None:
                    request = request_class(get_status={})
                else:
                    request = request_class(dish_inhibit_gps={"inhibit_gps": not opts.enable})

            response = stub.Handle(request, timeout=10)

            if opts.command == "set_sleep" and opts.start is None and opts.duration is None:
                config = response.dish_get_config.dish_config
                if config.power_save_mode:
                    print("Sleep start:", config.power_save_start_minutes,
                          "minutes past midnight UTC")
                    print("Sleep duration:", config.power_save_duration_minutes, "minutes")
                else:
                    print("Sleep disabled")
            elif opts.command == "set_gps" and opts.enable is None:
                status = response.dish_get_status
                if status.gps_stats.inhibit_gps:
                    print("GPS disabled")
                else:
                    print("GPS enabled")
    except (AttributeError, ValueError, grpc.RpcError) as e:
        if isinstance(e, grpc.Call):
            msg = e.details()
        elif isinstance(e, (AttributeError, ValueError)):
            msg = "Protocol error"
        else:
            msg = "Unknown communication or service error"
        logging.error(msg)
        return 1

    return 0


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    rc = loop_util.run_loop(opts, loop_body, opts)
    sys.exit(rc)


if __name__ == "__main__":
    main()
