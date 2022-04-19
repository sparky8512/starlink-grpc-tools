#!/usr/bin/python3
"""Manipulate operating state of a Starlink user terminal."""

import argparse
import logging
import sys

import grpc
from yagrc import reflector as yagrc_reflector


def parse_args():
    parser = argparse.ArgumentParser(description="Starlink user terminal state control")
    parser.add_argument("command", choices=["reboot", "stow", "unstow"])
    parser.add_argument("-e",
                        "--target",
                        default="192.168.100.1:9200",
                        help="host:port of dish to query, default is the standard IP address "
                        "and port (192.168.100.1:9200)")
    return parser.parse_args()


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    reflector = yagrc_reflector.GrpcReflectionClient()
    try:
        with grpc.insecure_channel(opts.target) as channel:
            reflector.load_protocols(channel, symbols=["SpaceX.API.Device.Device"])
            request_class = reflector.message_class("SpaceX.API.Device.Request")
            if opts.command == "reboot":
                request = request_class(reboot={})
            elif opts.command == "stow":
                request = request_class(dish_stow={})
            else:  # unstow
                request = request_class(dish_stow={"unstow": True})
            stub = reflector.service_stub_class("SpaceX.API.Device.Device")(channel)
            response = stub.Handle(request, timeout=10)
    except grpc.RpcError as e:
        if isinstance(e, grpc.Call):
            msg = e.details()
        else:
            msg = "Unknown communication or service error"
        logging.error(msg)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
