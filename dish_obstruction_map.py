#!/usr/bin/python3
"""Write a PNG image representing Starlink obstruction map data.

This scripts queries obstruction map data from the Starlink user terminal
reachable on the local network and writes a PNG image based on that data.
"""

import argparse
import logging
import os
import png
import sys

import starlink_grpc

DEFAULT_OBSTRUCTED_COLOR = "FFFF0000"
DEFAULT_UNOBSTRUCTED_COLOR = "FFFFFFFF"
DEFAULT_NO_DATA_COLOR = "00000000"
DEFAULT_OBSTRUCTED_GREYSCALE = "FF00"
DEFAULT_UNOBSTRUCTED_GREYSCALE = "FFFF"
DEFAULT_NO_DATA_GREYSCALE = "0000"


def run_loop(opts, context):
    snr_data = starlink_grpc.obstruction_map(context)

    def pixel_bytes(row):
        for point in row:
            if point > 1.0:
                # shouldn't happen, but just in case...
                point = 1.0

            if point >= 0.0:
                if opts.greyscale:
                    yield round(point * opts.unobstructed_color_g +
                                (1.0-point) * opts.obstructed_color_g)
                else:
                    yield round(point * opts.unobstructed_color_r +
                                (1.0-point) * opts.obstructed_color_r)
                    yield round(point * opts.unobstructed_color_g +
                                (1.0-point) * opts.obstructed_color_g)
                    yield round(point * opts.unobstructed_color_b +
                                (1.0-point) * opts.obstructed_color_b)
                if not opts.no_alpha:
                    yield round(point * opts.unobstructed_color_a +
                                (1.0-point) * opts.obstructed_color_a)
            else:
                if opts.greyscale:
                    yield opts.no_data_color_g
                else:
                    yield opts.no_data_color_r
                    yield opts.no_data_color_g
                    yield opts.no_data_color_b
                if not opts.no_alpha:
                    yield opts.no_data_color_a

    if opts.filename == "-":
        # Open new stdout file to get binary mode
        out_file = os.fdopen(sys.stdout.fileno(), "wb", closefd=False)
    else:
        out_file = open(opts.filename, "wb")
    if not snr_data or not snr_data[0]:
        logging.error("Invalid SNR map data: Zero-length")
        return 1
    writer = png.Writer(len(snr_data[0]),
                        len(snr_data),
                        alpha=(not opts.no_alpha),
                        greyscale=opts.greyscale)
    writer.write(out_file, (bytes(pixel_bytes(row)) for row in snr_data))
    out_file.close()
    return 0


def main():
    logging.basicConfig(format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(
        description="Collect directional obstruction map data from a Starlink user terminal and "
        "emit it as a PNG image")
    parser.add_argument("filename", help="The image file to write, or - to write to stdout")
    parser.add_argument(
        "-o",
        "--obstructed-color",
        help="Color of obstructed areas, in RGB, ARGB, L, or AL hex notation, default: " +
        DEFAULT_OBSTRUCTED_COLOR + " or " + DEFAULT_OBSTRUCTED_GREYSCALE)
    parser.add_argument(
        "-u",
        "--unobstructed-color",
        help="Color of unobstructed areas, in RGB, ARGB, L, or AL hex notation, default: " +
        DEFAULT_UNOBSTRUCTED_COLOR + " or " + DEFAULT_UNOBSTRUCTED_GREYSCALE)
    parser.add_argument(
        "-n",
        "--no-data-color",
        help="Color of areas with no data, in RGB, ARGB, L, or AL hex notation, default: " +
        DEFAULT_NO_DATA_COLOR + " or " + DEFAULT_NO_DATA_GREYSCALE)
    parser.add_argument(
        "-g",
        "--greyscale",
        action="store_true",
        help=
        "Emit a greyscale image instead of the default full color image; greyscale images use L or AL hex notation for the color options"
    )
    parser.add_argument(
        "-z",
        "--no-alpha",
        action="store_true",
        help=
        "Emit an image without alpha (transparency) channel instead of the default that includes alpha channel"
    )
    parser.add_argument("-t",
                        "--target",
                        help="host:port of dish to query, default is the standard IP address "
                        "and port (192.168.100.1:9200)")
    opts = parser.parse_args()

    if opts.obstructed_color is None:
        opts.obstructed_color = DEFAULT_OBSTRUCTED_GREYSCALE if opts.greyscale else DEFAULT_OBSTRUCTED_COLOR
    if opts.unobstructed_color is None:
        opts.unobstructed_color = DEFAULT_UNOBSTRUCTED_GREYSCALE if opts.greyscale else DEFAULT_UNOBSTRUCTED_COLOR
    if opts.no_data_color is None:
        opts.no_data_color = DEFAULT_NO_DATA_GREYSCALE if opts.greyscale else DEFAULT_NO_DATA_COLOR

    for option in ("obstructed_color", "unobstructed_color", "no_data_color"):
        try:
            color = int(getattr(opts, option), 16)
            if opts.greyscale:
                setattr(opts, option + "_a", (color >> 8) & 255)
                setattr(opts, option + "_g", color & 255)
            else:
                setattr(opts, option + "_a", (color >> 24) & 255)
                setattr(opts, option + "_r", (color >> 16) & 255)
                setattr(opts, option + "_g", (color >> 8) & 255)
                setattr(opts, option + "_b", color & 255)
        except ValueError:
            logging.error("Invalid hex number for %s", option)
            sys.exit(1)

    context = starlink_grpc.ChannelContext(target=opts.target)

    try:
        # XXX: make this actually run in a loop...
        rc = run_loop(opts, context)
    finally:
        context.close()

    sys.exit(rc)


if __name__ == '__main__':
    main()
