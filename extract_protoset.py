#!/usr/bin/env python3
"""Poll and record service information from a gRPC reflection server

This script will query a gRPC reflection server for descriptor information of
all services supported by the server, excluding the reflection service itself,
and write a serialized FileDescriptorSet protobuf containing all returned
descriptors to a file, either once or in a periodic loop. This file can then
be used by any tool that accepts such data, including protoc, the protocol
buffer compiler.

Output files are named with the CRC32 value and byte length of the serialized
FileDescriptorSet data. If those match the name of a file written previously,
the data is assumed not to have changed and no new file is written. For this
reason, it is recommended to use an output directory specific to the server,
to avoid mixing with files written with data from other servers.

Although the default target option is the local IP and port number used by the
gRPC service on a Starlink user terminal, this script is otherwise not
specific to Starlink and should work for any gRPC server that does not require
SSL and that has the reflection service enabled.
"""

import argparse
import binascii
import logging
import os
import sys
import time

import grpc
from yagrc import dump
from yagrc import reflector

TARGET_DEFAULT = "192.168.100.1:9200"
LOOP_TIME_DEFAULT = 0
RETRY_DELAY_DEFAULT = 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Poll a gRPC reflection server and record a serialized "
        "FileDescriptorSet (protoset) of the reflected information")

    parser.add_argument("outdir",
                        nargs="?",
                        metavar="OUTDIR",
                        help="Directory in which to write protoset files")
    parser.add_argument("-g",
                        "--target",
                        default=TARGET_DEFAULT,
                        help="host:port of device to query, default: " + TARGET_DEFAULT)
    parser.add_argument("-n",
                        "--print-only",
                        action="store_true",
                        help="Print the protoset filename instead of writing the data")
    parser.add_argument("-r",
                        "--retry-delay",
                        type=float,
                        default=float(RETRY_DELAY_DEFAULT),
                        help="Time in seconds to wait before retrying after network "
                        "error or 0 for no retry, default: " + str(RETRY_DELAY_DEFAULT))
    parser.add_argument("-t",
                        "--loop-interval",
                        type=float,
                        default=float(LOOP_TIME_DEFAULT),
                        help="Loop interval in seconds or 0 for no loop, default: " +
                        str(LOOP_TIME_DEFAULT))
    parser.add_argument("-v", "--verbose", action="store_true", help="Be verbose")

    opts = parser.parse_args()

    if opts.outdir is None and not opts.print_only:
        parser.error("Output dir is required unless --print-only option set")

    return opts


def loop_body(opts):
    while True:
        try:
            with grpc.insecure_channel(opts.target) as channel:
                protoset = dump.dump_protocols(channel)
            break
        except reflector.ServiceError as e:
            logging.error("Problem with reflection service: %s", str(e))
            # Only retry on network-related errors, not service errors
            return
        except grpc.RpcError as e:
            # grpc.RpcError error message is not very useful, but grpc.Call has
            # something slightly better
            if isinstance(e, grpc.Call):
                msg = e.details()
            else:
                msg = "Unknown communication or service error"
            print("Problem communicating with reflection service:", msg)
            if opts.retry_delay > 0.0:
                time.sleep(opts.retry_delay)
            else:
                return

    filename = "{0:08x}_{1}.protoset".format(binascii.crc32(protoset), len(protoset))
    if opts.print_only:
        print("Protoset:", filename)
    else:
        try:
            with open(filename, mode="xb") as outfile:
                outfile.write(protoset)
            print("New protoset found:", filename)
        except FileExistsError:
            if opts.verbose:
                print("Existing protoset:", filename)


def goto_dir(outdir):
    try:
        outdir_abs = os.path.abspath(outdir)
        os.makedirs(outdir_abs, exist_ok=True)
        os.chdir(outdir)
    except OSError as e:
        logging.error("Output directory error: %s", str(e))
        sys.exit(1)


def main():
    opts = parse_args()
    logging.basicConfig(format="%(levelname)s: %(message)s")
    if not opts.print_only:
        goto_dir(opts.outdir)

    next_loop = time.monotonic()
    while True:
        loop_body(opts)
        if opts.loop_interval > 0.0:
            now = time.monotonic()
            next_loop = max(next_loop + opts.loop_interval, now)
            time.sleep(next_loop - now)
        else:
            break


if __name__ == "__main__":
    main()
