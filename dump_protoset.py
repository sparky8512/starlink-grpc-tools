#!/usr/bin/python3
"""Dump gRPC service information from a file or a reflection server

This script will query a gRPC reflection server for descriptor information of
all services supported by the server, excluding the reflection service itself,
and dump it in a textual format, or read previous saved descriptor information
and dump that.

Although the default target option is the local IP and port number used by the
gRPC service on a Starlink user terminal, this script is otherwise not
specific to Starlink and should work for any gRPC server that does not require
SSL and that has the reflection service enabled.
"""

import argparse
import logging
import sys
import time

import grpc
from google.protobuf import descriptor_pb2
from yagrc import dump
from yagrc import reflector

TARGET_DEFAULT = "192.168.100.1:9200"
RETRY_DELAY_DEFAULT = 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Textually dump a serialized FileDescriptorSet (protoset) from "
        "either a file or a gRPC reflection server")

    parser.add_argument("in_file",
                        nargs="?",
                        metavar="IN_FILE",
                        help="File from which to read protoset data instead of getting "
                        "it via reflection")
    parser.add_argument("-g",
                        "--target",
                        default=TARGET_DEFAULT,
                        help="host:port of device to query, default: " + TARGET_DEFAULT)
    parser.add_argument("-r",
                        "--retry-delay",
                        type=float,
                        default=float(RETRY_DELAY_DEFAULT),
                        help="Time in seconds to wait before retrying after network "
                        "error or 0 for no retry, default: " + str(RETRY_DELAY_DEFAULT))

    opts = parser.parse_args()

    return opts


def defix(name, prefix):
    if prefix:
        packages = prefix.split(".")[1:]
        if packages:
            packages[0] = "." + packages[0]
            for package in packages:
                if name.startswith(package + "."):
                    name = name.removeprefix(package + ".")
                else:
                    break
    return name


def dump_service(indent, service, prefix):
    name = [indent, "service", service.name]
    if service.options.deprecated:
        name.append("deprecated")
    print(*name)
    indent = indent + "    "
    for method in service.method:
        items = [indent, " rpc ", method.name, "("]
        if method.client_streaming:
            items.append("stream ")
        items.extend([defix(method.input_type, prefix), ") returns ("])
        if method.server_streaming:
            items.append("stream ")
        items.extend([defix(method.output_type, prefix), ")"])
        if method.options.deprecated:
            items.append("deprecated")
        print(*items, sep="")


def dump_enum(indent, enum):
    name = [indent, "enum", enum.name]
    if enum.options.deprecated:
        name.append("deprecated")
    print(*name)
    indent = indent + "    "
    for value in enum.value:
        items = [indent, value.name, "=", value.number]
        if value.options.deprecated:
            items.append("deprecated")
        print(*items)


def field_type(field, prefix):
    return defix(
        field.type_name if field.type_name else descriptor_pb2.FieldDescriptorProto.Type.Name(
            field.type).removeprefix("TYPE_").lower(), prefix)


def dump_field(indent, field, prefix, maps):
    items = [indent]
    if field.type_name and not field.type_name.startswith("."):
        full_type = prefix + "." + field.type_name
    else:
        full_type = field.type_name
    if (field.label == descriptor_pb2.FieldDescriptorProto.Label.LABEL_REPEATED
            and full_type in maps):
        key_type, value_type = maps[full_type]
        items.append("".join(
            ["map<", defix(key_type, prefix), ", ",
             defix(value_type, prefix), ">"]))
    else:
        if field.label != descriptor_pb2.FieldDescriptorProto.Label.LABEL_OPTIONAL:
            items.append(
                descriptor_pb2.FieldDescriptorProto.Label.Name(
                    field.label).removeprefix("LABEL_").lower())
        items.append(field_type(field, prefix))
    items.extend([field.name, "=", field.number])
    if field.options.deprecated:
        items.append("deprecated")
    print(*items)


def dump_message(indent, message, prefix, maps):
    full_name = prefix + "." + message.name
    if message.options.map_entry:
        if len(message.field
               ) >= 2 and message.field[0].name == "key" and message.field[1].name == "value":
            maps[full_name] = (field_type(message.field[0],
                                          None), field_type(message.field[1], None))
        # else just assume it is malformed and ignore it
        return
    name = [indent, "message", message.name]
    if message.options.deprecated:
        name.append("deprecated")
    print(*name)
    indent = indent + "    "
    for nested_message in message.nested_type:
        dump_message(indent, nested_message, full_name, maps)
    for enum in message.enum_type:
        dump_enum(indent, enum)
    oneof_emitted = set()
    for field in message.field:
        if field.HasField("oneof_index") and field.oneof_index < len(message.oneof_decl):
            # Nothing in descriptor proto mandates that all the oneof fields
            # for a given index be grouped together, so make a separate pass
            # for each unique index and keep track of which were already seen.
            if field.oneof_index not in oneof_emitted:
                print(indent, "oneof", message.oneof_decl[field.oneof_index].name)
                indent = indent + "    "
                for oneof_field in message.field:
                    if oneof_field.HasField(
                            "oneof_index") and oneof_field.oneof_index == field.oneof_index:
                        dump_field(indent, oneof_field, full_name, {})
                oneof_emitted.add(field.oneof_index)
        else:
            dump_field(indent, field, full_name, maps)
    # Any leftover oneof decls will be for an empty oneof
    for index, decl in enumerate(message.oneof_decl):
        if index not in oneof_emitted:
            print(indent, "oneof", decl.name)


def dump_protoset(protoset):
    file_desc_set = descriptor_pb2.FileDescriptorSet.FromString(protoset)
    for file in file_desc_set.file:
        name = ["file", file.name]
        if file.options.deprecated:
            name.append("deprecated")
        print(*name)
        if file.package:
            print("    package", file.package)
            prefix = "." + file.package
        else:
            prefix = ""
        for depend in file.dependency:
            print("    import", depend)
        for service in file.service:
            dump_service("   ", service, prefix)
        maps = {}
        for message in file.message_type:
            dump_message("   ", message, prefix, maps)
        for enum in file.enum_type:
            dump_enum("   ", enum)
        print()


def reflect_protoset(opts):
    while True:
        try:
            with grpc.insecure_channel(opts.target) as channel:
                return dump.dump_protocols(channel)
            break
        except reflector.ServiceError as e:
            logging.error("Problem with reflection service: %s", str(e))
            # Only retry on network-related errors, not service errors
            return None
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
                return None


def main():
    opts = parse_args()
    logging.basicConfig(format="%(levelname)s: %(message)s")

    if opts.in_file is not None:
        try:
            with open(opts.in_file, mode="rb") as infile:
                protoset = infile.read()
        except OSError as e:
            logging.error("Failed to read file %s: %s", opts.in_file, str(e))
            protoset = None

    else:
        protoset = reflect_protoset(opts)
    if protoset:
        dump_protoset(protoset)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
