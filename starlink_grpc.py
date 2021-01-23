"""Helpers for grpc communication with a Starlink user terminal.

This module may eventually contain more expansive parsing logic, but for now
it contains functions to either get the history data as-is or parse it for
some specific packet loss statistics.

Those functions return data grouped into sets, as follows:

General data:
    This set of fields contains data relevant to all the other groups.

    The sample interval is currently 1 second.

        samples: The number of samples analyzed (for statistics) or returned
            (for bulk data).
        end_counter: The total number of data samples that have been written to
            the history buffer since dish reboot, irrespective of buffer wrap.
            This can be used to keep track of how many samples are new in
            comparison to a prior query of the history data.

Bulk history data:
    This group holds the history data as-is for the requested range of
    samples, just unwound from the circular buffers that the raw data holds.
    It contains some of the same fields as the status info, but instead of
    representing the current values, each field contains a sequence of values
    representing the value over time, ending at the current time.

        pop_ping_drop_rate: Fraction of lost ping replies per sample.
        pop_ping_latency_ms: Round trip time, in milliseconds, during the
            sample period, or None if a sample experienced 100% ping drop.
        downlink_throughput_bps: Download usage during the sample period
            (actual, not max available), in bits per second.
        uplink_throughput_bps: Upload usage during the sample period, in bits
            per second.
        snr: Signal to noise ratio during the sample period.
        scheduled: Boolean indicating whether or not a satellite was scheduled
            to be available for transmit/receive during the sample period.
            When false, ping drop shows as "No satellites" in Starlink app.
        obstructed: Boolean indicating whether or not the dish determined the
            signal between it and the satellite was obstructed during the
            sample period. When true, ping drop shows as "Obstructed" in the
            Starlink app.

    There is no specific data field in the raw history data that directly
    correlates with "Other" or "Beta downtime" in the Starlink app (or
    whatever it gets renamed to after beta), but empirical evidence suggests
    any sample where pop_ping_drop_rate is 1, scheduled is true, and
    obstructed is false is counted as "Beta downtime".

    Note that neither scheduled=false nor obstructed=true necessarily means
    packet loss occurred. Those need to be examined in combination with
    pop_ping_drop_rate to be meaningful.

General ping drop (packet loss) statistics:
    This group of statistics characterize the packet loss (labeled "ping drop"
    in the field names of the Starlink gRPC service protocol) in various ways.

        total_ping_drop: The total amount of time, in sample intervals, that
            experienced ping drop.
        count_full_ping_drop: The number of samples that experienced 100%
            ping drop.
        count_obstructed: The number of samples that were marked as
            "obstructed", regardless of whether they experienced any ping
            drop.
        total_obstructed_ping_drop: The total amount of time, in sample
            intervals, that experienced ping drop in samples marked as
            "obstructed".
        count_full_obstructed_ping_drop: The number of samples that were
            marked as "obstructed" and that experienced 100% ping drop.
        count_unscheduled: The number of samples that were not marked as
            "scheduled", regardless of whether they experienced any ping
            drop.
        total_unscheduled_ping_drop: The total amount of time, in sample
            intervals, that experienced ping drop in samples not marked as
            "scheduled".
        count_full_unscheduled_ping_drop: The number of samples that were
            not marked as "scheduled" and that experienced 100% ping drop.

    Total packet loss ratio can be computed with total_ping_drop / samples.

Ping drop run length statistics:
    This group of statistics characterizes packet loss by how long a
    consecutive run of 100% packet loss lasts.

        init_run_fragment: The number of consecutive sample periods at the
            start of the sample set that experienced 100% ping drop. This
            period may be a continuation of a run that started prior to the
            sample set, so is not counted in the following stats.
        final_run_fragment: The number of consecutive sample periods at the
            end of the sample set that experienced 100% ping drop. This
            period may continue as a run beyond the end of the sample set, so
            is not counted in the following stats.
        run_seconds: A 60 element sequence. Each element records the total
            amount of time, in sample intervals, that experienced 100% ping
            drop in a consecutive run that lasted for (index + 1) sample
            intervals (seconds). That is, the first element contains time
            spent in 1 sample runs, the second element contains time spent in
            2 sample runs, etc.
        run_minutes: A 60 element sequence. Each element records the total
            amount of time, in sample intervals, that experienced 100% ping
            drop in a consecutive run that lasted for more that (index + 1)
            multiples of 60 sample intervals (minutes), but less than or equal
            to (index + 2) multiples of 60 sample intervals. Except for the
            last element in the sequence, which records the total amount of
            time in runs of more than 60*60 samples.

    No sample should be counted in more than one of the run length stats or
    stat elements, so the total of all of them should be equal to
    count_full_ping_drop from the ping drop stats.

    Samples that experience less than 100% ping drop are not counted in this
    group of stats, even if they happen at the beginning or end of a run of
    100% ping drop samples. To compute the amount of time that experienced
    ping loss in less than a single run of 100% ping drop, use
    (total_ping_drop - count_full_ping_drop) from the ping drop stats.
"""

from itertools import chain

import grpc

import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc


class GrpcError(Exception):
    """Provides error info when something went wrong with a gRPC call."""
    def __init__(self, e, *args, **kwargs):
        # grpc.RpcError is too verbose to print in whole, but it may also be
        # a Call object, and that class has some minimally useful info.
        if isinstance(e, grpc.Call):
            msg = e.details()
        elif isinstance(e, grpc.RpcError):
            msg = "Unknown communication or service error"
        else:
            msg = str(e)
        super().__init__(msg, *args, **kwargs)


def get_status():
    """Fetch status data and return it in grpc structure format.

    Raises:
        grpc.RpcError: Communication or service error.
    """
    with grpc.insecure_channel("192.168.100.1:9200") as channel:
        stub = spacex.api.device.device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(spacex.api.device.device_pb2.Request(get_status={}))
    return response.dish_get_status


def get_id():
    """Return the ID from the dish status information.

    Returns:
        A string identifying the Starlink user terminal reachable from the
        local network.

    Raises:
        GrpcError: No user terminal is currently reachable.
    """
    try:
        status = get_status()
        return status.device_info.id
    except grpc.RpcError as e:
        raise GrpcError(e)


def history_ping_field_names():
    """Return the field names of the packet loss stats.

    Returns:
        A tuple with 3 lists, the first with general data names, the second
        with ping drop stat names, and the third with ping drop run length
        stat names.
    """
    return [
        "samples",
        "end_counter",
    ], [
        "total_ping_drop",
        "count_full_ping_drop",
        "count_obstructed",
        "total_obstructed_ping_drop",
        "count_full_obstructed_ping_drop",
        "count_unscheduled",
        "total_unscheduled_ping_drop",
        "count_full_unscheduled_ping_drop",
    ], [
        "init_run_fragment",
        "final_run_fragment",
        "run_seconds",
        "run_minutes",
    ]


def get_history():
    """Fetch history data and return it in grpc structure format.

    Raises:
        grpc.RpcError: Communication or service error.
    """
    with grpc.insecure_channel("192.168.100.1:9200") as channel:
        stub = spacex.api.device.device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(spacex.api.device.device_pb2.Request(get_history={}))
    return response.dish_get_history


def _compute_sample_range(history, parse_samples, start=None, verbose=False):
    current = int(history.current)
    samples = len(history.pop_ping_drop_rate)

    if verbose:
        print("current counter:       " + str(current))
        print("All samples:           " + str(samples))

    samples = min(samples, current)

    if verbose:
        print("Valid samples:         " + str(samples))

    if parse_samples < 0 or samples < parse_samples:
        parse_samples = samples

    if start is not None and start > current:
        if verbose:
            print("Counter reset detected, ignoring requested start count")
        start = None

    if start is None or start < current - parse_samples:
        start = current - parse_samples

    # This is ring buffer offset, so both index to oldest data sample and
    # index to next data sample after the newest one.
    end_offset = current % samples
    start_offset = start % samples

    # Set the range for the requested set of samples. This will iterate
    # sample index in order from oldest to newest.
    if start_offset < end_offset:
        sample_range = range(start_offset, end_offset)
    else:
        sample_range = chain(range(start_offset, samples), range(0, end_offset))

    return sample_range, current - start, current


def history_bulk_data(parse_samples, start=None, verbose=False):
    """Fetch history data for a range of samples.

    Args:
        parse_samples (int): Number of samples to process, or -1 to parse all
            available samples (bounded by start, if it is set).
        start (int): Optional. If set, the samples returned will be limited to
            the ones that have a counter value greater than this value. The
            "end_counter" field in the general data dict returned by this
            function represents the counter value of the last data sample
            returned, so if that value is passed as start in a subsequent call
            to this function, only new samples will be returned.
            NOTE: The sample counter will reset to 0 when the dish reboots. If
            the requested start value is greater than the new "end_counter"
            value, this function will assume that happened and treat all
            samples as being later than the requested start, and thus include
            them (bounded by parse_samples, if it is not -1).
        verbose (bool): Optionally produce verbose output.

    Returns:
        A tuple with 2 dicts, the first mapping general data names to their
        values and the second mapping bulk history data names to their values.

    Raises:
        GrpcError: Failed getting history info from the Starlink user
            terminal.
    """
    try:
        history = get_history()
    except grpc.RpcError as e:
        raise GrpcError(e)

    sample_range, parsed_samples, current = _compute_sample_range(history,
                                                                  parse_samples,
                                                                  start=start,
                                                                  verbose=verbose)

    pop_ping_drop_rate = []
    pop_ping_latency_ms = []
    downlink_throughput_bps = []
    uplink_throughput_bps = []
    snr = []
    scheduled = []
    obstructed = []

    for i in sample_range:
        pop_ping_drop_rate.append(history.pop_ping_drop_rate[i])
        pop_ping_latency_ms.append(
            history.pop_ping_latency_ms[i] if history.pop_ping_drop_rate[i] < 1 else None)
        downlink_throughput_bps.append(history.downlink_throughput_bps[i])
        uplink_throughput_bps.append(history.uplink_throughput_bps[i])
        snr.append(history.snr[i])
        scheduled.append(history.scheduled[i])
        obstructed.append(history.obstructed[i])

    return {
        "samples": parsed_samples,
        "end_counter": current,
    }, {
        "pop_ping_drop_rate": pop_ping_drop_rate,
        "pop_ping_latency_ms": pop_ping_latency_ms,
        "downlink_throughput_bps": downlink_throughput_bps,
        "uplink_throughput_bps": uplink_throughput_bps,
        "snr": snr,
        "scheduled": scheduled,
        "obstructed": obstructed,
    }


def history_ping_stats(parse_samples, verbose=False):
    """Fetch, parse, and compute the packet loss stats.

    Args:
        parse_samples (int): Number of samples to process, or -1 to parse all
            available samples.
        verbose (bool): Optionally produce verbose output.

    Returns:
        A tuple with 3 dicts, the first mapping general data names to their
        values, the second mapping ping drop stat names to their values and
        the third mapping ping drop run length stat names to their values.

    Raises:
        GrpcError: Failed getting history info from the Starlink user
            terminal.
    """
    try:
        history = get_history()
    except grpc.RpcError as e:
        raise GrpcError(e)

    sample_range, parse_samples, current = _compute_sample_range(history,
                                                                 parse_samples,
                                                                 verbose=verbose)

    tot = 0.0
    count_full_drop = 0
    count_unsched = 0
    total_unsched_drop = 0.0
    count_full_unsched = 0
    count_obstruct = 0
    total_obstruct_drop = 0.0
    count_full_obstruct = 0

    second_runs = [0] * 60
    minute_runs = [0] * 60
    run_length = 0
    init_run_length = None

    for i in sample_range:
        d = history.pop_ping_drop_rate[i]
        if d >= 1:
            # just in case...
            d = 1
            count_full_drop += 1
            run_length += 1
        elif run_length > 0:
            if init_run_length is None:
                init_run_length = run_length
            else:
                if run_length <= 60:
                    second_runs[run_length - 1] += run_length
                else:
                    minute_runs[min((run_length-1) // 60 - 1, 59)] += run_length
            run_length = 0
        elif init_run_length is None:
            init_run_length = 0
        if not history.scheduled[i]:
            count_unsched += 1
            total_unsched_drop += d
            if d >= 1:
                count_full_unsched += 1
        # scheduled=false and obstructed=true do not ever appear to overlap,
        # but in case they do in the future, treat that as just unscheduled
        # in order to avoid double-counting it.
        elif history.obstructed[i]:
            count_obstruct += 1
            total_obstruct_drop += d
            if d >= 1:
                count_full_obstruct += 1
        tot += d

    # If the entire sample set is one big drop run, it will be both initial
    # fragment (continued from prior sample range) and final one (continued
    # to next sample range), but to avoid double-reporting, just call it
    # the initial run.
    if init_run_length is None:
        init_run_length = run_length
        run_length = 0

    return {
        "samples": parse_samples,
        "end_counter": current,
    }, {
        "total_ping_drop": tot,
        "count_full_ping_drop": count_full_drop,
        "count_obstructed": count_obstruct,
        "total_obstructed_ping_drop": total_obstruct_drop,
        "count_full_obstructed_ping_drop": count_full_obstruct,
        "count_unscheduled": count_unsched,
        "total_unscheduled_ping_drop": total_unsched_drop,
        "count_full_unscheduled_ping_drop": count_full_unsched,
    }, {
        "init_run_fragment": init_run_length,
        "final_run_fragment": run_length,
        "run_seconds": second_runs,
        "run_minutes": minute_runs,
    }
