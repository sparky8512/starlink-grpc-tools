"""Helpers for grpc communication with a Starlink user terminal.

This module may eventually contain more expansive parsing logic, but for now
it contains functions to parse the history data for some specific packet loss
statistics.

General statistics:
    This group of statistics contains data relevant to all the other groups.

    The sample interval is currently 1 second.

        samples: The number of valid samples analyzed.

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
        run_seconds: A 60 element list. Each element records the total amount
            of time, in sample intervals, that experienced 100% ping drop in
            a consecutive run that lasted for (list index + 1) sample
            intervals (seconds). That is, the first element contains time
            spent in 1 sample runs, the second element contains time spent in
            2 sample runs, etc.
        run_minutes: A 60 element list. Each element records the total amount
            of time, in sample intervals, that experienced 100% ping drop in
            a consecutive run that lasted for more that (list index + 1)
            multiples of 60 sample intervals (minutes), but less than or equal
            to (list index + 2) multiples of 60 sample intervals. Except for
            the last element in the list, which records the total amount of
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

def history_ping_field_names():
    """Return the field names of the packet loss stats.

    Returns:
        A tuple with 3 lists, the first with general stat names, the second
        with ping drop stat names, and the third with ping drop run length
        stat names.
    """
    return [
        "samples"
    ], [
        "total_ping_drop",
        "count_full_ping_drop",
        "count_obstructed",
        "total_obstructed_ping_drop",
        "count_full_obstructed_ping_drop",
        "count_unscheduled",
        "total_unscheduled_ping_drop",
        "count_full_unscheduled_ping_drop"
    ], [
        "init_run_fragment",
        "final_run_fragment",
        "run_seconds",
        "run_minutes"
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

def history_ping_stats(parse_samples, verbose=False):
    """Fetch, parse, and compute the packet loss stats.

    Args:
        parse_samples (int): Number of samples to process, or -1 to parse all
            available samples.
        verbose (bool): Optionally produce verbose output.

    Returns:
        On success, a tuple with 3 dicts, the first mapping general stat names
        to their values, the second mapping ping drop stat names to their
        values and the third mapping ping drop run length stat names to their
        values.

        On failure, the tuple (None, None, None).
    """
    try:
        history = get_history()
    except grpc.RpcError:
        if verbose:
            # RpcError is too verbose to print the details.
            print("Failed getting history")
        return None, None, None

    # 'current' is the count of data samples written to the ring buffer,
    # irrespective of buffer wrap.
    current = int(history.current)
    samples = len(history.pop_ping_drop_rate)

    if verbose:
        print("current counter:       " + str(current))
        print("All samples:           " + str(samples))

    samples = min(samples, current)

    if verbose:
        print("Valid samples:         " + str(samples))

    # This is ring buffer offset, so both index to oldest data sample and
    # index to next data sample after the newest one.
    offset = current % samples

    tot = 0
    count_full_drop = 0
    count_unsched = 0
    total_unsched_drop = 0
    count_full_unsched = 0
    count_obstruct = 0
    total_obstruct_drop = 0
    count_full_obstruct = 0

    second_runs = [0] * 60
    minute_runs = [0] * 60
    run_length = 0
    init_run_length = None

    if parse_samples < 0 or samples < parse_samples:
        parse_samples = samples

    # Parse the most recent parse_samples-sized set of samples. This will
    # iterate samples in order from oldest to newest.
    if parse_samples <= offset:
        sample_range = range(offset - parse_samples, offset)
    else:
        sample_range = chain(range(samples + offset - parse_samples, samples), range(0, offset))

    for i in sample_range:
        d = history.pop_ping_drop_rate[i]
        tot += d
        if d >= 1:
            count_full_drop += d
            run_length += 1
        elif run_length > 0:
            if init_run_length is None:
                init_run_length = run_length
            else:
                if run_length <= 60:
                    second_runs[run_length - 1] += run_length
                else:
                    minute_runs[min((run_length - 1)//60 - 1, 59)] += run_length
            run_length = 0
        elif init_run_length is None:
            init_run_length = 0
        if not history.scheduled[i]:
            count_unsched += 1
            total_unsched_drop += d
            if d >= 1:
                count_full_unsched += d
        # scheduled=false and obstructed=true do not ever appear to overlap,
        # but in case they do in the future, treat that as just unscheduled
        # in order to avoid double-counting it.
        elif history.obstructed[i]:
            count_obstruct += 1
            total_obstruct_drop += d
            if d >= 1:
                count_full_obstruct += d

    # If the entire sample set is one big drop run, it will be both initial
    # fragment (continued from prior sample range) and final one (continued
    # to next sample range), but to avoid double-reporting, just call it
    # the initial run.
    if init_run_length is None:
        init_run_length = run_length
        run_length = 0

    return {
        "samples": parse_samples
    }, {
        "total_ping_drop": tot,
        "count_full_ping_drop": count_full_drop,
        "count_obstructed": count_obstruct,
        "total_obstructed_ping_drop": total_obstruct_drop,
        "count_full_obstructed_ping_drop": count_full_obstruct,
        "count_unscheduled": count_unsched,
        "total_unscheduled_ping_drop": total_unsched_drop,
        "count_full_unscheduled_ping_drop": count_full_unsched
    }, {
        "init_run_fragment": init_run_length,
        "final_run_fragment": run_length,
        "run_seconds": second_runs,
        "run_minutes": minute_runs
    }
