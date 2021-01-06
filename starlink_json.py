"""Parser for JSON format gRPC output from a Starlink user terminal.

Expects input as from grpcurl get_history request.

Handling output for other request responses may be added in the future, but
the others don't really need as much interpretation as the get_history
response does.

See the starlink_grpc module docstring for descriptions of the stat elements.
"""

import json
import sys

from itertools import chain

def history_ping_field_names():
    """Return the field names of the packet loss stats.

    Returns:
        A tuple with 2 lists, the first with general stat names and the
        second with ping drop run length stat names.
    """
    return [
        "samples",
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

def get_history(filename):
    """Read JSON data and return the raw history in dict format.

    Args:
        filename (str): Filename from which to read JSON data, or "-" to read
            from standard input.
    """
    if filename == "-":
        json_data = json.load(sys.stdin)
    else:
        json_file = open(filename)
        try:
            json_data = json.load(json_file)
        finally:
            json_file.close()
    return json_data["dishGetHistory"]

def history_ping_stats(filename, parse_samples, verbose=False):
    """Fetch, parse, and compute the packet loss stats.

    Args:
        filename (str): Filename from which to read JSON data, or "-" to read
            from standard input.
        parse_samples (int): Number of samples to process, or -1 to parse all
            available samples.
        verbose (bool): Optionally produce verbose output.

    Returns:
        On success, a tuple with 2 dicts, the first mapping general stat names
        to their values and the second mapping ping drop run length stat names
        to their values.

        On failure, the tuple (None, None).
    """
    try:
        history = get_history(filename)
    except Exception as e:
        if verbose:
            print("Failed getting history: " + str(e))
        return None, None

    # "current" is the count of data samples written to the ring buffer,
    # irrespective of buffer wrap.
    current = int(history["current"])
    samples = len(history["popPingDropRate"])

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
        d = history["popPingDropRate"][i]
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
        if not history["scheduled"][i]:
            count_unsched += 1
            total_unsched_drop += d
            if d >= 1:
                count_full_unsched += d
        # scheduled=false and obstructed=true do not ever appear to overlap,
        # but in case they do in the future, treat that as just unscheduled
        # in order to avoid double-counting it.
        elif history["obstructed"][i]:
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
        "samples": parse_samples,
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
