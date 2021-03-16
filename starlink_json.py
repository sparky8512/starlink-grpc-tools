"""Parser for JSON format gRPC output from a Starlink user terminal.

Expects input as from grpcurl get_history request.

Handling output for other request responses may be added in the future, but
the others don't really need as much interpretation as the get_history
response does.

See the starlink_grpc module docstring for descriptions of the stat elements.
"""

import json
import math
import statistics
import sys

from itertools import chain


class JsonError(Exception):
    """Provides error info when something went wrong with JSON parsing."""


def history_bulk_field_names():
    """Return the field names of the bulk history data.

    Note:
        See `starlink_grpc` module docs regarding brackets in field names.

    Returns:
        A tuple with 2 lists, the first with general data names, the second
        with bulk history data names.
    """
    return [
        "samples",
        "end_counter",
    ], [
        "pop_ping_drop_rate[]",
        "pop_ping_latency_ms[]",
        "downlink_throughput_bps[]",
        "uplink_throughput_bps[]",
        "snr[]",
        "scheduled[]",
        "obstructed[]",
    ]


def history_ping_field_names():
    """Deprecated. Use history_stats_field_names instead."""
    return history_stats_field_names()[0:3]


def history_stats_field_names():
    """Return the field names of the packet loss stats.

    Note:
        See `starlink_grpc` module docs regarding brackets in field names.

    Returns:
        A tuple with 6 lists, with general data names, ping drop stat names,
        ping drop run length stat names, ping latency stat names, loaded ping
        latency stat names, and bandwidth usage stat names, in that order.

        Note:
            Additional lists may be added to this tuple in the future with
            additional data groups, so it not recommended for the caller to
            assume exactly 6 elements.
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
        "run_seconds[1,61]",
        "run_minutes[1,61]",
    ], [
        "mean_all_ping_latency",
        "deciles_all_ping_latency[11]",
        "mean_full_ping_latency",
        "deciles_full_ping_latency[11]",
        "stdev_full_ping_latency",
    ], [
        "load_bucket_samples[15]",
        "load_bucket_min_latency[15]",
        "load_bucket_median_latency[15]",
        "load_bucket_max_latency[15]",
    ], [
        "download_usage",
        "upload_usage",
    ]


def get_history(filename):
    """Read JSON data and return the raw history in dict format.

    Args:
        filename (str): Filename from which to read JSON data, or "-" to read
            from standard input.

    Raises:
        Various exceptions depending on Python version: Failure to open or
            read input or invalid JSON read on input.
    """
    if filename == "-":
        json_data = json.load(sys.stdin)
    else:
        with open(filename) as json_file:
            json_data = json.load(json_file)
    return json_data["dishGetHistory"]


def _compute_sample_range(history, parse_samples, verbose=False):
    current = int(history["current"])
    samples = len(history["popPingDropRate"])

    if verbose:
        print("current counter:       " + str(current))
        print("All samples:           " + str(samples))

    samples = min(samples, current)

    if verbose:
        print("Valid samples:         " + str(samples))

    if parse_samples < 0 or samples < parse_samples:
        parse_samples = samples

    start = current - parse_samples

    if start == current:
        return range(0), 0, current

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


def history_bulk_data(filename, parse_samples, verbose=False):
    """Fetch history data for a range of samples.

    Args:
        filename (str): Filename from which to read JSON data, or "-" to read
            from standard input.
        parse_samples (int): Number of samples to process, or -1 to parse all
            available samples.
        verbose (bool): Optionally produce verbose output.

    Returns:
        A tuple with 2 dicts, the first mapping general data names to their
        values and the second mapping bulk history data names to their values.

        Note: The field names in the returned data do _not_ include brackets
            to indicate sequences, since those would just need to be parsed
            out.  The general data is all single items and the bulk history
            data is all sequences.

    Raises:
        JsonError: Failure to open, read, or parse JSON on input.
    """
    try:
        history = get_history(filename)
    except ValueError as e:
        raise JsonError("Failed to parse JSON: " + str(e))
    except Exception as e:
        raise JsonError(e)

    sample_range, parsed_samples, current = _compute_sample_range(history,
                                                                  parse_samples,
                                                                  verbose=verbose)

    pop_ping_drop_rate = []
    pop_ping_latency_ms = []
    downlink_throughput_bps = []
    uplink_throughput_bps = []
    snr = []
    scheduled = []
    obstructed = []

    for i in sample_range:
        pop_ping_drop_rate.append(history["popPingDropRate"][i])
        pop_ping_latency_ms.append(
            history["popPingLatencyMs"][i] if history["popPingDropRate"][i] < 1 else None)
        downlink_throughput_bps.append(history["downlinkThroughputBps"][i])
        uplink_throughput_bps.append(history["uplinkThroughputBps"][i])
        snr.append(history["snr"][i])
        scheduled.append(history["scheduled"][i])
        obstructed.append(history["obstructed"][i])

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


def history_ping_stats(filename, parse_samples, verbose=False):
    """Deprecated. Use history_stats instead."""
    return history_stats(filename, parse_samples, verbose=verbose)[0:3]


def history_stats(filename, parse_samples, verbose=False):
    """Fetch, parse, and compute ping and usage stats.

    Args:
        filename (str): Filename from which to read JSON data, or "-" to read
            from standard input.
        parse_samples (int): Number of samples to process, or -1 to parse all
            available samples.
        verbose (bool): Optionally produce verbose output.

    Returns:
        A tuple with 6 dicts, mapping general data names, ping drop stat
        names, ping drop run length stat names, ping latency stat names,
        loaded ping latency stat names, and bandwidth usage stat names to
        their respective values, in that order.

        Note:
            Additional dicts may be added to this tuple in the future with
            additional data groups, so it not recommended for the caller to
            assume exactly 6 elements.

    Raises:
        JsonError: Failure to open, read, or parse JSON on input.
    """
    try:
        history = get_history(filename)
    except ValueError as e:
        raise JsonError("Failed to parse JSON: " + str(e))
    except Exception as e:
        raise JsonError(e)

    sample_range, parsed_samples, current = _compute_sample_range(history,
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

    usage_down = 0.0
    usage_up = 0.0

    rtt_full = []
    rtt_all = []
    rtt_buckets = [[] for _ in range(15)]

    for i in sample_range:
        d = history["popPingDropRate"][i]
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
        if not history["scheduled"][i]:
            count_unsched += 1
            total_unsched_drop += d
            if d >= 1:
                count_full_unsched += 1
        # scheduled=false and obstructed=true do not ever appear to overlap,
        # but in case they do in the future, treat that as just unscheduled
        # in order to avoid double-counting it.
        elif history["obstructed"][i]:
            count_obstruct += 1
            total_obstruct_drop += d
            if d >= 1:
                count_full_obstruct += 1
        tot += d

        down = history["downlinkThroughputBps"][i]
        usage_down += down
        up = history["uplinkThroughputBps"][i]
        usage_up += up

        rtt = history["popPingLatencyMs"][i]
        # note that "full" here means the opposite of ping drop full
        if d == 0.0:
            rtt_full.append(rtt)
            if down + up > 500000:
                rtt_buckets[min(14, int(math.log2((down+up) / 500000)))].append(rtt)
            else:
                rtt_buckets[0].append(rtt)
        if d < 1.0:
            rtt_all.append((rtt, 1.0 - d))

    # If the entire sample set is one big drop run, it will be both initial
    # fragment (continued from prior sample range) and final one (continued
    # to next sample range), but to avoid double-reporting, just call it
    # the initial run.
    if init_run_length is None:
        init_run_length = run_length
        run_length = 0

    def weighted_mean_and_quantiles(data, n):
        if not data:
            return None, [None] * (n+1)
        total_weight = sum(x[1] for x in data)
        result = []
        items = iter(data)
        value, accum_weight = next(items)
        accum_value = value * accum_weight
        for boundary in (total_weight * x / n for x in range(n)):
            while accum_weight < boundary:
                try:
                    value, weight = next(items)
                    accum_value += value * weight
                    accum_weight += weight
                except StopIteration:
                    # shouldn't happen, but in case of float precision weirdness...
                    break
            result.append(value)
        result.append(data[-1][0])
        accum_value += sum(x[0] for x in items)
        return accum_value / total_weight, result

    bucket_samples = []
    bucket_min = []
    bucket_median = []
    bucket_max = []
    for bucket in rtt_buckets:
        if bucket:
            bucket_samples.append(len(bucket))
            bucket_min.append(min(bucket))
            bucket_median.append(statistics.median(bucket))
            bucket_max.append(max(bucket))
        else:
            bucket_samples.append(0)
            bucket_min.append(None)
            bucket_median.append(None)
            bucket_max.append(None)

    rtt_all.sort(key=lambda x: x[0])
    wmean_all, wdeciles_all = weighted_mean_and_quantiles(rtt_all, 10)
    rtt_full.sort()
    mean_full, deciles_full = weighted_mean_and_quantiles(tuple((x, 1.0) for x in rtt_full), 10)

    return {
        "samples": parsed_samples,
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
        "run_seconds[1,]": second_runs,
        "run_minutes[1,]": minute_runs,
    }, {
        "mean_all_ping_latency": wmean_all,
        "deciles_all_ping_latency[]": wdeciles_all,
        "mean_full_ping_latency": mean_full,
        "deciles_full_ping_latency[]": deciles_full,
        "stdev_full_ping_latency": statistics.pstdev(rtt_full) if rtt_full else None,
    }, {
        "load_bucket_samples[]": bucket_samples,
        "load_bucket_min_latency[]": bucket_min,
        "load_bucket_median_latency[]": bucket_median,
        "load_bucket_max_latency[]": bucket_max,
    }, {
        "download_usage": int(round(usage_down / 8)),
        "upload_usage": int(round(usage_up / 8)),
    }
