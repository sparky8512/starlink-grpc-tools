"""Helpers for grpc communication with a Starlink user terminal.

This module contains functions for getting the history and status data and
either return it as-is or parsed for some specific statistics, as well as a
handful of functions related to dish control.

The history and status functions return data grouped into sets, as follows.

Note:
    Functions that return field names may indicate which fields hold sequences
    (which are not necessarily lists) instead of single items. The field names
    returned in those cases will be in one of the following formats:

    : "name[]" : A sequence of indeterminate size (or a size that can be
        determined from other parts of the returned data).
    : "name[n]" : A sequence with exactly n elements.
    : "name[n1,]" : A sequence of indeterminate size with recommended starting
        index label n1.
    : "name[n1,n2]" : A sequence with n2-n1 elements with recommended starting
        index label n1. This is similar to the args to range() builtin.

    For example, the field name "foo[1,5]" could be expanded to "foo_1",
    "foo_2", "foo_3", and "foo_4" (or however else the caller wants to
    indicate index numbers, if at all).

General status data
-------------------
This group holds information about the current state of the user terminal.

: **id** : A string identifying the specific user terminal device that was
    reachable from the local network. Something like a serial number.
: **hardware_version** : A string identifying the user terminal hardware
    version.
: **software_version** : A string identifying the software currently installed
    on the user terminal.
: **state** : As string describing the current connectivity state of the user
    terminal. One of: "UNKNOWN", "CONNECTED", "BOOTING", "SEARCHING", "STOWED",
    "THERMAL_SHUTDOWN", "NO_SATS", "OBSTRUCTED", "NO_DOWNLINK", "NO_PINGS".
: **uptime** : The amount of time, in seconds, since the user terminal last
    rebooted.
: **snr** : Most recent sample value. See bulk history data for detail.
    **OBSOLETE**: The user terminal no longer provides this data.
: **seconds_to_first_nonempty_slot** : Amount of time from now, in seconds,
    until a satellite will be scheduled to be available for transmit/receive.
    See also *scheduled* in the bulk history data. May report as a negative
    number, which appears to indicate unknown time until next satellite
    scheduled and usually correlates with *state* reporting as other than
    "CONNECTED".
: **pop_ping_drop_rate** : Most recent sample value. See bulk history data for
    detail.
: **downlink_throughput_bps** : Most recent sample value. See bulk history
    data for detail.
: **uplink_throughput_bps** : Most recent sample value. See bulk history data
    for detail.
: **pop_ping_latency_ms** : Most recent sample value. See bulk history data
    for detail.
: **alerts** : A bit field combining all active alerts, where a 1 bit
    indicates the alert is active. See alert detail status data for which bits
    correspond with each alert, or to get individual alert flags instead of a
    combined bit mask.
: **fraction_obstructed** : The fraction of total area (or possibly fraction
    of time?) that the user terminal has determined to be obstructed between
    it and the satellites with which it communicates.
: **currently_obstructed** : Most recent sample value. See *obstructed* in
    bulk history data for detail. This item still appears to be reported by
    the user terminal despite no longer appearing in the bulk history data.
: **seconds_obstructed** : The amount of time within the history buffer,
    in seconds, that the user terminal determined to be obstructed, regardless
    of whether or not packets were able to be transmitted or received.
    **OBSOLETE**: The user terminal no longer provides this data.
: **obstruction_duration** : Average consecutive time, in seconds, the user
    terminal has detected its signal to be obstructed for a period of time
    that it considers "prolonged", or None if no such obstructions were
    recorded.
: **obstruction_interval** : Average time, in seconds, between the start of
    such "prolonged" obstructions, or None if no such obstructions were
    recorded.
: **direction_azimuth** : Azimuth angle, in degrees, of the direction in which
    the user terminal's dish antenna is physically pointing. Note that this
    generally is not the exact direction of the satellite with which the user
    terminal is communicating.
: **direction_elevation** : Elevation angle, in degrees, of the direction in
    which the user terminal's dish antenna is physically pointing.
: **is_snr_above_noise_floor** : Boolean indicating whether or not the dish
    considers the signal to noise ratio to be above some minimum threshold for
    connectivity, currently 3dB.

Obstruction detail status data
------------------------------
This group holds additional detail regarding the specific areas the user
terminal has determined to be obstructed.

: **wedges_fraction_obstructed** : A 12 element sequence. Each element
    represents a 30 degree wedge of area and its value indicates the fraction
    of area (time?) within that wedge that the user terminal has determined to
    be obstructed between it and the satellites with which it communicates.
    The values are expressed as a fraction of total, not a fraction of the
    wedge, so max value for each element should be something like 1/12, but
    may vary from wedge to wedge if they are weighted differently. The first
    element in the sequence represents the wedge that spans exactly North to
    30 degrees East of North, and subsequent wedges rotate 30 degrees further
    in the same direction. (It's not clear if this will hold true at all
    latitudes.)
    **OBSOLETE**: The user terminal no longer provides this data.
: **raw_wedges_fraction_obstructed** : A 12 element sequence. Wedges
    presumably correlate with the ones in *wedges_fraction_obstructed*, but
    the exact relationship is unknown. The numbers in this one are generally
    higher and may represent fraction of the wedge, in which case max value
    for each element should be 1.
    **OBSOLETE**: The user terminal no longer provides this data.
: **valid_s** : It is unclear what this field means exactly, but it appears to
    be a measure of how complete the data is that the user terminal uses to
    determine obstruction locations.

See also *fraction_obstructed* in general status data, which should equal the
sum of all *wedges_fraction_obstructed* elements.

Alert detail status data
------------------------
This group holds the current state of each individual alert reported by the
user terminal. Note that more alerts may be added in the future. See also
*alerts* in the general status data for a bit field combining them if you
need a set of fields that will not change size in the future.

Descriptions on these are vague due to them being difficult to confirm by
their nature, but the field names are pretty self-explanatory.

: **alert_motors_stuck** : Alert corresponding with bit 0 (bit mask 1) in
    *alerts*.
: **alert_thermal_shutdown** : Alert corresponding with bit 1 (bit mask 2) in
    *alerts*.
: **alert_thermal_throttle** : Alert corresponding with bit 2 (bit mask 4) in
    *alerts*.
: **alert_unexpected_location** : Alert corresponding with bit 3 (bit mask 8)
    in *alerts*.
: **alert_mast_not_near_vertical** : Alert corresponding with bit 4 (bit mask
    16) in *alerts*.
: **alert_slow_ethernet_speeds** : Alert corresponding with bit 5 (bit mask
    32) in *alerts*.
: **alert_roaming** : Alert corresponding with bit 6 (bit mask 64) in *alerts*.
: **alert_install_pending** : Alert corresponding with bit 7 (bit mask 128) in
    *alerts*.
: **alert_is_heating** : Alert corresponding with bit 8 (bit mask 256) in
    *alerts*.
: **alert_power_supply_thermal_throttle** : Alert corresponding with bit 9 (bit
    mask 512) in *alerts*.
: **alert_is_power_save_idle** : Alert corresponding with bit 10 (bit mask
    1024) in *alerts*.
: **alert_moving_while_not_mobile** : Alert corresponding with bit 11 (bit mask
    2048) in *alerts*.
: **alert_moving_fast_while_not_aviation** : Alert corresponding with bit 12
    (bit mask 4096) in *alerts*.

Location data
-------------
This group holds information about the physical location of the user terminal.

This group of fields should be considered EXPERIMENTAL, due to the requirement
to authorize access to location data on the user terminal.

: **latitude** : Latitude part of the current location, in degrees, or None if
    location data is not available.
: **longitude** : Longitude part of the current location, in degrees, or None if
    location data is not available.
: **altitude** : Altitude part of the current location, (probably) in meters, or
    None if location data is not available.

General history data
--------------------
This set of fields contains data relevant to all the other history groups.

The sample interval is currently 1 second.

: **samples** : The number of samples analyzed (for statistics) or returned
    (for bulk data).
: **end_counter** : The total number of data samples that have been written to
    the history buffer since reboot of the user terminal, irrespective of
    buffer wrap.  This can be used to keep track of how many samples are new
    in comparison to a prior query of the history data.

Bulk history data
-----------------
This group holds the history data as-is for the requested range of
samples, just unwound from the circular buffers that the raw data holds.
It contains some of the same fields as the status info, but instead of
representing the current values, each field contains a sequence of values
representing the value over time, ending at the current time.

: **pop_ping_drop_rate** : Fraction of lost ping replies per sample.
: **pop_ping_latency_ms** : Round trip time, in milliseconds, during the
    sample period, or None if a sample experienced 100% ping drop.
: **downlink_throughput_bps** : Download usage during the sample period
    (actual, not max available), in bits per second.
: **uplink_throughput_bps** : Upload usage during the sample period, in bits
    per second.
: **snr** : Signal to noise ratio during the sample period.
    **OBSOLETE**: The user terminal no longer provides this data.
: **scheduled** : Boolean indicating whether or not a satellite was scheduled
    to be available for transmit/receive during the sample period.  When
    false, ping drop shows as "No satellites" in Starlink app.
    **OBSOLETE**: The user terminal no longer provides this data.
: **obstructed** : Boolean indicating whether or not the user terminal
    determined the signal between it and the satellite was obstructed during
    the sample period. When true, ping drop shows as "Obstructed" in the
    Starlink app.
    **OBSOLETE**: The user terminal no longer provides this data.

There is no specific data field in the raw history data that directly
correlates with "Other" or "Beta downtime" in the Starlink app (or whatever it
gets renamed to after beta), but empirical evidence suggests any sample where
*pop_ping_drop_rate* is 1, *scheduled* is true, and *obstructed* is false is
counted as "Beta downtime".

Note that neither *scheduled*=false nor *obstructed*=true necessarily means
packet loss occurred. Those need to be examined in combination with
*pop_ping_drop_rate* to be meaningful.

General ping drop history statistics
------------------------------------
This group of statistics characterize the packet loss (labeled "ping drop" in
the field names of the Starlink gRPC service protocol) in various ways.

: **total_ping_drop** : The total amount of time, in sample intervals, that
    experienced ping drop.
: **count_full_ping_drop** : The number of samples that experienced 100% ping
    drop.
: **count_obstructed** : The number of samples that were marked as
    "obstructed", regardless of whether they experienced any ping
    drop.
    **OBSOLETE**: The user terminal no longer provides the data from which
    this was calculated.
: **total_obstructed_ping_drop** : The total amount of time, in sample
    intervals, that experienced ping drop in samples marked as "obstructed".
    **OBSOLETE**: The user terminal no longer provides the data from which
    this was calculated.
: **count_full_obstructed_ping_drop** : The number of samples that were marked
    as "obstructed" and that experienced 100% ping drop.
    **OBSOLETE**: The user terminal no longer provides the data from which
    this was calculated.
: **count_unscheduled** : The number of samples that were not marked as
    "scheduled", regardless of whether they experienced any ping drop.
    **OBSOLETE**: The user terminal no longer provides the data from which
    this was calculated.
: **total_unscheduled_ping_drop** : The total amount of time, in sample
    intervals, that experienced ping drop in samples not marked as
    "scheduled".
    **OBSOLETE**: The user terminal no longer provides the data from which
    this was calculated.
: **count_full_unscheduled_ping_drop** : The number of samples that were not
    marked as "scheduled" and that experienced 100% ping drop.
    **OBSOLETE**: The user terminal no longer provides the data from which
    this was calculated.

Total packet loss ratio can be computed with *total_ping_drop* / *samples*.

Ping drop run length history statistics
---------------------------------------
This group of statistics characterizes packet loss by how long a
consecutive run of 100% packet loss lasts.

: **init_run_fragment** : The number of consecutive sample periods at the
    start of the sample set that experienced 100% ping drop. This period may
    be a continuation of a run that started prior to the sample set, so is not
    counted in the following stats.
: **final_run_fragment** : The number of consecutive sample periods at the end
    of the sample set that experienced 100% ping drop. This period may
    continue as a run beyond the end of the sample set, so is not counted in
    the following stats.
: **run_seconds** : A 60 element sequence. Each element records the total
    amount of time, in sample intervals, that experienced 100% ping drop in a
    consecutive run that lasted for (index + 1) sample intervals (seconds).
    That is, the first element contains time spent in 1 sample runs, the
    second element contains time spent in 2 sample runs, etc.
: **run_minutes** : A 60 element sequence. Each element records the total
    amount of time, in sample intervals, that experienced 100% ping drop in a
    consecutive run that lasted for more that (index + 1) multiples of 60
    sample intervals (minutes), but less than or equal to (index + 2)
    multiples of 60 sample intervals. Except for the last element in the
    sequence, which records the total amount of time in runs of more than
    60*60 samples.

No sample should be counted in more than one of the run length stats or stat
elements, so the total of all of them should be equal to
*count_full_ping_drop* from the ping drop stats.

Samples that experience less than 100% ping drop are not counted in this group
of stats, even if they happen at the beginning or end of a run of 100% ping
drop samples. To compute the amount of time that experienced ping loss in less
than a single run of 100% ping drop, use (*total_ping_drop* -
*count_full_ping_drop*) from the ping drop stats.

Ping latency history statistics
-------------------------------
This group of statistics characterizes latency of ping request/response in
various ways. For all non-sequence fields and most sequence elements, the
value may report as None to indicate no matching samples. The exception is
*load_bucket_samples* elements, which report 0 for no matching samples.

The fields that have "all" in their name are computed across all samples that
had any ping success (ping drop < 1). The fields that have "full" in their
name are computed across only the samples that have 100% ping success (ping
drop = 0). Which one is more interesting may depend on intended use. High rate
of packet loss appears to cause outlier latency values on the high side. On
the one hand, those are real cases, so should not be dismissed lightly. On the
other hand, the "full" numbers are more directly comparable to sample sets
taken over time.

: **mean_all_ping_latency** : Weighted mean latency value, in milliseconds, of
    all samples that experienced less than 100% ping drop. Values are weighted
    by amount of ping success (1 - ping drop).
: **deciles_all_ping_latency** : An 11 element sequence recording the weighted
    deciles (10-quantiles) of latency values, in milliseconds, for all samples
    that experienced less that 100% ping drop, including the minimum and
    maximum values as the 0th and 10th deciles respectively. The 5th decile
    (at sequence index 5) is the weighted median latency value.
: **mean_full_ping_latency** : Mean latency value, in milliseconds, of samples
    that experienced no ping drop.
: **deciles_full_ping_latency** : An 11 element sequence recording the deciles
    (10-quantiles) of latency values, in milliseconds, for all samples that
    experienced no ping drop, including the minimum and maximum values as the
    0th and 10th deciles respectively. The 5th decile (at sequence index 5) is
    the median latency value.
: **stdev_full_ping_latency** : Population standard deviation of the latency
    value of samples that experienced no ping drop.

Loaded ping latency statistics
------------------------------
This group of statistics attempts to characterize latency of ping
request/response under various network load conditions. Samples are grouped by
total (down+up) bandwidth used during the sample period, using a log base 2
scale. These groups are referred to as "load buckets" below. The first bucket
in each sequence represents samples that use less than 1Mbps (millions of bits
per second). Subsequent buckets use more bandwidth than that covered by prior
buckets, but less than twice the maximum bandwidth of the immediately prior
bucket. The last bucket, at sequence index 14, represents all samples not
covered by a prior bucket, which works out to any sample using 8192Mbps or
greater. Only samples that experience no ping drop are included in any of the
buckets.

This group of fields should be considered EXPERIMENTAL and thus subject to
change without regard to backward compatibility.

Note that in all cases, the latency values are of "ping" traffic, which may be
prioritized lower than other traffic by various network layers. How much
bandwidth constitutes a fully loaded network connection may vary over time.
Buckets with few samples may not contain statistically significant latency
data.

: **load_bucket_samples** : A 15 element sequence recording the number of
    samples per load bucket. See above for load bucket partitioning.
    EXPERIMENTAL.
: **load_bucket_min_latency** : A 15 element sequence recording the minimum
    latency value, in milliseconds, per load bucket. EXPERIMENTAL.
: **load_bucket_median_latency** : A 15 element sequence recording the median
    latency value, in milliseconds, per load bucket. EXPERIMENTAL.
: **load_bucket_max_latency** : A 15 element sequence recording the maximum
    latency value, in milliseconds, per load bucket. EXPERIMENTAL.

Bandwidth usage history statistics
----------------------------------
This group of statistics characterizes total bandwidth usage over the sample
period.

: **download_usage** : Total number of bytes downloaded to the user terminal
    during the sample period.
: **upload_usage** : Total number of bytes uploaded from the user terminal
    during the sample period.
"""

from itertools import chain
import math
import statistics
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, get_type_hints
from typing_extensions import TypedDict, get_args

import grpc

try:
    from yagrc import importer
    importer.add_lazy_packages(["spacex.api.device"])
    imports_pending = True
except (ImportError, AttributeError):
    imports_pending = False

from spacex.api.device import device_pb2
from spacex.api.device import device_pb2_grpc
from spacex.api.device import dish_pb2

# Max wait time for gRPC request completion, in seconds. This is just to
# prevent hang if the connection goes dead without closing.
REQUEST_TIMEOUT = 10

HISTORY_FIELDS = ("pop_ping_drop_rate", "pop_ping_latency_ms", "downlink_throughput_bps",
                  "uplink_throughput_bps")

StatusDict = TypedDict(
    "StatusDict", {
        "id": str,
        "hardware_version": str,
        "software_version": str,
        "state": str,
        "uptime": int,
        "snr": Optional[float],
        "seconds_to_first_nonempty_slot": float,
        "pop_ping_drop_rate": float,
        "downlink_throughput_bps": float,
        "uplink_throughput_bps": float,
        "pop_ping_latency_ms": float,
        "alerts": int,
        "fraction_obstructed": float,
        "currently_obstructed": bool,
        "seconds_obstructed": Optional[float],
        "obstruction_duration": Optional[float],
        "obstruction_interval": Optional[float],
        "direction_azimuth": float,
        "direction_elevation": float,
        "is_snr_above_noise_floor": bool,
    })

ObstructionDict = TypedDict(
    "ObstructionDict", {
        "wedges_fraction_obstructed[]": Sequence[Optional[float]],
        "raw_wedges_fraction_obstructed[]": Sequence[Optional[float]],
        "valid_s": float,
    })

AlertDict = Dict[str, bool]

LocationDict = TypedDict("LocationDict", {
    "latitude": Optional[float],
    "longitude": Optional[float],
    "altitude": Optional[float],
})

HistGeneralDict = TypedDict("HistGeneralDict", {
    "samples": int,
    "end_counter": int,
})

HistBulkDict = TypedDict(
    "HistBulkDict", {
        "pop_ping_drop_rate": Sequence[float],
        "pop_ping_latency_ms": Sequence[Optional[float]],
        "downlink_throughput_bps": Sequence[float],
        "uplink_throughput_bps": Sequence[float],
        "snr": Sequence[Optional[float]],
        "scheduled": Sequence[Optional[bool]],
        "obstructed": Sequence[Optional[bool]],
    })

PingDropDict = TypedDict(
    "PingDropDict", {
        "total_ping_drop": float,
        "count_full_ping_drop": int,
        "count_obstructed": int,
        "total_obstructed_ping_drop": float,
        "count_full_obstructed_ping_drop": int,
        "count_unscheduled": int,
        "total_unscheduled_ping_drop": float,
        "count_full_unscheduled_ping_drop": int,
    })

PingDropRlDict = TypedDict(
    "PingDropRlDict", {
        "init_run_fragment": int,
        "final_run_fragment": int,
        "run_seconds[1,]": Sequence[int],
        "run_minutes[1,]": Sequence[int],
    })

PingLatencyDict = TypedDict(
    "PingLatencyDict", {
        "mean_all_ping_latency": float,
        "deciles_all_ping_latency[]": Sequence[float],
        "mean_full_ping_latency": float,
        "deciles_full_ping_latency[]": Sequence[float],
        "stdev_full_ping_latency": Optional[float],
    })

LoadedLatencyDict = TypedDict(
    "LoadedLatencyDict", {
        "load_bucket_samples[]": Sequence[int],
        "load_bucket_min_latency[]": Sequence[Optional[float]],
        "load_bucket_median_latency[]": Sequence[Optional[float]],
        "load_bucket_max_latency[]": Sequence[Optional[float]],
    })

UsageDict = TypedDict("UsageDict", {
    "download_usage": int,
    "upload_usage": int,
})

# For legacy reasons, there is a slight difference between the field names
# returned in the actual data vs the *_field_names functions. This is a map of
# the differences. Bulk data fields are handled separately because the field
# "snr" overlaps with a status field and needs to map differently.
_FIELD_NAME_MAP = {
    "wedges_fraction_obstructed[]": "wedges_fraction_obstructed[12]",
    "raw_wedges_fraction_obstructed[]": "raw_wedges_fraction_obstructed[12]",
    "run_seconds[1,]": "run_seconds[1,61]",
    "run_minutes[1,]": "run_minutes[1,61]",
    "deciles_all_ping_latency[]": "deciles_all_ping_latency[11]",
    "deciles_full_ping_latency[]": "deciles_full_ping_latency[11]",
    "load_bucket_samples[]": "load_bucket_samples[15]",
    "load_bucket_min_latency[]": "load_bucket_min_latency[15]",
    "load_bucket_median_latency[]": "load_bucket_median_latency[15]",
    "load_bucket_max_latency[]": "load_bucket_max_latency[15]",
}


def _field_names(hint_type):
    return list(_FIELD_NAME_MAP.get(key, key) for key in get_type_hints(hint_type))


def _field_names_bulk(hint_type):
    return list(key + "[]" for key in get_type_hints(hint_type))


def _field_types(hint_type):
    def xlate(value):
        while not isinstance(value, type):
            args = get_args(value)
            value = args[0] if args[0] is not type(None) else args[1]
        return value

    return list(xlate(val) for val in get_type_hints(hint_type).values())


def resolve_imports(channel: grpc.Channel):
    importer.resolve_lazy_imports(channel)
    global imports_pending
    imports_pending = False


class GrpcError(Exception):
    """Provides error info when something went wrong with a gRPC call."""
    def __init__(self, e, *args, **kwargs):
        # grpc.RpcError is too verbose to print in whole, but it may also be
        # a Call object, and that class has some minimally useful info.
        if isinstance(e, grpc.Call):
            msg = e.details()
        elif isinstance(e, grpc.RpcError):
            msg = "Unknown communication or service error"
        elif isinstance(e, (AttributeError, IndexError, TypeError, ValueError)):
            msg = "Protocol error"
        else:
            msg = str(e)
        super().__init__(msg, *args, **kwargs)


class UnwrappedHistory:
    """Class for holding a copy of grpc history data."""

    unwrapped: bool


class ChannelContext:
    """A wrapper for reusing an open grpc Channel across calls.

    `close()` should be called on the object when it is no longer
    in use.
    """
    def __init__(self, target: Optional[str] = None) -> None:
        self.channel = None
        self.target = "192.168.100.1:9200" if target is None else target

    def get_channel(self) -> Tuple[grpc.Channel, bool]:
        reused = True
        if self.channel is None:
            self.channel = grpc.insecure_channel(self.target)
            reused = False
        return self.channel, reused

    def close(self) -> None:
        if self.channel is not None:
            self.channel.close()
        self.channel = None


def call_with_channel(function, *args, context: Optional[ChannelContext] = None, **kwargs):
    """Call a function with a channel object.

    Args:
        function: Function to call with channel as first arg.
        args: Additional args to pass to function
        context (ChannelContext): Optionally provide a channel for (re)use.
            If not set, a new default channel will be used and then closed.
        kwargs: Additional keyword args to pass to function.
    """
    if context is None:
        with grpc.insecure_channel("192.168.100.1:9200") as channel:
            return function(channel, *args, **kwargs)

    while True:
        channel, reused = context.get_channel()
        try:
            return function(channel, *args, **kwargs)
        except grpc.RpcError:
            context.close()
            if not reused:
                raise


def status_field_names(context: Optional[ChannelContext] = None):
    """Return the field names of the status data.

    Note:
        See module level docs regarding brackets in field names.

    Args:
        context (ChannelContext): Optionally provide a channel for (re)use
            with reflection service.

    Returns:
        A tuple with 3 lists, with status data field names, obstruction detail
        field names, and alert detail field names, in that order.

    Raises:
        GrpcError: No user terminal is currently available to resolve imports
            via reflection.
    """
    if imports_pending:
        try:
            call_with_channel(resolve_imports, context=context)
        except grpc.RpcError as e:
            raise GrpcError(e) from e
    alert_names = []
    try:
        for field in dish_pb2.DishAlerts.DESCRIPTOR.fields:
            alert_names.append("alert_" + field.name)
    except AttributeError:
        pass

    return _field_names(StatusDict), _field_names(ObstructionDict), alert_names


def status_field_types(context: Optional[ChannelContext] = None):
    """Return the field types of the status data.

    Return the type classes for each field. For sequence types, the type of
    element in the sequence is returned, not the type of the sequence.

    Args:
        context (ChannelContext): Optionally provide a channel for (re)use
            with reflection service.

    Returns:
        A tuple with 3 lists, with status data field types, obstruction detail
        field types, and alert detail field types, in that order.

    Raises:
        GrpcError: No user terminal is currently available to resolve imports
            via reflection.
    """
    if imports_pending:
        try:
            call_with_channel(resolve_imports, context=context)
        except grpc.RpcError as e:
            raise GrpcError(e) from e
    num_alerts = 0
    try:
        num_alerts = len(dish_pb2.DishAlerts.DESCRIPTOR.fields)
    except AttributeError:
        pass
    return (_field_types(StatusDict), _field_types(ObstructionDict), [bool] * num_alerts)


def get_status(context: Optional[ChannelContext] = None):
    """Fetch status data and return it in grpc structure format.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        grpc.RpcError: Communication or service error.
        AttributeError, ValueError: Protocol error. Either the target is not a
            Starlink user terminal or the grpc protocol has changed in a way
            this module cannot handle.
    """
    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(get_status={}), timeout=REQUEST_TIMEOUT)
        return response.dish_get_status

    return call_with_channel(grpc_call, context=context)


def get_id(context: Optional[ChannelContext] = None) -> str:
    """Return the ID from the dish status information.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls.

    Returns:
        A string identifying the Starlink user terminal reachable from the
        local network.

    Raises:
        GrpcError: No user terminal is currently reachable.
    """
    try:
        status = get_status(context)
        return status.device_info.id
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e


def status_data(
        context: Optional[ChannelContext] = None) -> Tuple[StatusDict, ObstructionDict, AlertDict]:
    """Fetch current status data.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls.

    Returns:
        A tuple with 3 dicts, mapping status data field names, obstruction
        detail field names, and alert detail field names to their respective
        values, in that order.

    Raises:
        GrpcError: Failed getting status info from the Starlink user terminal.
    """
    try:
        status = get_status(context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e

    try:
        if status.HasField("outage"):
            if status.outage.cause == dish_pb2.DishOutage.Cause.NO_SCHEDULE:
                # Special case translate this to equivalent old name
                state = "SEARCHING"
            else:
                try:
                    state = dish_pb2.DishOutage.Cause.Name(status.outage.cause)
                except ValueError:
                    # Unlikely, but possible if dish is running newer firmware
                    # than protocol data pulled via reflection
                    state = str(status.outage.cause)
        else:
            state = "CONNECTED"
    except (AttributeError, ValueError):
        state = "UNKNOWN"

    # More alerts may be added in future, so in addition to listing them
    # individually, provide a bit field based on field numbers of the
    # DishAlerts message.
    alerts = {}
    alert_bits = 0
    try:
        for field in status.alerts.DESCRIPTOR.fields:
            value = getattr(status.alerts, field.name, False)
            alerts["alert_" + field.name] = value
            if field.number < 65:
                alert_bits |= (1 if value else 0) << (field.number - 1)
    except AttributeError:
        pass

    obstruction_duration = None
    obstruction_interval = None
    obstruction_stats = getattr(status, "obstruction_stats", None)
    if obstruction_stats is not None:
        try:
            if (obstruction_stats.avg_prolonged_obstruction_duration_s > 0.0
                    and not math.isnan(obstruction_stats.avg_prolonged_obstruction_interval_s)):
                obstruction_duration = obstruction_stats.avg_prolonged_obstruction_duration_s
                obstruction_interval = obstruction_stats.avg_prolonged_obstruction_interval_s
        except AttributeError:
            pass

    device_info = getattr(status, "device_info", None)
    return {
        "id": getattr(device_info, "id", None),
        "hardware_version": getattr(device_info, "hardware_version", None),
        "software_version": getattr(device_info, "software_version", None),
        "state": state,
        "uptime": getattr(getattr(status, "device_state", None), "uptime_s", None),
        "snr": None,  # obsoleted in grpc service
        "seconds_to_first_nonempty_slot": getattr(status, "seconds_to_first_nonempty_slot", None),
        "pop_ping_drop_rate": getattr(status, "pop_ping_drop_rate", None),
        "downlink_throughput_bps": getattr(status, "downlink_throughput_bps", None),
        "uplink_throughput_bps": getattr(status, "uplink_throughput_bps", None),
        "pop_ping_latency_ms": getattr(status, "pop_ping_latency_ms", None),
        "alerts": alert_bits,
        "fraction_obstructed": getattr(obstruction_stats, "fraction_obstructed", None),
        "currently_obstructed": getattr(obstruction_stats, "currently_obstructed", None),
        "seconds_obstructed": None,  # obsoleted in grpc service
        "obstruction_duration": obstruction_duration,
        "obstruction_interval": obstruction_interval,
        "direction_azimuth": getattr(status, "boresight_azimuth_deg", None),
        "direction_elevation": getattr(status, "boresight_elevation_deg", None),
        "is_snr_above_noise_floor": getattr(status, "is_snr_above_noise_floor", None),
    }, {
        "wedges_fraction_obstructed[]": [None] * 12,  # obsoleted in grpc service
        "raw_wedges_fraction_obstructed[]": [None] * 12,  # obsoleted in grpc service
        "valid_s": getattr(obstruction_stats, "valid_s", None),
    }, alerts


def location_field_names():
    """Return the field names of the location data.

    Returns:
        A list with location data field names.
    """
    return _field_names(LocationDict)


def location_field_types():
    """Return the field types of the location data.

    Return the type classes for each field.

    Returns:
        A list with location data field types.
    """
    return _field_types(LocationDict)


def get_location(context: Optional[ChannelContext] = None):
    """Fetch location data and return it in grpc structure format.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        grpc.RpcError: Communication or service error.
        AttributeError, ValueError: Protocol error. Either the target is not a
            Starlink user terminal or the grpc protocol has changed in a way
            this module cannot handle.
    """
    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(get_location={}), timeout=REQUEST_TIMEOUT)
        return response.get_location

    return call_with_channel(grpc_call, context=context)


def location_data(context: Optional[ChannelContext] = None) -> LocationDict:
    """Fetch current location data.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls.

    Returns:
        A dict mapping location data field names to their values. Values will
        be set to None in the case that location request is not enabled (ie:
        not authorized).

    Raises:
        GrpcError: Failed getting location info from the Starlink user terminal.
    """
    try:
        location = get_location(context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        if isinstance(e, grpc.Call) and e.code() is grpc.StatusCode.PERMISSION_DENIED:
            return {
                "latitude": None,
                "longitude": None,
                "altitude": None,
            }
        raise GrpcError(e) from e

    try:
        return {
            "latitude": location.lla.lat,
            "longitude": location.lla.lon,
            "altitude": getattr(location.lla, "alt", None),
        }
    except AttributeError as e:
        # Allow None for altitude, but since all None values has special
        # meaning for this function, any other protocol change is flagged as
        # an error.
        raise GrpcError(e) from e


def history_bulk_field_names():
    """Return the field names of the bulk history data.

    Note:
        See module level docs regarding brackets in field names.

    Returns:
        A tuple with 2 lists, the first with general data names, the second
        with bulk history data names.
    """
    return _field_names(HistGeneralDict), _field_names_bulk(HistBulkDict)


def history_bulk_field_types():
    """Return the field types of the bulk history data.

    Return the type classes for each field. For sequence types, the type of
    element in the sequence is returned, not the type of the sequence.

    Returns:
        A tuple with 2 lists, the first with general data types, the second
        with bulk history data types.
    """
    return _field_types(HistGeneralDict), _field_types(HistBulkDict)


def history_ping_field_names():
    """Deprecated. Use history_stats_field_names instead."""
    return history_stats_field_names()[0:3]


def history_stats_field_names():
    """Return the field names of the packet loss stats.

    Note:
        See module level docs regarding brackets in field names.

    Returns:
        A tuple with 6 lists, with general data names, ping drop stat names,
        ping drop run length stat names, ping latency stat names, loaded ping
        latency stat names, and bandwidth usage stat names, in that order.

        Note:
            Additional lists may be added to this tuple in the future with
            additional data groups, so it not recommended for the caller to
            assume exactly 6 elements.
    """
    return (_field_names(HistGeneralDict), _field_names(PingDropDict), _field_names(PingDropRlDict),
            _field_names(PingLatencyDict), _field_names(LoadedLatencyDict), _field_names(UsageDict))


def history_stats_field_types():
    """Return the field types of the packet loss stats.

    Return the type classes for each field. For sequence types, the type of
    element in the sequence is returned, not the type of the sequence.

    Returns:
        A tuple with 6 lists, with general data types, ping drop stat types,
        ping drop run length stat types, ping latency stat types, loaded ping
        latency stat types, and bandwidth usage stat types, in that order.

        Note:
            Additional lists may be added to this tuple in the future with
            additional data groups, so it not recommended for the caller to
            assume exactly 6 elements.
    """
    return (_field_types(HistGeneralDict), _field_types(PingDropDict), _field_types(PingDropRlDict),
            _field_types(PingLatencyDict), _field_types(LoadedLatencyDict), _field_types(UsageDict))


def get_history(context: Optional[ChannelContext] = None):
    """Fetch history data and return it in grpc structure format.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        grpc.RpcError: Communication or service error.
        AttributeError, ValueError: Protocol error. Either the target is not a
            Starlink user terminal or the grpc protocol has changed in a way
            this module cannot handle.
    """
    def grpc_call(channel: grpc.Channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(get_history={}), timeout=REQUEST_TIMEOUT)
        return response.dish_get_history

    return call_with_channel(grpc_call, context=context)


def _compute_sample_range(history,
                          parse_samples: int,
                          start: Optional[int] = None,
                          verbose: bool = False):
    try:
        current = int(history.current)
        samples = len(history.pop_ping_drop_rate)
    except (AttributeError, TypeError):
        # Without current and pop_ping_drop_rate, history is unusable.
        return range(0), 0, None

    if verbose:
        print("current counter:       " + str(current))
        print("All samples:           " + str(samples))

    if not hasattr(history, "unwrapped"):
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

    if start == current:
        return range(0), 0, current

    # Not a ring buffer is simple case.
    if hasattr(history, "unwrapped"):
        return range(samples - (current-start), samples), current - start, current

    # This is ring buffer offset, so both index to oldest data sample and
    # index to next data sample after the newest one.
    end_offset = current % samples
    start_offset = start % samples

    # Set the range for the requested set of samples. This will iterate
    # sample index in order from oldest to newest.
    sample_range: Iterable[int]
    if start_offset < end_offset:
        sample_range = range(start_offset, end_offset)
    else:
        sample_range = chain(range(start_offset, samples), range(0, end_offset))

    return sample_range, current - start, current


def concatenate_history(history1,
                        history2,
                        samples1: int = -1,
                        start1: Optional[int] = None,
                        verbose: bool = False):
    """Append the sample-dependent fields of one history object to another.

    Note:
        Samples data will be appended regardless of dish reboot or history
        data ring buffer wrap, which may result in discontiguous sample data
        with lost data.

    Args:
        history1: The grpc history object, such as one returned by a prior
            call to `get_history`, or object with similar attributes, to which
            to append.
        history2: The grpc history object, such as one returned by a prior
            call to `get_history`, from which to append.
        samples1 (int): Optional number of samples to process, or -1 to parse
            all available samples (bounded by start1, if it is set).
        start1 (int): Optional starting counter value to be applied to the
            history1 data. See `history_bulk_data` documentation for more
            details on how this parameter is used.
        verbose (bool): Optionally produce verbose output.

    Returns:
        An object with the unwrapped history data and the same attribute
        fields as a grpc history object.
    """
    try:
        size2 = len(history2.pop_ping_drop_rate)
        new_samples = history2.current - history1.current
    except (AttributeError, TypeError):
        # Something is wrong. Probably both history objects are bad, so no
        # point in trying to combine them.
        return history1

    if new_samples < 0:
        if verbose:
            print("Dish reboot detected. Appending anyway.")
        new_samples = history2.current if history2.current < size2 else size2
    elif new_samples > size2:
        # This should probably go to stderr and not depend on verbose flag,
        # but this layer of the code tries not to make that sort of logging
        # policy decision, so honor requested verbosity.
        if verbose:
            print("WARNING: Appending discontiguous samples. Polling interval probably too short.")
        new_samples = size2

    unwrapped = UnwrappedHistory()
    for field in HISTORY_FIELDS:
        if hasattr(history1, field) and hasattr(history2, field):
            setattr(unwrapped, field, [])
    unwrapped.unwrapped = True

    sample_range, ignore1, ignore2 = _compute_sample_range(  # pylint: disable=unused-variable
        history1, samples1, start=start1)
    for i in sample_range:
        for field in HISTORY_FIELDS:
            if hasattr(unwrapped, field):
                try:
                    getattr(unwrapped, field).append(getattr(history1, field)[i])
                except (IndexError, TypeError):
                    pass

    sample_range, ignore1, ignore2 = _compute_sample_range(history2, new_samples)  # pylint: disable=unused-variable
    for i in sample_range:
        for field in HISTORY_FIELDS:
            if hasattr(unwrapped, field):
                try:
                    getattr(unwrapped, field).append(getattr(history2, field)[i])
                except (IndexError, TypeError):
                    pass

    unwrapped.current = history2.current
    return unwrapped


def history_bulk_data(parse_samples: int,
                      start: Optional[int] = None,
                      verbose: bool = False,
                      context: Optional[ChannelContext] = None,
                      history=None) -> Tuple[HistGeneralDict, HistBulkDict]:
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

            Note: The sample counter will reset to 0 when the dish reboots. If
            the requested start value is greater than the new "end_counter"
            value, this function will assume that happened and treat all
            samples as being later than the requested start, and thus include
            them (bounded by parse_samples, if it is not -1).
        verbose (bool): Optionally produce verbose output.
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls.
        history: Optionally provide the history data to use instead of fetching
            it, from a prior call to `get_history`.

    Returns:
        A tuple with 2 dicts, the first mapping general data names to their
        values and the second mapping bulk history data names to their values.

        Note: The field names in the returned data do _not_ include brackets
            to indicate sequences, since those would just need to be parsed
            out.  The general data is all single items and the bulk history
            data is all sequences.

    Raises:
        GrpcError: Failed getting history info from the Starlink user
            terminal.
    """
    if history is None:
        try:
            history = get_history(context)
        except (AttributeError, ValueError, grpc.RpcError) as e:
            raise GrpcError(e) from e

    sample_range, parsed_samples, current = _compute_sample_range(history,
                                                                  parse_samples,
                                                                  start=start,
                                                                  verbose=verbose)

    pop_ping_drop_rate = []
    pop_ping_latency_ms = []
    downlink_throughput_bps = []
    uplink_throughput_bps = []

    for i in sample_range:
        # pop_ping_drop_rate is checked in _compute_sample_range
        pop_ping_drop_rate.append(history.pop_ping_drop_rate[i])

        latency = None
        try:
            if history.pop_ping_drop_rate[i] < 1:
                latency = history.pop_ping_latency_ms[i]
        except (AttributeError, IndexError, TypeError):
            pass
        pop_ping_latency_ms.append(latency)

        downlink = None
        try:
            downlink = history.downlink_throughput_bps[i]
        except (AttributeError, IndexError, TypeError):
            pass
        downlink_throughput_bps.append(downlink)

        uplink = None
        try:
            uplink = history.uplink_throughput_bps[i]
        except (AttributeError, IndexError, TypeError):
            pass
        uplink_throughput_bps.append(uplink)

    return {
        "samples": parsed_samples,
        "end_counter": current,
    }, {
        "pop_ping_drop_rate": pop_ping_drop_rate,
        "pop_ping_latency_ms": pop_ping_latency_ms,
        "downlink_throughput_bps": downlink_throughput_bps,
        "uplink_throughput_bps": uplink_throughput_bps,
        "snr": [None] * parsed_samples,  # obsoleted in grpc service
        "scheduled": [None] * parsed_samples,  # obsoleted in grpc service
        "obstructed": [None] * parsed_samples,  # obsoleted in grpc service
    }


def history_ping_stats(parse_samples: int,
                       verbose: bool = False,
                       context: Optional[ChannelContext] = None
                       ) -> Tuple[HistGeneralDict, PingDropDict, PingDropRlDict]:
    """Deprecated. Use history_stats instead."""
    return history_stats(parse_samples, verbose=verbose, context=context)[0:3]


def history_stats(
    parse_samples: int,
    start: Optional[int] = None,
    verbose: bool = False,
    context: Optional[ChannelContext] = None,
    history=None
) -> Tuple[HistGeneralDict, PingDropDict, PingDropRlDict, PingLatencyDict, LoadedLatencyDict,
           UsageDict]:
    """Fetch, parse, and compute ping and usage stats.

    Note:
        See module level docs regarding brackets in field names.

    Args:
        parse_samples (int): Number of samples to process, or -1 to parse all
            available samples.
        start (int): Optional starting counter value to be applied to the
            history data. See `history_bulk_data` documentation for more
            details on how this parameter is used.
        verbose (bool): Optionally produce verbose output.
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls.
        history: Optionally provide the history data to use instead of fetching
            it, from a prior call to `get_history`.

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
        GrpcError: Failed getting history info from the Starlink user
            terminal.
    """
    if history is None:
        try:
            history = get_history(context)
        except (AttributeError, ValueError, grpc.RpcError) as e:
            raise GrpcError(e) from e

    sample_range, parsed_samples, current = _compute_sample_range(history,
                                                                  parse_samples,
                                                                  start=start,
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

    rtt_full: List[float] = []
    rtt_all: List[Tuple[float, float]] = []
    rtt_buckets: List[List[float]] = [[] for _ in range(15)]

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
        tot += d

        down = 0.0
        try:
            down = history.downlink_throughput_bps[i]
        except (AttributeError, IndexError, TypeError):
            pass
        usage_down += down

        up = 0.0
        try:
            up = history.uplink_throughput_bps[i]
        except (AttributeError, IndexError, TypeError):
            pass
        usage_up += up

        rtt = 0.0
        try:
            rtt = history.pop_ping_latency_ms[i]
        except (AttributeError, IndexError, TypeError):
            pass
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

    bucket_samples: List[int] = []
    bucket_min: List[Optional[float]] = []
    bucket_median: List[Optional[float]] = []
    bucket_max: List[Optional[float]] = []
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


def get_obstruction_map(context: Optional[ChannelContext] = None):
    """Fetch obstruction map data and return it in grpc structure format.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        grpc.RpcError: Communication or service error.
        AttributeError, ValueError: Protocol error. Either the target is not a
            Starlink user terminal or the grpc protocol has changed in a way
            this module cannot handle.
    """
    def grpc_call(channel: grpc.Channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(dish_get_obstruction_map={}),
                               timeout=REQUEST_TIMEOUT)
        return response.dish_get_obstruction_map

    return call_with_channel(grpc_call, context=context)


def obstruction_map(context: Optional[ChannelContext] = None):
    """Fetch current obstruction map data.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls.

    Returns:
        A tuple of row data, each of which is a tuple of column data, which
        hold floats indicating SNR info per direction in the range of 0.0 to
        1.0 for valid data and -1.0 for invalid data. To get a flat
        representation the SNR data instead, see `get_obstruction_map`.

    Raises:
        GrpcError: Failed getting obstruction data from the Starlink user
            terminal.
    """
    try:
        map_data = get_obstruction_map(context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e

    try:
        cols = map_data.num_cols
        return tuple((map_data.snr[i:i + cols]) for i in range(0, cols * map_data.num_rows, cols))
    except (AttributeError, IndexError, TypeError) as e:
        raise GrpcError(e) from e


def reboot(context: Optional[ChannelContext] = None) -> None:
    """Request dish reboot operation.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        GrpcError: Communication or service error.
    """
    def grpc_call(channel: grpc.Channel) -> None:
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        stub.Handle(device_pb2.Request(reboot={}), timeout=REQUEST_TIMEOUT)
        # response is empty message in this case, so just ignore it

    try:
        call_with_channel(grpc_call, context=context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e


def set_stow_state(unstow: bool = False, context: Optional[ChannelContext] = None) -> None:
    """Request dish stow or unstow operation.

    Args:
        unstow (bool): If True, request an unstow operation, otherwise a stow
            operation will be requested.
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        GrpcError: Communication or service error.
    """
    def grpc_call(channel: grpc.Channel) -> None:
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        stub.Handle(device_pb2.Request(dish_stow={"unstow": unstow}), timeout=REQUEST_TIMEOUT)
        # response is empty message in this case, so just ignore it

    try:
        call_with_channel(grpc_call, context=context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e


def set_sleep_config(start: int,
                     duration: int,
                     enable: bool = True,
                     context: Optional[ChannelContext] = None) -> None:
    """Set sleep mode configuration.

    Args:
        start (int): Time, in minutes past midnight UTC, to start sleep mode
            each day. Ignored if enable is set to False.
        duration (int): Duration of sleep mode, in minutes. Ignored if enable
            is set to False.
        enable (bool): Whether or not to enable sleep mode.
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        GrpcError: Communication or service error, including invalid start or
            duration.
    """
    if not enable:
        start = 0
        # duration of 0 not allowed, even when disabled
        duration = 1

    def grpc_call(channel: grpc.Channel) -> None:
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        stub.Handle(device_pb2.Request(
            dish_power_save={
                "power_save_start_minutes": start,
                "power_save_duration_minutes": duration,
                "enable_power_save": enable
            }),
                    timeout=REQUEST_TIMEOUT)
        # response is empty message in this case, so just ignore it

    try:
        call_with_channel(grpc_call, context=context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e
