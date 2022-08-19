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
: **raw_wedges_fraction_obstructed** : A 12 element sequence. Wedges
    presumably correlate with the ones in *wedges_fraction_obstructed*, but
    the exact relationship is unknown. The numbers in this one are generally
    higher and may represent fraction of the wedge, in which case max value
    for each element should be 1.
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


def resolve_imports(channel):
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
        else:
            msg = str(e)
        super().__init__(msg, *args, **kwargs)


class UnwrappedHistory:
    """Empty class for holding a copy of grpc history data."""


class ChannelContext:
    """A wrapper for reusing an open grpc Channel across calls.

    `close()` should be called on the object when it is no longer
    in use.
    """
    def __init__(self, target=None):
        self.channel = None
        self.target = "192.168.100.1:9200" if target is None else target

    def get_channel(self):
        reused = True
        if self.channel is None:
            self.channel = grpc.insecure_channel(self.target)
            reused = False
        return self.channel, reused

    def close(self):
        if self.channel is not None:
            self.channel.close()
        self.channel = None


def call_with_channel(function, *args, context=None, **kwargs):
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


def status_field_names(context=None):
    """Return the field names of the status data.

    Note:
        See module level docs regarding brackets in field names.

    Args:
        context (ChannelContext): Optionally provide a channel for (re)use
            with reflection service.

    Returns:
        A tuple with 3 lists, with status data field names, alert detail field
        names, and obstruction detail field names, in that order.

    Raises:
        GrpcError: No user terminal is currently available to resolve imports
            via reflection.
    """
    if imports_pending:
        try:
            call_with_channel(resolve_imports, context=context)
        except grpc.RpcError as e:
            raise GrpcError(e)
    alert_names = []
    for field in dish_pb2.DishAlerts.DESCRIPTOR.fields:
        alert_names.append("alert_" + field.name)

    return [
        "id",
        "hardware_version",
        "software_version",
        "state",
        "uptime",
        "snr",
        "seconds_to_first_nonempty_slot",
        "pop_ping_drop_rate",
        "downlink_throughput_bps",
        "uplink_throughput_bps",
        "pop_ping_latency_ms",
        "alerts",
        "fraction_obstructed",
        "currently_obstructed",
        "seconds_obstructed",
        "obstruction_duration",
        "obstruction_interval",
        "direction_azimuth",
        "direction_elevation",
    ], [
        "wedges_fraction_obstructed[12]",
        "raw_wedges_fraction_obstructed[12]",
        "valid_s",
    ], alert_names


def status_field_types(context=None):
    """Return the field types of the status data.

    Return the type classes for each field. For sequence types, the type of
    element in the sequence is returned, not the type of the sequence.

    Args:
        context (ChannelContext): Optionally provide a channel for (re)use
            with reflection service.

    Returns:
        A tuple with 3 lists, with status data field types, alert detail field
        types, and obstruction detail field types, in that order.

    Raises:
        GrpcError: No user terminal is currently available to resolve imports
            via reflection.
    """
    if imports_pending:
        try:
            call_with_channel(resolve_imports, context=context)
        except grpc.RpcError as e:
            raise GrpcError(e)
    return [
        str,  # id
        str,  # hardware_version
        str,  # software_version
        str,  # state
        int,  # uptime
        float,  # snr
        float,  # seconds_to_first_nonempty_slot
        float,  # pop_ping_drop_rate
        float,  # downlink_throughput_bps
        float,  # uplink_throughput_bps
        float,  # pop_ping_latency_ms
        int,  # alerts
        float,  # fraction_obstructed
        bool,  # currently_obstructed
        float,  # seconds_obstructed
        float,  # obstruction_duration
        float,  # obstruction_interval
        float,  # direction_azimuth
        float,  # direction_elevation
    ], [
        float,  # wedges_fraction_obstructed[]
        float,  # raw_wedges_fraction_obstructed[]
        float,  # valid_s
    ], [bool] * len(dish_pb2.DishAlerts.DESCRIPTOR.fields)


def get_status(context=None):
    """Fetch status data and return it in grpc structure format.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        grpc.RpcError: Communication or service error.
    """
    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(get_status={}), timeout=REQUEST_TIMEOUT)
        return response.dish_get_status

    return call_with_channel(grpc_call, context=context)


def get_id(context=None):
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
    except grpc.RpcError as e:
        raise GrpcError(e)


def status_data(context=None):
    """Fetch current status data.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls.

    Returns:
        A tuple with 3 dicts, mapping status data field names, alert detail
        field names, and obstruction detail field names to their respective
        values, in that order.

    Raises:
        GrpcError: Failed getting status info from the Starlink user terminal.
    """
    try:
        status = get_status(context)
    except grpc.RpcError as e:
        raise GrpcError(e)

    if status.HasField("outage"):
        if status.outage.cause == dish_pb2.DishOutage.Cause.NO_SCHEDULE:
            # Special case translate this to equivalent old name
            state = "SEARCHING"
        else:
            state = dish_pb2.DishOutage.Cause.Name(status.outage.cause)
    else:
        state = "CONNECTED"

    # More alerts may be added in future, so in addition to listing them
    # individually, provide a bit field based on field numbers of the
    # DishAlerts message.
    alerts = {}
    alert_bits = 0
    for field in status.alerts.DESCRIPTOR.fields:
        value = getattr(status.alerts, field.name)
        alerts["alert_" + field.name] = value
        if field.number < 65:
            alert_bits |= (1 if value else 0) << (field.number - 1)

    if (status.obstruction_stats.avg_prolonged_obstruction_duration_s > 0.0 and not
        math.isnan(status.obstruction_stats.avg_prolonged_obstruction_interval_s)):
        obstruction_duration = status.obstruction_stats.avg_prolonged_obstruction_duration_s
        obstruction_interval = status.obstruction_stats.avg_prolonged_obstruction_interval_s
    else:
        obstruction_duration = None
        obstruction_interval = None

    return {
        "id": status.device_info.id,
        "hardware_version": status.device_info.hardware_version,
        "software_version": status.device_info.software_version,
        "state": state,
        "uptime": status.device_state.uptime_s,
        "snr": None,  # obsoleted in grpc service
        "seconds_to_first_nonempty_slot": status.seconds_to_first_nonempty_slot,
        "pop_ping_drop_rate": status.pop_ping_drop_rate,
        "downlink_throughput_bps": status.downlink_throughput_bps,
        "uplink_throughput_bps": status.uplink_throughput_bps,
        "pop_ping_latency_ms": status.pop_ping_latency_ms,
        "alerts": alert_bits,
        "fraction_obstructed": status.obstruction_stats.fraction_obstructed,
        "currently_obstructed": status.obstruction_stats.currently_obstructed,
        "seconds_obstructed": None,  # obsoleted in grpc service
        "obstruction_duration": obstruction_duration,
        "obstruction_interval": obstruction_interval,
        "direction_azimuth": status.boresight_azimuth_deg,
        "direction_elevation": status.boresight_elevation_deg,
    }, {
        "wedges_fraction_obstructed[]": status.obstruction_stats.wedge_abs_fraction_obstructed,
        "raw_wedges_fraction_obstructed[]": status.obstruction_stats.wedge_fraction_obstructed,
        "valid_s": status.obstruction_stats.valid_s,
    }, alerts


def history_bulk_field_names():
    """Return the field names of the bulk history data.

    Note:
        See module level docs regarding brackets in field names.

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


def history_bulk_field_types():
    """Return the field types of the bulk history data.

    Return the type classes for each field. For sequence types, the type of
    element in the sequence is returned, not the type of the sequence.

    Returns:
        A tuple with 2 lists, the first with general data types, the second
        with bulk history data types.
    """
    return [
        int,  # samples
        int,  # end_counter
    ], [
        float,  # pop_ping_drop_rate[]
        float,  # pop_ping_latency_ms[]
        float,  # downlink_throughput_bps[]
        float,  # uplink_throughput_bps[]
        float,  # snr[]
        bool,  # scheduled[]
        bool,  # obstructed[]
    ]


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
    return [
        int,  # samples
        int,  # end_counter
    ], [
        float,  # total_ping_drop
        int,  # count_full_ping_drop
        int,  # count_obstructed
        float,  # total_obstructed_ping_drop
        int,  # count_full_obstructed_ping_drop
        int,  # count_unscheduled
        float,  # total_unscheduled_ping_drop
        int,  # count_full_unscheduled_ping_drop
    ], [
        int,  # init_run_fragment
        int,  # final_run_fragment
        int,  # run_seconds[]
        int,  # run_minutes[]
    ], [
        float,  # mean_all_ping_latency
        float,  # deciles_all_ping_latency[]
        float,  # mean_full_ping_latency
        float,  # deciles_full_ping_latency[]
        float,  # stdev_full_ping_latency
    ], [
        int,  # load_bucket_samples[]
        float,  # load_bucket_min_latency[]
        float,  # load_bucket_median_latency[]
        float,  # load_bucket_max_latency[]
    ], [
        int,  # download_usage
        int,  # upload_usage
    ]


def get_history(context=None):
    """Fetch history data and return it in grpc structure format.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        grpc.RpcError: Communication or service error.
    """
    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(get_history={}), timeout=REQUEST_TIMEOUT)
        return response.dish_get_history

    return call_with_channel(grpc_call, context=context)


def _compute_sample_range(history, parse_samples, start=None, verbose=False):
    current = int(history.current)
    samples = len(history.pop_ping_drop_rate)

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
    if start_offset < end_offset:
        sample_range = range(start_offset, end_offset)
    else:
        sample_range = chain(range(start_offset, samples), range(0, end_offset))

    return sample_range, current - start, current


def concatenate_history(history1, history2, samples1=-1, start1=None, verbose=False):
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
    size2 = len(history2.pop_ping_drop_rate)
    new_samples = history2.current - history1.current
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
        setattr(unwrapped, field, [])
    unwrapped.unwrapped = True

    sample_range, ignore1, ignore2 = _compute_sample_range(  # pylint: disable=unused-variable
        history1, samples1, start=start1)
    for i in sample_range:
        for field in HISTORY_FIELDS:
            getattr(unwrapped, field).append(getattr(history1, field)[i])

    sample_range, ignore1, ignore2 = _compute_sample_range(history2, new_samples)  # pylint: disable=unused-variable
    for i in sample_range:
        for field in HISTORY_FIELDS:
            getattr(unwrapped, field).append(getattr(history2, field)[i])

    unwrapped.current = history2.current
    return unwrapped


def history_bulk_data(parse_samples, start=None, verbose=False, context=None, history=None):
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

    for i in sample_range:
        pop_ping_drop_rate.append(history.pop_ping_drop_rate[i])
        pop_ping_latency_ms.append(
            history.pop_ping_latency_ms[i] if history.pop_ping_drop_rate[i] < 1 else None)
        downlink_throughput_bps.append(history.downlink_throughput_bps[i])
        uplink_throughput_bps.append(history.uplink_throughput_bps[i])

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


def history_ping_stats(parse_samples, verbose=False, context=None):
    """Deprecated. Use history_stats instead."""
    return history_stats(parse_samples, verbose=verbose, context=context)[0:3]


def history_stats(parse_samples, start=None, verbose=False, context=None, history=None):
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
        except grpc.RpcError as e:
            raise GrpcError(e)

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

    rtt_full = []
    rtt_all = []
    rtt_buckets = [[] for _ in range(15)]

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

        down = history.downlink_throughput_bps[i]
        usage_down += down
        up = history.uplink_throughput_bps[i]
        usage_up += up

        rtt = history.pop_ping_latency_ms[i]
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


def get_obstruction_map(context=None):
    """Fetch obstruction map data and return it in grpc structure format.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        grpc.RpcError: Communication or service error.
    """
    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(dish_get_obstruction_map={}),
                               timeout=REQUEST_TIMEOUT)
        return response.dish_get_obstruction_map

    return call_with_channel(grpc_call, context=context)


def obstruction_map(context=None):
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
        GrpcError: Failed getting status info from the Starlink user terminal.
    """
    try:
        map_data = get_obstruction_map(context)
    except grpc.RpcError as e:
        raise GrpcError(e)

    cols = map_data.num_cols
    return tuple((map_data.snr[i:i + cols]) for i in range(0, cols * map_data.num_rows, cols))


def reboot(context=None):
    """Request dish reboot operation.

    Args:
        context (ChannelContext): Optionally provide a channel for reuse
            across repeated calls. If an existing channel is reused, the RPC
            call will be retried at most once, since connectivity may have
            been lost and restored in the time since it was last used.

    Raises:
        GrpcError: Communication or service error.
    """
    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        stub.Handle(device_pb2.Request(reboot={}), timeout=REQUEST_TIMEOUT)
        # response is empty message in this case, so just ignore it
        return 0

    try:
        call_with_channel(grpc_call, context=context)
    except grpc.RpcError as e:
        raise GrpcError(e)


def set_stow_state(unstow=False, context=None):
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
    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        stub.Handle(device_pb2.Request(dish_stow={"unstow": unstow}), timeout=REQUEST_TIMEOUT)
        # response is empty message in this case, so just ignore it
        return 0

    try:
        call_with_channel(grpc_call, context=context)
    except grpc.RpcError as e:
        raise GrpcError(e)
