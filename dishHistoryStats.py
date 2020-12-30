#!/usr/bin/python3
######################################################################
#
# Equivalent script to parseJsonHistory.py, except integrating the
# gRPC calls, instead of relying on separatate invocation of grpcurl.
#
# This script examines the most recent samples from the history data
# and computes several different metrics related to packet loss. By
# default, it will print the results in CSV format.
#
######################################################################

import grpc

import spacex.api.device.device_pb2
import spacex.api.device.device_pb2_grpc

import datetime
import sys
import getopt

from itertools import chain


fArgError = False

try:
    opts, args = getopt.getopt(sys.argv[1:], "ahrs:vH")
except getopt.GetoptError as err:
    print(str(err))
    fArgError = True

# Default to 1 hour worth of data samples.
parseSamples = 3600
fUsage = False
fVerbose = False
fParseAll = False
fHeader = False
fRunLengths = False

if not fArgError:
    if len(args) > 0:
        fArgError = True
    else:
        for opt, arg in opts:
            if opt == "-a":
                fParseAll = True
            elif opt == "-h":
                fUsage = True
            elif opt == "-r":
                fRunLengths = True
            elif opt == "-s":
                parseSamples = int(arg)
            elif opt == "-v":
                fVerbose = True
            elif opt == "-H":
                fHeader = True

if fUsage or fArgError:
    print("Usage: "+sys.argv[0]+" [options...]")
    print("Options:")
    print("    -a: Parse all valid samples")
    print("    -h: Be helpful")
    print("    -r: Include ping drop run length stats")
    print("    -s <num>: Parse <num> data samples, default: "+str(parseSamples))
    print("    -v: Be verbose")
    print("    -H: print CSV header instead of parsing file")
    sys.exit(1 if fArgError else 0)

if fHeader:
    header = "datetimestamp_utc,samples,total_ping_drop,count_full_ping_drop,count_obstructed,total_obstructed_ping_drop,count_full_obstructed_ping_drop,count_unscheduled,total_unscheduled_ping_drop,count_full_unscheduled_ping_drop"
    if fRunLengths:
        header+= ",init_run_fragment,final_run_fragment,"
        header += ",".join("run_seconds_" + str(x) for x in range(1, 61)) + ","
        header += ",".join("run_minutes_" + str(x) for x in range(1, 60))
        header += ",run_minutes_60_or_greater"
    print(header)
    sys.exit(0)

with grpc.insecure_channel('192.168.100.1:9200') as channel:
    stub = spacex.api.device.device_pb2_grpc.DeviceStub(channel)
    response = stub.Handle(spacex.api.device.device_pb2.Request(get_history={}))

timestamp = datetime.datetime.utcnow()

historyData = response.dish_get_history

# 'current' is the count of data samples written to the ring buffer,
# irrespective of buffer wrap.
current = int(historyData.current)
nSamples = len(historyData.pop_ping_drop_rate)

if fVerbose:
    print("current:               " + str(current))
    print("All samples:           " + str(nSamples))

nSamples = min(nSamples,current)

if fVerbose:
    print("Valid samples:         " + str(nSamples))

# This is ring buffer offset, so both index to oldest data sample and
# index to next data sample after the newest one.
offset = current % nSamples

tot = 0
totOne = 0
totUnsched = 0
totUnschedD = 0
totUnschedOne = 0
totObstruct = 0
totObstructD = 0
totObstructOne = 0

secondRuns = [0] * 60
minuteRuns = [0] * 60
runLength = 0
initRun = None

if fParseAll or nSamples < parseSamples:
    parseSamples = nSamples

# Parse the most recent parseSamples-sized set of samples. This will
# iterate samples in order from oldest to newest, although that's not
# actually required for the current set of stats being computed below.
if parseSamples <= offset:
    sampleRange = range(offset - parseSamples, offset)
else:
    sampleRange = chain(range(nSamples + offset - parseSamples, nSamples), range(0, offset))

for i in sampleRange:
    d = historyData.pop_ping_drop_rate[i]
    tot += d
    if d >= 1:
        totOne += d
        runLength += 1
    elif runLength > 0:
        if initRun is None:
            initRun = runLength
        else:
            if runLength <= 60:
                secondRuns[runLength-1] += runLength
            else:
                minuteRuns[min((runLength-1)//60-1, 59)] += runLength
        runLength = 0
    elif initRun is None:
        initRun = 0
    if not historyData.scheduled[i]:
        totUnsched += 1
        totUnschedD += d
        if d >= 1:
            totUnschedOne += d
    # scheduled=false and obstructed=true do not ever appear to overlap,
    # but in case they do in the future, treat that as just unscheduled
    # in order to avoid double-counting it.
    elif historyData.obstructed[i]:
        totObstruct += 1
        totObstructD += d
        if d >= 1:
            totObstructOne += d

# If the entire sample set is one big drop run, it will be both initial
# fragment (continued from prior sample range) and final one (continued
# to next sample range), but to avoid double-reporting, just call it
# the initial run.
if initRun is None:
    initRun = runLength
    runLength = 0

if fVerbose:
    print("Parsed samples:        " + str(parseSamples))
    print("Total ping drop:       " + str(tot))
    print("Count of drop == 1:    " + str(totOne))
    print("Obstructed:            " + str(totObstruct))
    print("Obstructed ping drop:  " + str(totObstructD))
    print("Obstructed drop == 1:  " + str(totObstructOne))
    print("Unscheduled:           " + str(totUnsched))
    print("Unscheduled ping drop: " + str(totUnschedD))
    print("Unscheduled drop == 1: " + str(totUnschedOne))
    if fRunLengths:
        print("Initial drop run fragment: " + str(initRun))
        print("Final drop run fragment: " + str(runLength))
        print("Per-second drop runs:  " + ", ".join(str(x) for x in secondRuns))
        print("Per-minute drop runs:  " + ", ".join(str(x) for x in minuteRuns))
else:
    # NOTE: When changing data output format, also change the -H header printing above.
    csvData = timestamp.replace(microsecond=0).isoformat() + "," + ",".join(str(x) for x in [parseSamples, tot, totOne, totObstruct, totObstructD, totObstructOne, totUnsched, totUnschedD, totUnschedOne])
    if fRunLengths:
        csvData += "," + ",".join(str(x) for x in chain([initRun, runLength], secondRuns, minuteRuns))
    print(csvData)
