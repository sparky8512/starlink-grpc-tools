#!/usr/bin/env python3
"""Unit tests for the weighted latency stats in history_stats()"""

import json
import os
import tempfile
import types
import unittest

import starlink_grpc
import starlink_json

# 10 seconds with no ping drop at 10 ms, followed by 5 seconds with 90% ping
# drop where the few pings that did succeed took 100 ms. Each sample is
# weighted by its ping success (1 - drop), so the 5 lossy samples have weight
# 0.1 each.
PING_DROP = [0.0] * 10 + [0.9] * 5
PING_LATENCY = [10.0] * 10 + [100.0] * 5
EXPECTED_WEIGHTED_MEAN = (10 * 10.0 * 1.0 + 5 * 100.0 * 0.1) / (10 * 1.0 + 5 * 0.1)


def fake_history(drop, latency):
    history = types.SimpleNamespace()
    history.current = len(drop)
    history.pop_ping_drop_rate = list(drop)
    history.pop_ping_latency_ms = list(latency)
    history.downlink_throughput_bps = [0.0] * len(drop)
    history.uplink_throughput_bps = [0.0] * len(drop)
    return history


class TestWeightedMeanLatency(unittest.TestCase):

    def test_grpc_mean_all_ping_latency_is_weighted_for_tail_samples(self):
        groups = starlink_grpc.history_stats(-1, history=fake_history(PING_DROP, PING_LATENCY))
        latency_stats = groups[3]

        self.assertAlmostEqual(latency_stats["mean_all_ping_latency"], EXPECTED_WEIGHTED_MEAN)
        # sanity check that the expected value is not just the unweighted mean
        self.assertAlmostEqual(EXPECTED_WEIGHTED_MEAN, 100 / 7)

    def test_grpc_mean_all_ping_latency_with_uniform_weights(self):
        groups = starlink_grpc.history_stats(-1,
                                             history=fake_history([0.0] * 4, [10.0, 20.0, 30.0, 40.0]))

        self.assertAlmostEqual(groups[3]["mean_all_ping_latency"], 25.0)

    def test_json_mean_all_ping_latency_is_weighted_for_tail_samples(self):
        dump = {
            "dishGetHistory": {
                "current": len(PING_DROP),
                "popPingDropRate": PING_DROP,
                "popPingLatencyMs": PING_LATENCY,
                "downlinkThroughputBps": [0.0] * len(PING_DROP),
                "uplinkThroughputBps": [0.0] * len(PING_DROP),
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "history.json")
            with open(filename, "w", encoding="utf-8") as outfile:
                json.dump(dump, outfile)
            groups = starlink_json.history_stats(filename, -1)
        latency_stats = groups[3]

        self.assertAlmostEqual(latency_stats["mean_all_ping_latency"], EXPECTED_WEIGHTED_MEAN)


if __name__ == "__main__":
    unittest.main()
