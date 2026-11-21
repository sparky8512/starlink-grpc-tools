#!/usr/bin/env python3
"""Unit tests for the status data in starlink_grpc.py"""

import types
import unittest
from unittest import mock

import starlink_grpc


class TestStatusDataSignalQuality(unittest.TestCase):

    def test_signal_quality_is_reported(self):
        status = types.SimpleNamespace(signal_quality=0.7)

        with mock.patch("starlink_grpc.get_status", return_value=status):
            status_data = starlink_grpc.status_data()[0]

        self.assertAlmostEqual(status_data["signal_quality"], 0.7)

    def test_signal_quality_is_none_when_not_reported(self):
        status = types.SimpleNamespace()

        with mock.patch("starlink_grpc.get_status", return_value=status):
            status_data = starlink_grpc.status_data()[0]

        self.assertIsNone(status_data["signal_quality"])

    def test_signal_quality_is_in_status_field_names(self):
        with mock.patch.object(starlink_grpc, "imports_pending", False):
            names = starlink_grpc.status_field_names()[0]

        self.assertIn("signal_quality", names)


if __name__ == "__main__":
    unittest.main()
