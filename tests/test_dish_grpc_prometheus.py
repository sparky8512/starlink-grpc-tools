#!/usr/bin/env python3
"""Unit tests for dish_grpc_prometheus.py"""

from http import HTTPStatus
import io
import unittest
from unittest import mock

import dish_grpc_prometheus


class TestDishGrpcPrometheus(unittest.TestCase):

    def setUp(self):
        self.opts = mock.Mock()
        self.gstate = mock.Mock()
        self.gstate.lock = mock.MagicMock()

    def test_status_mode_does_not_report_inactive_modes_as_missing(self):
        self.opts.mode = ["status"]

        def fake_get_data(opts, gstate, add_item, add_sequence):
            add_item("uptime", 12345, "status")
            add_item("pop_ping_drop_rate", 0.0, "status")
            return 0, 1000, None

        with mock.patch("dish_common.get_data", side_effect=fake_get_data):
            output = dish_grpc_prometheus.prometheus_export(self.opts, self.gstate)

        # Expected status metrics should be emitted
        self.assertIn("starlink_status_uptime_seconds 12345.0 1000000", output)
        self.assertIn("starlink_status_pop_ping_drop_rate 0.0 1000000", output)

        # Inactive modes (ping_drop, usage, power) must not appear in missing metrics
        self.assertNotIn('metric="ping_stats_samples"', output)
        self.assertNotIn('metric="ping_stats_total_ping_drop"', output)
        self.assertNotIn('metric="usage_download_usage"', output)
        self.assertNotIn('metric="power_latest_power"', output)

    def test_ping_drop_mode_does_not_report_status_metrics_as_missing(self):
        self.opts.mode = ["ping_drop"]

        def fake_get_data(opts, gstate, add_item, add_sequence):
            add_item("samples", 100, "ping_stats")
            add_item("total_ping_drop", 1.5, "ping_stats")
            return 0, None, 2000

        with mock.patch("dish_common.get_data", side_effect=fake_get_data):
            output = dish_grpc_prometheus.prometheus_export(self.opts, self.gstate)

        # Expected ping stats metrics should be emitted
        self.assertIn("starlink_ping_stats_samples 100.0 2000000", output)
        self.assertIn("starlink_ping_stats_total_ping_drop 1.5 2000000", output)

        # Status and power metrics must not be flagged as missing
        self.assertNotIn('metric="status_uptime"', output)
        self.assertNotIn('metric="status_id"', output)
        self.assertNotIn('metric="power_latest_power"', output)

    def test_genuine_missing_metric_reported_when_mode_is_active(self):
        self.opts.mode = ["status"]

        def fake_get_data(opts, gstate, add_item, add_sequence):
            # Only provide uptime; other status metrics will be missing
            add_item("uptime", 500, "status")
            return 0, 1000, None

        with mock.patch("dish_common.get_data", side_effect=fake_get_data):
            output = dish_grpc_prometheus.prometheus_export(self.opts, self.gstate)

        # Missing status metrics should be reported
        self.assertIn('metric="status_pop_ping_drop_rate"', output)
        self.assertIn('metric="status_id"', output)

        # Inactive mode metrics still should not be reported
        self.assertNotIn('metric="ping_stats_samples"', output)

    def test_metrics_request_handler_content_length_and_charset(self):
        class DummyServer:
            def __init__(self):
                self.opts = mock.Mock()
                self.opts.mode = ["status"]
                self.gstate = mock.Mock()
                self.gstate.lock = mock.MagicMock()

        server = DummyServer()
        handler = dish_grpc_prometheus.MetricsRequestHandler.__new__(
            dish_grpc_prometheus.MetricsRequestHandler
        )
        handler.server = server
        handler.client_address = ("127.0.0.1", 54321)
        handler.path = "/metrics"
        handler.requestline = "GET /metrics HTTP/1.1"
        handler.request_version = "HTTP/1.1"
        handler.rfile = io.BytesIO()
        handler.wfile = io.BytesIO()

        # Multi-byte UTF-8 test string
        unicode_metric_content = "starlink_test_metric 1.0\n# Comment with UTF-8: \U0001F680\n"
        expected_bytes = unicode_metric_content.encode("utf-8")

        with mock.patch("dish_grpc_prometheus.prometheus_export", return_value=unicode_metric_content):
            handler.do_GET()

        raw_response = handler.wfile.getvalue()
        headers_part, body_part = raw_response.split(b"\r\n\r\n")

        self.assertEqual(body_part, expected_bytes)
        self.assertIn(f"Content-Length: {len(expected_bytes)}".encode(), headers_part)
        self.assertIn(b"Content-type: text/plain; charset=utf-8", headers_part)


if __name__ == "__main__":
    unittest.main()
