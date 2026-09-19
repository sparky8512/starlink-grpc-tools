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

    def test_status_mode_does_not_report_other_status_groups_as_missing(self):
        self.opts.mode = ["status"]

        def fake_get_data(opts, gstate, add_item, add_sequence):
            add_item("uptime", 500, "status")
            return 0, 1000, None

        with mock.patch("dish_common.get_data", side_effect=fake_get_data):
            output = dish_grpc_prometheus.prometheus_export(self.opts, self.gstate)

        # software_update_detail, location and alert_detail are separate groups
        self.assertNotIn('metric="status_software_update_progress"', output)
        self.assertNotIn('metric="status_software_update_reboot_ready"', output)
        self.assertNotIn(
            'metric="status_seconds_until_software_update_reboot_possible"', output)
        self.assertNotIn('metric="status_latitude"', output)
        self.assertNotIn('metric="status_alert_motors_stuck"', output)

    def test_software_update_detail_mode_reports_only_its_own_metrics(self):
        self.opts.mode = ["software_update_detail"]

        def fake_get_data(opts, gstate, add_item, add_sequence):
            add_item("software_update_progress", 0.5, "status")
            add_item("software_update_reboot_ready", False, "status")
            return 0, 1000, None

        with mock.patch("dish_common.get_data", side_effect=fake_get_data):
            output = dish_grpc_prometheus.prometheus_export(self.opts, self.gstate)

        self.assertIn("starlink_status_software_update_progress 0.5 1000000", output)
        self.assertIn("starlink_status_software_update_reboot_ready 0.0 1000000", output)

        # A field the dish did not report is still flagged
        self.assertIn(
            'metric="status_seconds_until_software_update_reboot_possible"', output)

        # Nothing from the main status group is collected in this mode
        self.assertNotIn('metric="status_uptime"', output)
        self.assertNotIn('metric="status_id"', output)
        self.assertNotIn("starlink_info", output)

    def test_software_update_detail_metrics_missing_when_combined_with_status(self):
        self.opts.mode = ["status", "software_update_detail"]

        def fake_get_data(opts, gstate, add_item, add_sequence):
            add_item("uptime", 500, "status")
            return 0, 1000, None

        with mock.patch("dish_common.get_data", side_effect=fake_get_data):
            output = dish_grpc_prometheus.prometheus_export(self.opts, self.gstate)

        self.assertIn('metric="status_software_update_progress"', output)
        self.assertIn('metric="status_software_update_reboot_ready"', output)

    def test_info_metric_only_emitted_when_info_fields_present(self):
        self.opts.mode = ["ping_drop"]

        def fake_get_data(opts, gstate, add_item, add_sequence):
            add_item("samples", 100, "ping_stats")
            return 0, None, 2000

        with mock.patch("dish_common.get_data", side_effect=fake_get_data):
            output = dish_grpc_prometheus.prometheus_export(self.opts, self.gstate)

        self.assertNotIn("starlink_info", output)

        self.opts.mode = ["status"]

        def fake_get_data_with_info(opts, gstate, add_item, add_sequence):
            add_item("id", "ut01", "status")
            add_item("hardware_version", "rev4", "status")
            add_item("software_version", "abc", "status")
            return 0, 1000, None

        with mock.patch("dish_common.get_data", side_effect=fake_get_data_with_info):
            output = dish_grpc_prometheus.prometheus_export(self.opts, self.gstate)

        self.assertIn('starlink_info{id="ut01",hardware_version="rev4",software_version="abc"}',
                      output)

    def test_every_metric_belongs_to_a_mode(self):
        all_modes = [
            "status", "software_update_detail", "alert_detail", "usage", "location", "power",
            "ping_drop"
        ]
        for name in dish_grpc_prometheus.METRICS_INFO:
            self.assertTrue(dish_grpc_prometheus.is_metric_expected(name, all_modes), name)
            # A metric with no owning mode would be expected regardless of the selection
            self.assertFalse(dish_grpc_prometheus.is_metric_expected(name, []), name)

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
