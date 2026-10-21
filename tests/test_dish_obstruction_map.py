#!/usr/bin/env python3
"""Unit tests for dish_obstruction_map.py"""

import os
import sys
import tempfile
import unittest
from unittest import mock

import dish_obstruction_map
import starlink_grpc


class TestDishObstructionMapMain(unittest.TestCase):

    def run_main(self, args):
        """Run main() with the given command line, returns the SystemExit code."""
        with mock.patch.object(sys, "argv", ["dish_obstruction_map.py"] + args):
            with self.assertRaises(SystemExit) as cm:
                dish_obstruction_map.main()
        return cm.exception.code

    def test_reset_only_exits_cleanly(self):
        """--reset without a filename is allowed and should not crash."""
        with mock.patch.object(starlink_grpc, "ChannelContext") as mock_context_class, \
                mock.patch.object(starlink_grpc, "reset_obstruction_map") as mock_reset, \
                mock.patch.object(starlink_grpc, "obstruction_map") as mock_get_map:
            code = self.run_main(["--reset"])

        self.assertEqual(code, 0)
        mock_reset.assert_called_once_with(mock_context_class.return_value)
        mock_get_map.assert_not_called()
        mock_context_class.return_value.close.assert_called_once_with()

    def test_filename_exit_code_follows_loop_body(self):
        with mock.patch.object(starlink_grpc, "ChannelContext"), \
                mock.patch.object(dish_obstruction_map, "loop_body", return_value=1):
            code = self.run_main(["out.png"])

        self.assertEqual(code, 1)


class TestDishObstructionMapLoopBody(unittest.TestCase):

    def run_loop_body(self, snr_data, filename):
        with mock.patch.object(sys, "argv", ["dish_obstruction_map.py", filename]):
            opts = dish_obstruction_map.parse_args()
        with mock.patch.object(starlink_grpc, "obstruction_map", return_value=snr_data):
            return dish_obstruction_map.loop_body(opts, mock.Mock())

    def test_empty_map_data_does_not_create_output_file(self):
        for snr_data in ([], [[]]):
            with self.subTest(snr_data=snr_data), tempfile.TemporaryDirectory() as tmpdir:
                filename = os.path.join(tmpdir, "map.png")
                with self.assertLogs(level="ERROR"):
                    rc = self.run_loop_body(snr_data, filename)

                self.assertEqual(rc, 1)
                self.assertEqual(os.listdir(tmpdir), [])

    def test_valid_map_data_writes_png_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "map.png")
            rc = self.run_loop_body([[1.0, 0.0], [0.5, -1.0]], filename)

            self.assertEqual(rc, 0)
            with open(filename, "rb") as infile:
                self.assertEqual(infile.read(8)[1:4], b"PNG")


if __name__ == "__main__":
    unittest.main()
