#!/usr/bin/env python3
"""Unit tests for dish_obstruction_map.py"""

import sys
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


if __name__ == "__main__":
    unittest.main()
