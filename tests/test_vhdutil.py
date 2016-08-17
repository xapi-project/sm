import unittest
import mock

import SR
import vhdutil
import xs_errors
import testlib

MAX_VHD_SIZE = 2093050 * 1024 * 1024

class TestVhdUtil(unittest.TestCase):

    def test_validate_and_round_min_size(self):
        size = vhdutil.validate_and_round_vhd_size(2 * 1024 * 1024)

        self.assertTrue(size == 2 * 1024 * 1024)

    def test_validate_and_round_max_size(self):
        size = vhdutil.validate_and_round_vhd_size(MAX_VHD_SIZE)

        self.assertTrue(size == MAX_VHD_SIZE)

    def test_validate_and_round_odd_size_up_to_next_boundary(self):
        size = vhdutil.validate_and_round_vhd_size(MAX_VHD_SIZE - 1)

        self.assertTrue(size == MAX_VHD_SIZE)

    @testlib.with_context
    def test_validate_and_round_negative(self, context):
        context.setup_error_codes()
        with self.assertRaises(SR.SROSError):
            vhdutil.validate_and_round_vhd_size(-1)

    @testlib.with_context
    def test_validate_and_round_too_large(self, context):
        context.setup_error_codes()
        with self.assertRaises(SR.SROSError):
            vhdutil.validate_and_round_vhd_size(MAX_VHD_SIZE + 1)
