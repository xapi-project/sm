import unittest

import testlib

import xs_errors


class TestXenError(unittest.TestCase):
    @testlib.with_context
    def test_without_xml_defs(self, context):
        with self.assertRaises(Exception) as e:
            xs_errors.XenError('blah')

        self.assertTrue("No XML def file found" in str(e.exception))

    @testlib.with_context
    def test_xml_defs(self, context):
        context.setup_error_codes()

        with self.assertRaises(Exception) as e:
            raise xs_errors.XenError('SRInUse')

        self.assertTrue("The SR device is currently in use" in str(e.exception))
