import unittest
from unittest import mock

import xs_errors


@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
class TestXenError(unittest.TestCase):
    @mock.patch('xs_errors.os.path.exists', autospec=True)
    def test_without_xml_defs(self, mock_exists):
        mock_exists.return_value = False

        with self.assertRaises(Exception) as e:
            xs_errors.XenError('blah')

        self.assertTrue("No XML def file found" in str(e.exception))

    def test_xml_defs(self):
        with self.assertRaises(Exception) as e:
            raise xs_errors.XenError('SRInUse')

        self.assertTrue("The SR device is currently in use" in str(e.exception))
