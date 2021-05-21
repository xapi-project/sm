from __future__ import print_function
import unittest
import mock
import SR
from SR import deviceCheck
import EXTSR
import xs_errors


class TestSR(unittest.TestCase):

    class deviceTest(object):

        def __init__(self, device=None):
            self.dconf = {}
            if device:
                self.dconf['device'] = device

        @deviceCheck
        def verify(self):
            pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def create_SR(self, cmd, dconf):
        srcmd = mock.Mock()
        srcmd.dconf = dconf
        srcmd.params = {'command': cmd}
        return SR.SR(srcmd, "some SR UUID")

    def test_checkroot_no_device(self):
        sr1 = self.create_SR("sr_create", {'ISCSIid': '12333423'})
        sr1.checkroot()

    @mock.patch('SR.SR._isvalidpathstring', autospec=True)
    def test_checkroot_validdevices(self, mock_validpath):
        sr1 = self.create_SR("sr_create",
                             {'device': '/dev/sdb,/dev/sdc/,/dev/sdz'})
        mock_validpath.side_effect = iter([True, True, True])
        sr1.checkroot()

    @mock.patch('FileSR.xs_errors.XML_DEFS', "drivers/XE_SR_ERRORCODES.xml")
    @mock.patch('SR.SR._isvalidpathstring', autospec=True)
    def test_checkroot_exceptionraised(self, mock_validpath):
        sr1 = self.create_SR("sr_create",
                             {'device': '/dev/sdb,/dev/sdc/,/dev/sdz'})
        mock_validpath.side_effect = iter([True, True, False])
        self.assertRaises(SR.SROSError, sr1.checkroot)

    def test_device_check_success(self):
        """
        Test the device check decorator with a device configured
        """
        checker = TestSR.deviceTest(device="/dev/sda")

        checker.verify()

    @mock.patch('SR.xs_errors.XML_DEFS', "drivers/XE_SR_ERRORCODES.xml")
    def test_device_check_nodevice(self):
        """
        Test the device check decorator with no device configured
        """
        checker = TestSR.deviceTest()

        with self.assertRaises(SR.SROSError) as sre:
            checker.verify()
