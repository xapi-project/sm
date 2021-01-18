from __future__ import print_function
import unittest
import mock
import SR
import xs_errors


class TestSR(unittest.TestCase):

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
        mock_validpath.side_effect = [True, True, True]
        sr1.checkroot()

    @mock.patch('FileSR.xs_errors.XML_DEFS', "drivers/XE_SR_ERRORCODES.xml")
    @mock.patch('SR.SR._isvalidpathstring', autospec=True)
    def test_checkroot_exceptionraised(self, mock_validpath):
        sr1 = self.create_SR("sr_create",
                             {'device': '/dev/sdb,/dev/sdc/,/dev/sdz'})
        mock_validpath.side_effect = [True, True, False]
        self.assertRaises(SR.SROSError, sr1.checkroot)
