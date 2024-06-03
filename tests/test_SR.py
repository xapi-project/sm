import unittest
import unittest.mock as mock
import SR
from SR import deviceCheck
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

    def create_SR(self, cmd, dconf, cmd_params=None):
        srcmd = mock.Mock()
        srcmd.dconf = dconf
        srcmd.params = {'command': cmd}
        if cmd_params:
            srcmd.params.update(cmd_params)
        return SR.SR(srcmd, "some SR UUID")

    def test_device_check_success(self):
        """
        Test the device check decorator with a device configured
        """
        checker = TestSR.deviceTest(device="/dev/sda")

        checker.verify()

    def test_device_check_nodevice(self):
        """
        Test the device check decorator with no device configured
        """
        checker = TestSR.deviceTest()

        with self.assertRaises(xs_errors.SROSError):
            checker.verify()

    @mock.patch('SR.SR.scan', autospec=True)
    def test_after_master_attach_success(self, mock_scan):
        """
        Test that after_master_attach calls scan
        """
        sr1 = self.create_SR("sr_create", {'ISCSIid': '12333423'})

        sr1.after_master_attach('dummy uuid')

        mock_scan.assert_called_once_with(sr1, 'dummy uuid')

    @mock.patch('SR.XenAPI')
    @mock.patch('SR.SR.scan', autospec=True)
    @mock.patch('SR.util.SMlog', autospec=True)
    def test_after_master_attach_vdi_not_available(
            self, mock_log, mock_scan, mock_xenapi):
        """
        Test that after_master_attach calls scan
        """
        mock_session = mock.MagicMock(name='MockXapiSession')
        mock_xenapi.xapi_local.return_value = mock_session
        sr1 = self.create_SR("sr_create", {'ISCSIid': '12333423'},
            {'session_ref': 'session1'})

        mock_scan.side_effect = xs_errors.SROSError(
            46, "The VDI is not available")

        sr1.after_master_attach('dummy uuid')

        mock_scan.assert_called_once_with(sr1, 'dummy uuid')

        self.assertEqual(1, mock_log.call_count)
        self.assertIn("Error in SR.after_master_attach",
                      mock_log.call_args[0][0])
        mock_session.xenapi.message.create.assert_called_once_with(
            "POST_ATTACH_SCAN_FAILED", 2, 'SR', 'dummy uuid', mock.ANY)
