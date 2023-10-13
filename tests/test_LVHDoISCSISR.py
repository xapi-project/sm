import mock
import unittest

import SR
import LVHDoISCSISR
import xs_errors

import testlib
from test_ISCSISR import NonInitingISCSISR


class RandomError(Exception):
    pass


class NonInitingLVHDoISCSISR(LVHDoISCSISR.LVHDoISCSISR):

    """
    Helper class; Creates dummy LVHDoISCSISR object.
    Add attributes/methods as appropriate.
    """

    def __init__(self, extra_dconf=None, extra_params=None):

        from SRCommand import SRCommand
        from DummySR import DRIVER_INFO

        self.mpath = "false"
        self.dconf = {
            'target': 'target',
            'localIQN': 'localIQN',
            'targetIQN': 'targetIQN',
            'SCSIid': 'SCSIid'
        }

        self.srcmd = mock.Mock(spec=SRCommand(DRIVER_INFO))
        self.srcmd.dconf = self.dconf

        self.original_srcmd = self.srcmd

        self.srcmd.params = {'command': 'command'}

        self.srcmd.dconf.update(extra_dconf or {})
        self.srcmd.params.update(extra_params or {})


class TestLVHDoISCSISR_load(unittest.TestCase):

    """
    Tests for 'LVHDoISCSISR.load()'
    """

    def setUp(self):
        patchers = [
            mock.patch(
                'BaseISCSI.BaseISCSISR',
                return_value=NonInitingISCSISR()
            ),
            mock.patch('util._convertDNS', return_value='127.0.0.1'),
            mock.patch('SR.driver'),
        ]

        for patcher in patchers:
            patcher.start()

        self.lvhd_o_iscsi_sr = NonInitingLVHDoISCSISR(
            {'targetIQN': '*'},
            {'command': 'sr_create'}
        )

        self.fake_uuid = 'deadbeef'

        self.addCleanup(mock.patch.stopall)

    @mock.patch('iscsilib.ensure_daemon_running_ok')
    @testlib.with_context
    def test_1st_try_block_raise_XenError(
            self,
            context,
            mock_iscsilib_ensure_daemon_running_ok):
        context.setup_error_codes()

        mock_iscsilib_ensure_daemon_running_ok.side_effect = xs_errors.XenError(
            'ISCSIInitiator',
            'Raise XenError'
        )

        with self.assertRaises(SR.SROSError) as cm:
            self.lvhd_o_iscsi_sr.load(self.fake_uuid)

        self.assertEqual(cm.exception.errno, 70)
        self.assertEqual(
            str(cm.exception),
            'Failed to set ISCSI initiator [opterr=Raise XenError]'
        )

    @mock.patch('iscsilib.ensure_daemon_running_ok')
    @testlib.with_context
    def test_1st_try_block_raise_RandomError(
            self,
            context,
            mock_iscsilib_ensure_daemon_running_ok):
        context.setup_error_codes()

        mock_iscsilib_ensure_daemon_running_ok.side_effect = RandomError(
            'Raise RandomError'
        )

        with self.assertRaises(SR.SROSError) as cm:
            self.lvhd_o_iscsi_sr.load(self.fake_uuid)

        self.assertEqual(cm.exception.errno, 202)
        self.assertEqual(
            str(cm.exception),
            'General backend error [opterr=Raise RandomError]'
        )
