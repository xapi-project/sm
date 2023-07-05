import unittest
import unittest.mock as mock

from uuid import uuid4

import SR
import LVHDoISCSISR
from BaseISCSI import BaseISCSISR
import SRCommand
import util
import xs_errors

import testlib
from test_ISCSISR import NonInitingISCSISR

TEST_SR_UUID = 'test_uuid'


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


class TestLVHDoISCSISR(unittest.TestCase):

    def setUp(self):
        util_patcher = mock.patch('LVHDoISCSISR.util')
        self.mock_util = util_patcher.start()
        self.mock_util.sessions_less_than_targets = util.sessions_less_than_targets
        baseiscsi_patcher = mock.patch('LVHDoISCSISR.BaseISCSI.BaseISCSISR')
        patched_baseiscsi = baseiscsi_patcher.start()
        self.mock_baseiscsi = mock.create_autospec(BaseISCSISR)
        patched_baseiscsi.return_value = self.mock_baseiscsi
        self.mock_session = mock.MagicMock()
        xenapi_patcher = mock.patch('SR.XenAPI')
        mock_xenapi = xenapi_patcher.start()
        mock_xenapi.xapi_local.return_value = self.mock_session

        copy_patcher = mock.patch('LVHDoISCSISR.SR.copy.deepcopy')
        self.mock_copy = copy_patcher.start()

        def deepcopy(to_copy):
            return to_copy

        self.mock_copy.side_effect = deepcopy

        lock_patcher = mock.patch('LVHDSR.Lock')
        self.mock_lock = lock_patcher.start()

        self.addCleanup(mock.patch.stopall)

        dummy_cmd = mock.create_autospec(SRCommand)
        dummy_cmd.dconf = {
            'SCSIid': '3600a098038313577792450384a4a6275',
            'multihomelist': 'tgt1:3260,tgt2:3260',
            'target': "10.70.89.34",
            'targetIQN': 'iqn.2009-01.example.test:iscsi085e938a'
        }
        dummy_cmd.params = {
            'command': 'nop',
            'session_ref': 'test_session',
            'host_ref': 'test_host',
            'sr_ref': 'sr_ref'
        }
        dummy_cmd.cmd = None

        self.sr_uuid = str(uuid4())
        self.subject = LVHDoISCSISR.LVHDoISCSISR(
            dummy_cmd, self.sr_uuid)

    def test_check_sr_pbd_not_found(self):
        # Arrange
        self.mock_util.find_my_pbd.return_value = None

        # Act
        self.subject.check_sr(TEST_SR_UUID)

        # Assert
        self.mock_util.find_my_pbd.assert_called_with(
            self.mock_session, 'test_host', 'sr_ref')

    def test_check_sr_correct_sessions_count(self):
        # Arrange
        self.mock_util.find_my_pbd.return_value = 'my_pbd'
        self.mock_session.xenapi.PBD.get_other_config.return_value = {
            'iscsi_sessions': 2
        }

        # Act
        self.subject.check_sr(TEST_SR_UUID)

        # Assert
        self.mock_session.xenapi.PBD.get_other_config.assert_called_with('my_pbd')

    def test_check_sr_not_enough_sessions(self):
        # Arrange
        self.mock_util.find_my_pbd.return_value = 'my_pbd'
        self.mock_session.xenapi.PBD.get_other_config.return_value = {
            'iscsi_sessions': 1
        }


        # Act
        self.subject.check_sr(TEST_SR_UUID)

        # Assert
        self.mock_baseiscsi.attach.assert_called_with(
            TEST_SR_UUID
        )
