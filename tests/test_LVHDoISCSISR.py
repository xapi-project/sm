import os
import unittest
import unittest.mock as mock

import traceback

from uuid import uuid4

import SR
import LVHDoISCSISR
from sm.core import iscsi
from sm.BaseISCSI import BaseISCSISR
from sm import SRCommand
from sm.core import util
from sm.core import xs_errors

import testlib
from shared_iscsi_test_base import ISCSITestCase
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


@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
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
            mock.patch('LVHDoISCSISR.util._convertDNS', return_value='127.0.0.1'),
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

    @mock.patch('LVHDoISCSISR.iscsi.ensure_daemon_running_ok')
    def test_1st_try_block_raise_XenError(
            self,
            mock_iscsi_ensure_daemon_running_ok):
        mock_iscsi_ensure_daemon_running_ok.side_effect = xs_errors.XenError(
            'ISCSIInitiator',
            'Raise XenError'
        )

        with self.assertRaises(xs_errors.SROSError) as cm:
            self.lvhd_o_iscsi_sr.load(self.fake_uuid)

        self.assertEqual(cm.exception.errno, 70)
        self.assertEqual(
            str(cm.exception),
            'Failed to set ISCSI initiator [opterr=Raise XenError]'
        )

    @mock.patch('LVHDoISCSISR.iscsi.ensure_daemon_running_ok')
    def test_1st_try_block_raise_RandomError(
            self,
            mock_iscsi_ensure_daemon_running_ok):
        mock_iscsi_ensure_daemon_running_ok.side_effect = RandomError(
            'Raise RandomError'
        )

        with self.assertRaises(xs_errors.SROSError) as cm:
            self.lvhd_o_iscsi_sr.load(self.fake_uuid)

        self.assertEqual(cm.exception.errno, 202)
        self.assertEqual(
            str(cm.exception),
            'General backend error [opterr=Raise RandomError]'
        )


@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
class TestLVHDoISCSISR(ISCSITestCase):

    TEST_CLASS = 'LVHDoISCSISR'

    def setUp(self):
        util_patcher = mock.patch('LVHDoISCSISR.util', autospec=True)
        self.mock_util = util_patcher.start()
        # self.mock_util.SMlog.side_effect = print
        self.mock_util.isVDICommand = util.isVDICommand
        self.mock_util.sessions_less_than_targets = util.sessions_less_than_targets

        self.base_srs = set()
        baseiscsi_patcher = mock.patch('LVHDoISCSISR.BaseISCSI.BaseISCSISR',
                                       autospec=True)
        patched_baseiscsi = baseiscsi_patcher.start()
        patched_baseiscsi.side_effect = self.baseiscsi
        lvhdsr_patcher = mock.patch ('LVHDoISCSISR.LVHDSR')

        self.mock_lvhdsr = lvhdsr_patcher.start()
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
        lvlock_patcher = mock.patch('LVHDSR.lvutil.Fairlock')
        self.mock_lvlock = lvlock_patcher.start()

        self.addCleanup(mock.patch.stopall)

        super().setUp()

    @property
    def mock_baseiscsi(self):
        assert len(self.base_srs) == 1
        single_sr = None
        for sr in self.base_srs:
            single_sr = sr

        return single_sr

    def baseiscsi(self, srcmd, sr_uuid):
        new_baseiscsi = mock.create_autospec(BaseISCSISR)
        local_iqn = srcmd.dconf['localIQN']
        target_iqn = srcmd.dconf['targetIQN']
        target = srcmd.dconf['target']
        new_baseiscsi.localIQN = local_iqn
        new_baseiscsi.targetIQN = target_iqn
        new_baseiscsi.target = target
        new_baseiscsi.path = os.path.join('/dev/iscsi', target_iqn, target)
        new_baseiscsi.port = 3260
        new_baseiscsi.chapuser = srcmd.dconf.get('chapuser')
        new_baseiscsi.chappassword = srcmd.dconf.get('chappassword')
        new_baseiscsi.incoming_chapuser = srcmd.dconf.get('incoming_chapuser')
        new_baseiscsi.incoming_chappassword = srcmd.dconf.get('incoming_chappassword')
        self.base_srs.add(new_baseiscsi)

        return new_baseiscsi

    def create_test_sr(self, sr_cmd):
        self.sr_uuid = str(uuid4())
        self.subject = LVHDoISCSISR.LVHDoISCSISR(
            sr_cmd, self.sr_uuid)

    def test_check_sr_pbd_not_found(self):
        # Arrange
        self.mock_util.find_my_pbd.return_value = None
        self.create_test_sr(self.create_sr_command())

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
        self.create_test_sr(self.create_sr_command())

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
        self.create_test_sr(self.create_sr_command())

        # Act
        self.subject.check_sr(TEST_SR_UUID)

        # Assert
        self.mock_baseiscsi.attach.assert_called_with(
            TEST_SR_UUID
        )

    def test_sr_attach_multi_session(self):
        # Arrange
        self.mock_util.find_my_pbd.return_value = 'my_pbd'
        additional_dconf = {
            'multiSession': '10.207.6.60,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3393|'
                            '10.207.3.65,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3394|'
                            '10.207.3.61,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3393|'
                            '10.207.6.61,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3393|'
                            '10.207.3.63,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3394|'
                            '10.207.6.62,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3393|'
                            '10.207.3.62,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3393|'
                            '10.207.3.60,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3393|'
                            '10.207.6.64,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3394|'
                            '10.207.6.65,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3394|'
                            '10.207.3.64,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3394|'
                            '10.207.6.63,3260,iqn.2009-11.com.infinidat:storage:infinibox-sn-3394|'
        }

        tpg_data = [
            [
                ('10.207.3.60:3260', 1, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393'),
                ('10.207.3.61:3260', 1, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393'),
                ('10.207.3.62:3260', 1, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393')],
            [
                ('10.207.3.63:3260', 1, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3394'),
                ('10.207.3.64:3260', 1, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3394'),
                ('10.207.3.65:3260', 1, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3394')],
            [
                ('10.207.6.60:3260', 2, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393'),
                ('10.207.6.61:3260', 2, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393'),
                ('10.207.6.62:3260', 2, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393')
            ],
            [
                ('10.207.6.63:3260', 2, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3394'),
                ('10.207.6.64:3260', 2, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3394'),
                ('10.207.6.65:3260', 2, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3394')
            ]
        ]

        self.discovery_data = {
            '10.207.3.60': tpg_data[0],
            '10.207.3.61': tpg_data[0],
            '10.207.3.62': tpg_data[0],
            '10.207.3.63': tpg_data[1],
            '10.207.3.64': tpg_data[1],
            '10.207.3.65': tpg_data[1],
            '10.207.6.60': tpg_data[2],
            '10.207.6.61': tpg_data[2],
            '10.207.6.62': tpg_data[2],
            '10.207.6.63': tpg_data[3],
            '10.207.6.64': tpg_data[3],
            '10.207.6.65': tpg_data[3]
        }

        # Create SR
        self.create_test_sr(self.create_sr_command(
            additional_dconf=additional_dconf,
            cmd='sr_attach',
            target_iqn='*'))

        # Act
        self.subject.attach(TEST_SR_UUID)

        # Assert
        # print(f"iscsi calls {self.mock_iscsi.mock_calls}")
        attach_count = 0
        for sr in self.base_srs:
           attach_count += sr.attach.call_count

        self.assertEqual(12, attach_count)
        self.assertEqual(12, self.mock_iscsi.discovery.call_count)
        self.assertEqual(12, self.mock_iscsi.login.call_count)
