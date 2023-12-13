"""
Unit tests for the Base ISCSI SR
"""

import mock
import unittest
from uuid import uuid4

import util
from BaseISCSI import BaseISCSISR
import SR
import SRCommand
from shared_iscsi_test_base import ISCSITestCase
from util import CommandException


class TestBaseISCSI(ISCSITestCase):

    TEST_CLASS = 'BaseISCSI'

    def setUp(self):
        self.addCleanup(mock.patch.stopall)

        util_patcher = mock.patch('BaseISCSI.util', autospec=True)
        self.mock_util = util_patcher.start()
        self.mock_util.CommandException = CommandException
        self.mock_util.sessions_less_than_targets = util.sessions_less_than_targets
        self.mock_util._convertDNS.side_effect = lambda x: x
        # self.mock_util.SMlog.side_effect = print

        scsi_util_patcher = mock.patch('BaseISCSI.scsiutil', autospec=True)
        self.mock_scsiutil = scsi_util_patcher.start()

        self.mock_session = mock.MagicMock()
        xenapi_patcher = mock.patch('SR.XenAPI')
        mock_xenapi = xenapi_patcher.start()
        mock_xenapi.xapi_local.return_value = self.mock_session

        copy_patcher = mock.patch('LVHDoISCSISR.SR.copy.deepcopy')
        self.mock_copy = copy_patcher.start()

        def deepcopy(to_copy):
            return to_copy

        self.mock_copy.side_effect = deepcopy

        self.sr_uuid = str(uuid4())

        super(TestBaseISCSI, self).setUp()

    def setup_path_mocks(self):
        self.path_contents = {}
        exists_patcher = mock.patch('BaseISCSI.os.path.exists', autospec=True)
        self.mock_exists = exists_patcher.start()
        self.mock_exists.side_effect = self.exists
        listdir_patcher = mock.patch('BaseISCSI.os.listdir', autospec=True)
        mock_listdir = listdir_patcher.start()
        mock_listdir.side_effect = self.listdir

    def exists(self, path):
        return path in self.path_contents

    def listdir(self, path):
        return self.path_contents[path]

    def create_test_sr(self, sr_cmd):
        self.sr_uuid = str(uuid4())
        self.subject = BaseISCSISR(
            sr_cmd, self.sr_uuid)

    @mock.patch('BaseISCSI.BaseISCSISR._initPaths', autospec=True)
    def test_attach_tgt_present_path_found(self, mock_init_paths):
        # Arrange
        self.setup_path_mocks()
        self.path_contents.update(
            {'/dev/disk/by-scsid/3600a098038313577792450384a4a6275': ['sdb']})
        self.mock_util._testHost.return_value = None
        self.discovery_data = {
            'tgt1': [
                ('tgt1:3260', 1, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393')],
        }

        self.create_test_sr(self.create_sr_command(
            cmd='sr_attach',
            target_iqn='iqn.2009-11.com.infinidat:storage:infinibox-sn-3393'))

        # Act
        self.subject.attach(self.sr_uuid)

    @mock.patch('BaseISCSI.BaseISCSISR._initPaths', autospec=True)
    @mock.patch('BaseISCSI.xs_errors.XML_DEFS', "drivers/XE_SR_ERRORCODES.xml")
    def test_attach_tgt_present_path_not_found(self, mock_init_paths):
        # Arrange
        self.mock_util._testHost.return_value = None
        self.discovery_data = {
            'tgt1': [
                ('tgt1:3260', 1, 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393')],
        }

        self.create_test_sr(self.create_sr_command(
            cmd='sr_attach',
            target_iqn='iqn.2009-11.com.infinidat:storage:infinibox-sn-3393'))

        # Act
        with self.assertRaises(SR.SROSError) as srose:
            self.subject.attach(self.sr_uuid)

        # Assert
        self.assertEqual(107, srose.exception.errno)

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

        self.mock_scsiutil._genHostList.return_value = [1, 2]
        self.mock_iscsilib.get_targetIQN.return_value = 'iqn.2009-11.com.infinidat:storage:infinibox-sn-3393'
        self.mock_scsiutil.cacheSCSIidentifiers.return_value = [
            ['NONE', '0', '0', '0', '0', '0', '/dev/sdb']
        ]
        self.setup_path_mocks()
        self.path_contents.update(
            {'/dev/iscsi/iqn.2009-11.com.infinidat:storage:infinibox-sn-3393/10.207.3.60:3260': ['LUN0'],
             '/dev/disk/by-scsid/3600a098038313577792450384a4a6275': []})

        # Create SR
        self.create_test_sr(self.create_sr_command(
            additional_dconf=additional_dconf,
            cmd='sr_attach',
            multihomelist="10.207.3.62:3260,10.207.6.61:3260,10.207.6.62:3260,10.207.6.60:3260",
            target='10.207.3.60',
            target_iqn='iqn.2009-11.com.infinidat:storage:infinibox-sn-3393'))

        # Act
        self.subject.attach(self.sr_uuid)

        # Assert
        self.assertEqual(1, self.mock_iscsilib.discovery.call_count)
        self.assertEqual(1, self.mock_iscsilib.login.call_count)
