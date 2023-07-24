"""
Unit tests for the Base ISCSI SR
"""

import mock
import unittest
from uuid import uuid4

from BaseISCSI import BaseISCSISR
import SR
import SRCommand
from util import CommandException


class TestBaseISCSI(unittest.TestCase):

    def setUp(self):
        self.addCleanup(mock.patch.stopall)

        util_patcher = mock.patch('BaseISCSI.util', autospec=True)
        self.mock_util = util_patcher.start()
        self.mock_util.CommandException = CommandException

        self.mock_session = mock.MagicMock()
        xenapi_patcher = mock.patch('SR.XenAPI')
        mock_xenapi = xenapi_patcher.start()
        mock_xenapi.xapi_local.return_value = self.mock_session

        iscsilib_patcher = mock.patch('BaseISCSI.iscsilib', autospec=True)
        self.mock_iscsilib = iscsilib_patcher.start()

        copy_patcher = mock.patch('LVHDoISCSISR.SR.copy.deepcopy')
        self.mock_copy = copy_patcher.start()

        def deepcopy(to_copy):
            return to_copy

        self.mock_copy.side_effect = deepcopy

        dummy_cmd = mock.create_autospec(SRCommand)
        dummy_cmd.dconf = {
            'SCSIid': '3600a098038313577792450384a4a6275',
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

        self.subject = BaseISCSISR(
            dummy_cmd, self.sr_uuid
        )

    def setup_path_mocks(self):
        self.path_contents = {}
        exists_patcher = mock.patch('BaseISCSI.os.path.exists', autospec=True)
        self.mock_exists = exists_patcher.start()
        self.mock_exists.side_effect = self.exists
        listdir_patcher = mock.patch('BaseISCSI.os.listdir', autospec=True)
        mock_listdir = listdir_patcher.start()
        mock_listdir.side_effect = self.listdir

    def exists(self, path):
        print(f'checking existance of {path}')
        return path in self.path_contents

    def listdir(self, path):
        return self.path_contents[path]

    @mock.patch('BaseISCSI.BaseISCSISR._initPaths', autospec=True)
    def test_attach_tgt_present_path_found(self, mock_init_paths):
        # Arrange
        self.setup_path_mocks()
        self.path_contents.update(
            {'/dev/disk/by-scsid/3600a098038313577792450384a4a6275': ['sdb']})
        self.mock_util._testHost.return_value = None
        self.mock_util.sessions_less_than_targets.return_value = False
        self.mock_iscsilib._checkTGT.return_value = True
        self.mock_iscsilib.parse_IP_port.side_effect = [
            ('tgt1', '3260')
            ]

        # Act
        self.subject.attach(self.sr_uuid)

    @mock.patch('BaseISCSI.BaseISCSISR._initPaths', autospec=True)
    @mock.patch('BaseISCSI.xs_errors.XML_DEFS', "drivers/XE_SR_ERRORCODES.xml")
    def test_attach_tgt_present_path_not_found(self, mock_init_paths):
        # Arrange
        self.mock_util._testHost.return_value = None
        self.mock_util.sessions_less_than_targets.return_value = False
        self.mock_iscsilib._checkTGT.return_value = True
        self.mock_iscsilib.parse_IP_port.side_effect = [
            ('tgt1', '3260')
            ]

        # Act
        with self.assertRaises(SR.SROSError) as srose:
            self.subject.attach(self.sr_uuid)

        # Assert
        self.assertEqual(107, srose.exception.errno)
