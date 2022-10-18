import errno
import os
import stat
import unittest
import unittest.mock as mock
import uuid
from builtins import str
from xml.dom.minidom import parseString

import FileSR
import SR
import util
import vhdutil
from XenAPI import Failure


class FakeFileVDI(FileSR.FileVDI):
    def load(self, uuid):
        self.vdi_type = vhdutil.VDI_TYPE_VHD
        self.hidden = False
        self.path = os.path.join(self.sr.path, '%s.%s' % (
               uuid, vhdutil.VDI_TYPE_VHD))


class TestFileVDI(unittest.TestCase):
    def setUp(self):
        startlog_patcher = mock.patch('FileSR.util.start_log_entry',
                                        autospec=True)
        self.mock_startlog = startlog_patcher.start()
        endlog_patcher = mock.patch('FileSR.util.end_log_entry',
                                      autospec=True)
        self.mock_endlog = endlog_patcher.start()
        os_link_patcher = mock.patch('FileSR.os.link', autospec=True)
        self.mock_os_link = os_link_patcher.start()
        os_stat_patcher = mock.patch('FileSR.os.stat')
        self.mock_os_stat = os_stat_patcher.start()
        os_rename_patcher = mock.patch('FileSR.os.rename', autospec=True)
        self.mock_os_rename = os_rename_patcher.start()
        os_unlink_patcher = mock.patch('FileSR.os.unlink', autospec=True)
        self.mock_os_unlink = os_unlink_patcher.start()
        pread_patcher = mock.patch('FileSR.util.pread')
        self.mock_pread = pread_patcher.start()
        gethidden_patch = mock.patch('FileSR.vhdutil.getHidden')
        self.mock_gethidden = gethidden_patch.start()

        errors_patcher = mock.patch('FileSR.xs_errors.XML_DEFS',
                "drivers/XE_SR_ERRORCODES.xml")
        errors_patcher.start()

        fist_patcher = mock.patch('FileSR.util.FistPoint.is_active',
                                  autospec=True)
        self.mock_fist = fist_patcher.start()
        self.active_fists = set()
        def active_fists():
            return self.active_fists

        def is_active(self, name):
            return name in active_fists()

        self.mock_fist.side_effect = is_active

        self.addCleanup(mock.patch.stopall)

    @mock.patch('os.lstat', autospec=True)
    def test_find_vhd_path(self, mock_os_stat):
        vdi_uuid = uuid.uuid4()
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, None)
        vdi.sr = sr
        mock_os_stat.side_effect = [os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 1024, 0, 0, 0))]

        found = vdi._find_path_with_retries(vdi_uuid)

        self.assertTrue(found)
        expected_path = 'sr_path/%s.vhd' % vdi_uuid
        mock_os_stat.assert_called_with(expected_path)
        self.assertEqual(vdi.path, expected_path)

    @mock.patch('os.lstat', autospec=True)
    def test_find_raw_path(self, mock_os_stat):
        vdi_uuid = uuid.uuid4()
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, None)
        vdi.sr = sr
        mock_os_stat.side_effect = [OSError(errno.ENOENT),
                                     os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 1024, 0, 0, 0))]

        found = vdi._find_path_with_retries(vdi_uuid)

        self.assertTrue(found)
        expected_path = 'sr_path/%s.raw' % vdi_uuid
        mock_os_stat.assert_called_with(expected_path)
        self.assertEqual(vdi.path, expected_path)

    @mock.patch('time.sleep', autospec=True)
    @mock.patch('os.lstat', autospec=True)
    def test_find_retry_vhd_path(self, mock_os_stat, sleep):
        vdi_uuid = uuid.uuid4()
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, None)
        vdi.sr = sr
        mock_os_stat.side_effect = [OSError(errno.ENOENT),
                                     OSError(errno.ENOENT),
                                     OSError(errno.ENOENT),
                                     os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 1024, 0, 0, 0))]

        found = vdi._find_path_with_retries(vdi_uuid)

        self.assertTrue(found)
        expected_path = 'sr_path/%s.vhd' % vdi_uuid
        mock_os_stat.assert_called_with(expected_path)
        self.assertEqual(vdi.path, expected_path)

    @mock.patch('time.sleep', autospec=True)
    def test_find_not_found(self, sleep):
        vdi_uuid = uuid.uuid4()
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, None)
        vdi.sr = sr
        self.mock_os_stat.side_effect = OSError(errno.ENOENT)

        found = vdi._find_path_with_retries(vdi_uuid)

        self.assertFalse(found)

    def decode_vdi_xml(self, vdi_xml):
        vdi = parseString(vdi_xml)

        return {
               x.getElementsByTagName('name')[0].firstChild.nodeValue:
               x.getElementsByTagName('value')[0].getElementsByTagName(
                    'string')[0].firstChild.nodeValue
               for x in vdi.getElementsByTagName('member')}

    @mock.patch('FileSR.util.gen_uuid')
    @mock.patch('FileSR.FileVDI._query_p_uuid')
    @mock.patch('FileSR.util.pathexists', autospec=True)
    @mock.patch('FileSR.vhdutil.getDepth', autospec=True)
    @mock.patch('FileSR.blktap2', autospec=True)
    def test_clone_success(self, mock_blktap, mock_getDepth, mock_pathexists,
                            mock_query_p_uuid, mock_uuid):
        # Arrange
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, vdi_uuid)
        vdi.sr = sr

        mock_getDepth.return_value = 1
        mock_pathexists.return_value = True
        new_vdi_uuid = str(uuid.uuid4())
        clone_vdi_uuid = str(uuid.uuid4())
        mock_uuid.side_effect = [clone_vdi_uuid, new_vdi_uuid]
        grandp_uuid = str(uuid.uuid4())

        mock_query_p_uuid.side_effect = [new_vdi_uuid, new_vdi_uuid, grandp_uuid]

        # Act
        clone_xml = vdi.clone(sr_uuid, vdi_uuid)

        # Assert
        clone_vdi = self.decode_vdi_xml(clone_xml)

        self.assertEqual(clone_vdi_uuid, clone_vdi['uuid'])
        self.mock_os_link.assert_called_with(
               'sr_path/%s.vhd' % vdi_uuid,
               'sr_path/%s.vhd' % new_vdi_uuid)
        self.assertEqual(0, self.mock_os_unlink.call_count)
        self.mock_os_rename.assert_has_calls([
               mock.call('sr_path/%s.vhd.new' % vdi_uuid,
                         'sr_path/%s.vhd' % vdi_uuid)])

    @mock.patch('FileSR.util.gen_uuid')
    @mock.patch('FileSR.FileVDI._query_p_uuid')
    @mock.patch('FileSR.util.pathexists', autospec=True)
    @mock.patch('FileSR.vhdutil.getDepth', autospec=True)
    @mock.patch('FileSR.blktap2', autospec=True)
    def test_clone_no_links_success(
            self, mock_blktap, mock_getDepth, mock_pathexists,
            mock_query_p_uuid, mock_uuid):
        # Arrange
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, vdi_uuid)
        vdi.sr = sr

        mock_getDepth.return_value = 1
        mock_pathexists.return_value = True
        new_vdi_uuid = str(uuid.uuid4())
        clone_vdi_uuid = str(uuid.uuid4())
        mock_uuid.side_effect = [clone_vdi_uuid, new_vdi_uuid]
        grandp_uuid = str(uuid.uuid4())

        mock_query_p_uuid.side_effect = [new_vdi_uuid, new_vdi_uuid, grandp_uuid]

        sr.session.xenapi.SR.get_sm_config.return_value = {
            "no_hardlinks": "True"}

        # Act
        clone_xml = vdi.clone(sr_uuid, vdi_uuid)

        # Assert
        clone_vdi = self.decode_vdi_xml(clone_xml)

        self.assertEqual(clone_vdi_uuid, clone_vdi['uuid'])
        self.assertEqual(0, self.mock_os_link.call_count)
        self.assertEqual(0, self.mock_os_unlink.call_count)
        self.mock_os_rename.assert_has_calls([
            mock.call('sr_path/%s.vhd' % vdi_uuid,
                      'sr_path/%s.vhd' % new_vdi_uuid),
            mock.call('sr_path/%s.vhd.new' % vdi_uuid,
                      'sr_path/%s.vhd' % vdi_uuid)])

    @mock.patch('FileSR.FileVDI._snap')
    @mock.patch('FileSR.util.gen_uuid')
    @mock.patch('FileSR.FileVDI._query_p_uuid')
    @mock.patch('FileSR.util.pathexists', autospec=True)
    @mock.patch('FileSR.vhdutil.getDepth', autospec=True)
    @mock.patch('FileSR.blktap2', autospec=True)
    def test_clone_nospace_snap_1(
               self, mock_blktap, mock_getDepth, mock_pathexists,
               mock_query_p_uuid, mock_uuid, mock_snap):
        # Arrange
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, vdi_uuid)
        vdi.sr = sr

        mock_getDepth.return_value = 1
        mock_pathexists.return_value = True
        new_vdi_uuid = str(uuid.uuid4())
        clone_vdi_uuid = str(uuid.uuid4())
        mock_uuid.side_effect = [clone_vdi_uuid, new_vdi_uuid]
        grandp_uuid = str(uuid.uuid4())

        mock_snap.side_effect = [util.CommandException(errno.ENOSPC)]

        real_stat = os.stat

        def my_stat(tgt):
            if tgt.endswith('.vhd'):
                return os.stat_result((stat.S_IFREG, 0, 0, 2, 0, 0, 1024, 0, 0, 0))
            return real_stat(tgt)

        # Act
        with self.assertRaises(SR.SROSError) as srose, mock.patch('FileSR.os.stat') as mock_stat:
            mock_stat.side_effect = my_stat
            clone_xml = vdi.clone(sr_uuid, vdi_uuid)

        # Assert
        self.mock_os_link.assert_called_with(
               'sr_path/%s.vhd' % vdi_uuid,
               'sr_path/%s.vhd' % new_vdi_uuid)
        self.assertEqual(2, self.mock_os_unlink.call_count)
        self.assertEqual(1, mock_snap.call_count)
        self.assertEqual(0, self.mock_os_rename.call_count)

    @mock.patch('FileSR.FileVDI._snap')
    @mock.patch('FileSR.util.gen_uuid')
    @mock.patch('FileSR.FileVDI._query_p_uuid')
    @mock.patch('FileSR.util.pathexists', autospec=True)
    @mock.patch('FileSR.vhdutil.getDepth', autospec=True)
    @mock.patch('FileSR.blktap2', autospec=True)
    def test_clone_nospace_snap_2(
               self, mock_blktap, mock_getDepth, mock_pathexists,
               mock_query_p_uuid, mock_uuid, mock_snap):
        # Arrange
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, vdi_uuid)
        vdi.sr = sr

        mock_getDepth.return_value = 1
        mock_pathexists.return_value = True
        new_vdi_uuid = str(uuid.uuid4())
        clone_vdi_uuid = str(uuid.uuid4())
        mock_uuid.side_effect = [clone_vdi_uuid, new_vdi_uuid]
        grandp_uuid = str(uuid.uuid4())

        mock_snap.side_effect = [None, util.CommandException(errno.ENOSPC)]
        self.mock_gethidden.return_value = False

        real_stat = os.stat

        def my_stat(tgt):
            if tgt.endswith('.vhd'):
                return os.stat_result((stat.S_IFREG, 0, 0, 1, 0, 0, 1024, 0, 0, 0))
            return real_stat(tgt)

        # Act
        with self.assertRaises(SR.SROSError) as srose, mock.patch('FileSR.os.stat') as mock_stat:
            mock_stat.side_effect = my_stat
            clone_xml = vdi.clone(sr_uuid, vdi_uuid)

        # Assert
        self.mock_os_link.assert_called_with(
               'sr_path/%s.vhd' % vdi_uuid,
               'sr_path/%s.vhd' % new_vdi_uuid)
        self.assertEqual(1, self.mock_os_unlink.call_count)
        self.assertEqual(2, mock_snap.call_count)
        self.mock_os_rename.assert_has_calls([
               mock.call('sr_path/%s.vhd.new' % vdi_uuid,
                         'sr_path/%s.vhd' % vdi_uuid),
               mock.call('sr_path/%s.vhd' % new_vdi_uuid,
                         'sr_path/%s.vhd' % vdi_uuid)])


class FakeSharedFileSR(FileSR.SharedFileSR):
    """
    Test SR class for SharedFileSR
    """
    def load(self, sr_uuid):
        self.path = os.path.join(SR.MOUNT_BASE, sr_uuid)

    def attach(self, sr_uuid):
        self._check_hardlinks()


class TestShareFileSR(unittest.TestCase):
    """
    Tests for common Shared File SR operations
    """
    TEST_SR_REF = "test_sr_ref"
    ERROR_524 = "Unknown error 524"
    NO_HARDLINKS = "no_hardlinks"

    def setUp(self):
        fist_patcher = mock.patch('FileSR.util.FistPoint.is_active',
                                  autospec=True)
        self.mock_fist = fist_patcher.start()

        self.active_fists = set()
        def active_fists():
            return self.active_fists

        def is_active(self, name):
            return name in active_fists()

        self.mock_fist.side_effect = is_active

        link_patcher = mock.patch('FileSR.os.link')
        self.mock_link = link_patcher.start()

        unlink_patcher = mock.patch('FileSR.util.force_unlink')
        self.mock_unlink = unlink_patcher.start()

        lock_patcher = mock.patch('FileSR.Lock')
        self.mock_lock = lock_patcher.start()

        xapi_patcher = mock.patch('SR.XenAPI')
        self.mock_xapi = xapi_patcher.start()
        self.mock_session = mock.MagicMock()
        self.mock_xapi.xapi_local.return_value = self.mock_session

        self.session_ref = "dummy_session"

        self.addCleanup(mock.patch.stopall)

        self.sr_uuid = str(uuid.uuid4())

    def create_test_sr(self):
        srcmd = mock.Mock()
        srcmd.dconf = {}
        srcmd.params = {'command': "some_command",
                        'session_ref': self.session_ref,
                        'sr_ref': TestShareFileSR.TEST_SR_REF}
        return FakeSharedFileSR(srcmd, self.sr_uuid)

    def test_attach_success(self):
        """
        Attach SR on FS with expected features
        """
        test_sr = self.create_test_sr()

        with mock.patch('FileSR.open'):
            test_sr.attach(self.sr_uuid)

        # Assert
        self.mock_session.xenapi.SR.remove_from_sm_config.assert_called_with(
            TestShareFileSR.TEST_SR_REF, TestShareFileSR.NO_HARDLINKS)

    def test_attach_link_fail(self):
        """
        Attach SR on FS with no hardlinks
        """
        test_sr = self.create_test_sr()

        self.mock_link.side_effect = OSError(524, TestShareFileSR.ERROR_524)

        # Act
        with mock.patch('FileSR.open'):
            test_sr.attach(self.sr_uuid)

        # Assert
        self.mock_session.xenapi.SR.add_to_sm_config.assert_called_with(
            TestShareFileSR.TEST_SR_REF, TestShareFileSR.NO_HARDLINKS, 'True')
        self.mock_session.xenapi.message.create.assert_called_with(
            'sr_does_not_support_hardlinks', 2, "SR", self.sr_uuid, mock.ANY)

    def test_attach_link_fail_already_set(self):
        """
        Attach SR on FS with no hardlinks with config set
        """
        test_sr = self.create_test_sr()

        self.mock_link.side_effect = OSError(524, TestShareFileSR.ERROR_524)
        self.mock_session.xenapi.SR.add_to_sm_config.side_effect = Failure(
            ['MAP_DUPLICATE_KEY', 'SR', 'sm_config',
            'OpaqueRef:be8cc595-4924-4946-9082-59aef531daae',
             TestShareFileSR.NO_HARDLINKS])

        # Act
        with mock.patch('FileSR.open'):
            test_sr.attach(self.sr_uuid)

        # Assert
        self.mock_session.xenapi.SR.add_to_sm_config.assert_called_with(
            TestShareFileSR.TEST_SR_REF, TestShareFileSR.NO_HARDLINKS, 'True')

    def test_attach_fist_active(self):
        """
        Attach SR with FIST point active to set no hardlinks
        """
        # Arrange
        test_sr = self.create_test_sr()
        self.active_fists.add('FileSR_fail_hardlink')

        self.mock_link.side_effect = OSError(524, TestShareFileSR.ERROR_524)

        # Act
        with mock.patch('FileSR.open'):
            test_sr.attach(self.sr_uuid)

        # Assert
        self.mock_session.xenapi.SR.add_to_sm_config.assert_called_with(
            TestShareFileSR.TEST_SR_REF, TestShareFileSR.NO_HARDLINKS, 'True')
        self.mock_session.xenapi.message.create.assert_called_with(
            'sr_does_not_support_hardlinks', 2, "SR", self.sr_uuid, mock.ANY)
