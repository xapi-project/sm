import errno
import os
import stat
import unittest
import unittest.mock as mock
import uuid
import xmlrpc.client

from xml.dom.minidom import parseString

import FileSR
import SR
import SRCommand
import util
import vhdutil


class FakeFileVDI(FileSR.FileVDI):
    def load(self, uuid):
        self.vdi_type = vhdutil.VDI_TYPE_VHD
        self.hidden = False
        self.path = os.path.join(self.sr.path, '%s.%s' % (
               uuid, vhdutil.VDI_TYPE_VHD))
        self.key_hash = None


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
        sr._check_hardlinks.return_value = False
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

    @mock.patch('FileSR.vhdutil', spec=True)
    def test_create_vdi_vhd(self, mock_vhdutil):
        # Arrange
        mock_vhdutil.VDI_TYPE_VHD = vhdutil.VDI_TYPE_VHD
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, vdi_uuid)
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD
        mock_vhdutil.validate_and_round_vhd_size.side_effect = vhdutil.validate_and_round_vhd_size

        # Act
        vdi.create(sr_uuid, vdi_uuid, 20 * 1024 * 1024)

        # Assert
        expected_path = f"sr_path/{vdi_uuid}.vhd"
        self.mock_pread.assert_has_calls([
            mock.call(["/usr/sbin/td-util", "create", "vhd",
                       "20", expected_path]),
            mock.call(["/usr/sbin/td-util", "query", "vhd", "-v",
                       expected_path])])

    @mock.patch('FileSR.vhdutil', spec=True)
    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_create_vdi_raw(self, mock_open, mock_vhdutil):
        # Arrange
        mock_vhdutil.VDI_TYPE_RAW = vhdutil.VDI_TYPE_RAW
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi = FakeFileVDI(sr, vdi_uuid)
        vdi.vdi_type = vhdutil.VDI_TYPE_RAW

        # Act
        vdi.create(sr_uuid, vdi_uuid, 20 * 1024 * 1024)

        # Assert
        expected_path = f"sr_path/{vdi_uuid}.vhd"
        mock_open.assert_called_with(expected_path, 'w')

    @mock.patch("FileSR.util.pathexists", autospec=True)
    @mock.patch("FileSR.os.chdir", autospec=True)
    def test_vdi_load_vhd(self, mock_chdir, mock_pathexists):
        # Arrange
        self.mock_pread.return_value = """10240
/dev/VG_XenStorage-602fa2e9-2f9e-84af-ac1d-de4616cdcccb/VHD-155a6d00-2f70-411f-9bc7-3fa51fa543ca has no parent
hidden: 0
"""
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        srcmd = mock.MagicMock(SRCommand)
        srcmd.cmd = "vdi_create"
        srcmd.dconf = {}
        srcmd.params = {
            'command': 'vdi_create'
        }
        sr = FakeSharedFileSR(srcmd, sr_uuid)
        vdi = FileSR.FileVDI(sr, vdi_uuid)
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD
        mock_pathexists.return_value = True

        # Act
        vdi.load(vdi_uuid)

        # Assert
        sr_path = f"/var/run/sr-mount/{sr_uuid}"
        mock_chdir.assert_has_calls([
            mock.call(sr_path),
            mock.call(sr_path)])

    @mock.patch("FileSR.util.pathexists", autospec=True)
    def test_vdi_generate_config(self, mock_pathexists):
        # Arrange
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        srcmd = mock.MagicMock(SRCommand)
        srcmd.cmd = "vdi_create"
        srcmd.dconf = {}
        srcmd.params = {
            'command': 'vdi_create'
        }
        sr = FakeSharedFileSR(srcmd, sr_uuid)
        vdi = FakeFileVDI(sr, vdi_uuid)
        mock_pathexists.return_value = True

        # Act
        response = vdi.generate_config(sr_uuid, vdi_uuid)

        # Assert
        xml_response = xmlrpc.client.loads(response)
        self.assertIsNotNone(xml_response)
        config = xmlrpc.client.loads(xml_response[0][0])[0][0]
        self.assertEqual(sr_uuid, config['sr_uuid'])
        self.assertEqual(vdi_uuid, config['vdi_uuid'])
        self.assertEqual("vdi_attach_from_config",
                         config['command'])


class FakeSharedFileSR(FileSR.SharedFileSR):
    """
    Test SR class for SharedFileSR
    """
    def load(self, sr_uuid):
        self.path = os.path.join(SR.MOUNT_BASE, sr_uuid)
        self.lock = None

    def attach(self, sr_uuid):
        self._check_hardlinks()

    def _read_hardlink_conf(self):
        return None

class TestShareFileSR(unittest.TestCase):
    """
    Tests for common Shared File SR operations
    """
    TEST_SR_REF = "test_sr_ref"
    ERROR_524 = "Unknown error 524"

    def setUp(self):
        util_patcher = mock.patch('FileSR.util', autospec=True)
        self.mock_util = util_patcher.start()

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

        vhdutil_patcher = mock.patch('FileSR.vhdutil', autospec=True)
        self.mock_vhdutil = vhdutil_patcher.start()
        glob_patcher = mock.patch("FileSR.glob", autospec=True)
        self.mock_glob = glob_patcher.start()

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
        self.assertEqual(0, self.mock_session.xenapi.message.create.call_count)

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
        self.mock_session.xenapi.message.create.assert_called_with(
            'sr_does_not_support_hardlinks', 2, "SR", self.sr_uuid, mock.ANY)

    def test_attach_fist_active(self):
        """
        Attach SR with FIST point active to set no hardlinks
        """
        # Arrange
        test_sr = self.create_test_sr()
        self.mock_util.fistpoint.activate_custom_fn.side_effect = OSError(524, TestShareFileSR.ERROR_524)

        # Act
        with mock.patch('FileSR.open'):
            test_sr.attach(self.sr_uuid)

        # Assert
        self.mock_session.xenapi.message.create.assert_called_with(
            'sr_does_not_support_hardlinks', 2, "SR", self.sr_uuid, mock.ANY)

    def test_scan_load_vdis_scan_list_differ(self):
        """
            Load the SR VDIs
        """
        # Arrange
        test_sr = self.create_test_sr()

        self.mock_util.pathexists.return_value = True
        self.mock_util.ismount.return_value = True

        vdi1_uuid = str(uuid.uuid4())
        vdi1_info = vhdutil.VHDInfo(vdi1_uuid)
        vdi1_info.error = False
        vdi1_info.path = f"sr_path/{vdi1_uuid}.vhd"
        test_vhds = {
            vdi1_uuid: vdi1_info
        }
        self.mock_glob.glob.return_value = []

        self.mock_vhdutil.getAllVHDs.return_value = test_vhds

        # Act
        test_sr.scan(self.sr_uuid)

        # Assert
        self.assertEqual(1, len(test_sr.vdis))
