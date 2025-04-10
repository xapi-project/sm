import unittest
import unittest.mock as mock
import uuid

import SR
import SMBSR
import testlib
from sm.core import util
import errno
import XenAPI
from sm.core import xs_errors


class FakeSMBSR(SMBSR.SMBSR):
    uuid = None
    sr_ref = None
    mountpoint = None
    linkpath = None
    path = None
    session = None
    remoteserver = None

    def __init__(self, srcmd, none):
        self.dconf = srcmd.dconf
        self.srcmd = srcmd
        self.uuid = 'auuid'
        self.sr_ref = 'asr_ref'
        self.mountpoint = 'aMountpoint'
        self.linkpath = 'aLinkpath'
        self.path = 'aPath'
        self.remoteserver = 'aRemoteserver'


@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
class Test_SMBSR(unittest.TestCase):

    def setUp(self):
        self.addCleanup(mock.patch.stopall)

        pread_patcher = mock.patch('SMBSR.util.pread', autospec=True)
        self.mock_pread = pread_patcher.start()
        self.mock_pread.side_effect = self.pread
        self.pread_results = {}

        listdir_patcher = mock.patch('SMBSR.util.listdir', autospec=True)
        self.mock_list_dir = listdir_patcher.start()

        rmdir_patcher = mock.patch('SMBSR.os.rmdir', autospec=True)
        self.mock_rmdir = rmdir_patcher.start()

    def arg_key(self, args):
        return ':'.join(args)

    def pread(self, args, new_env=None, quiet=False, text=False):
        return self.pread_results.get(self.arg_key(args), None)

    def create_smbsr(self, sr_uuid='asr_uuid', server='\\aServer', serverpath='/aServerpath', username='aUsername', password='aPassword', dconf_update={}):
        srcmd = mock.Mock()
        srcmd.dconf = {
            'server': server,
            'serverpath': serverpath,
            'username': username,
            'password': password
        }
        srcmd.params = {
            'command': 'some_command',
            'device_config': {}
        }
        srcmd.dconf.update(dconf_update)
        smbsr = FakeSMBSR(srcmd, None)
        smbsr.load(sr_uuid)
        return smbsr

    #Attach
    @mock.patch('SMBSR.SMBSR.checkmount', autospec=True)
    @mock.patch('SMBSR.SMBSR.mount', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    def test_attach_smbexception_raises_xenerror(
            self, mock_lock, mock_mount, mock_checkmount):
        smbsr = self.create_smbsr()
        mock_mount.side_effect = SMBSR.SMBException("mount raised SMBException")
        mock_checkmount.return_value = False
        with self.assertRaises(xs_errors.SROSError) as cm:
            smbsr.attach('asr_uuid')
        # Check that we get the SMBMount error from XE_SR_ERRORCODES.xml
        self.assertEqual(cm.exception.errno, 111)

    @mock.patch('SMBSR.SMBSR.checkmount', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    def test_attach_if_mounted_then_attached(self, mock_lock, mock_checkmount):
        smbsr = self.create_smbsr()
        mock_checkmount.return_value = True
        smbsr.attach('asr_uuid')
        self.assertTrue(smbsr.attached)

    @mock.patch('FileSR.SharedFileSR._check_writable', autospec=True)
    @mock.patch('FileSR.SharedFileSR._check_hardlinks', autospec=True)
    @mock.patch('SMBSR.SMBSR.checkmount', autospec=True)
    @mock.patch('SMBSR.SMBSR.makeMountPoint', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    @mock.patch('os.symlink', autospec=True)
    def test_attach_vanilla(self, symlink, mock_lock,
                            makeMountPoint, mock_checkmount, mock_checklinks,
                            mock_checkwritable):
        mock_checkmount.return_value = False
        smbsr = self.create_smbsr()
        makeMountPoint.return_value = "/var/mount"
        smbsr.attach('asr_uuid')
        self.assertTrue(smbsr.attached)
        self.mock_pread.assert_called_with(
            ['mount.cifs', '\\aServer', "/var/mount", '-o', 'cache=loose,vers=3.0,actimeo=0'],
            new_env={'PASSWD': 'aPassword', 'USER': 'aUsername'})

    @mock.patch('FileSR.SharedFileSR._check_writable', autospec=True)
    @mock.patch('FileSR.SharedFileSR._check_hardlinks', autospec=True)
    @mock.patch('SMBSR.SMBSR.checkmount', autospec=True)
    @mock.patch('SMBSR.SMBSR.makeMountPoint', autospec=True)
    @mock.patch('SMBSR.Lock', autospecd=True)
    @mock.patch('os.symlink', autospec=True)
    def test_attach_with_cifs_password(
            self, symlink, mock_lock, makeMountPoint,
            mock_checkmount, mock_checklinks, mock_checkwritable):
        smbsr = self.create_smbsr(dconf_update={"password": "winter2019"})
        mock_checkmount.return_value = False
        makeMountPoint.return_value = "/var/mount"
        smbsr.attach('asr_uuid')
        self.assertTrue(smbsr.attached)
        self.mock_pread.assert_called_with(['mount.cifs', '\\aServer', "/var/mount", '-o', 'cache=loose,vers=3.0,actimeo=0'], new_env={'PASSWD': 'winter2019', 'USER': 'aUsername'})

    @mock.patch('FileSR.SharedFileSR._check_writable', autospec=True)
    @mock.patch('FileSR.SharedFileSR._check_hardlinks', autospec=True)
    @mock.patch('SMBSR.SMBSR.checkmount', autospec=True)
    @mock.patch('SMBSR.SMBSR.makeMountPoint', autospec=True)
    @mock.patch('SMBSR.Lock', autospecd=True)
    @mock.patch('os.symlink', autospec=True)
    def test_attach_with_cifs_password_and_domain(
            self, symlink, mock_lock, makeMountPoint,
            mock_checkmount, mock_checklinks, mock_checkwritable):
        smbsr = self.create_smbsr(username="citrix\jsmith", dconf_update={"password": "winter2019"})
        mock_checkmount.return_value = False
        makeMountPoint.return_value = "/var/mount"
        smbsr.attach('asr_uuid')
        self.assertTrue(smbsr.attached)
        # We mocked listdir as this calls pread and assert_called_with only records the last call.
        self.mock_pread.assert_called_with(['mount.cifs', '\\aServer', "/var/mount", '-o', 'cache=loose,vers=3.0,actimeo=0,domain=citrix'], new_env={'PASSWD': 'winter2019', 'USER': 'jsmith'})

    @mock.patch('FileSR.SharedFileSR._check_writable', autospec=True)
    @mock.patch('FileSR.SharedFileSR._check_hardlinks', autospec=True)
    @mock.patch('SMBSR.SMBSR.checkmount', autospec=True)
    @mock.patch('SMBSR.SMBSR.mount', autospec=True)
    @mock.patch('SMBSR.SMBSR.unmount', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    @mock.patch('os.symlink', autospec=True)
    @mock.patch('os.unlink', autospec=True)
    @mock.patch('SMBSR.util.pathexists', autospec=True)
    def test_attach_not_writable(self, mock_pathexists, mock_unlink,
                                 mock_symlink, mock_lock, mock_unmount,
                                 mock_mount, mock_checkmount, mock_checklinks,
                                 mock_checkwritable):
        events = []

        def is_mounted():
            return len([ev for ev in events if ev.endswith("mount")]) % 2 == 1

        def fake_mount(*args, **kwargs):
            assert not is_mounted()
            events.append("mount")

        def fake_unmount(*args, **kwargs):
            assert is_mounted()
            events.append("unmount")

        def link_exists():
            return len([ev for ev in events if ev.endswith("link")]) % 2 == 1

        def fake_symlink(*args, **kwargs):
            assert not link_exists()
            events.append("symlink")

        def fake_unlink(*args, **kwargs):
            assert link_exists()
            events.append("unlink")

        mock_checkmount.side_effect = lambda *args: is_mounted()
        mock_mount.side_effect = fake_mount
        mock_unmount.side_effect = fake_unmount
        mock_pathexists.side_effect = lambda *args: link_exists()
        mock_symlink.side_effect = fake_symlink
        mock_unlink.side_effect = fake_unlink
        mock_checkwritable.side_effect = xs_errors.SRException("aFailure")

        smbsr = self.create_smbsr()
        with self.assertRaises(xs_errors.SRException):
            smbsr.attach('asr_uuid')
        mock_mount.assert_called_once()
        mock_unmount.assert_called_once_with(smbsr, smbsr.mountpoint, True)
        mock_unlink.assert_called_once_with(smbsr.path)

    @mock.patch('SMBSR.SMBSR.checkmount', autospec=True)
    @mock.patch('SMBSR.SMBSR.mount', autospec=True)
    @mock.patch('SMBSR.SMBSR.unmount', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    @mock.patch('os.unlink', autospec=True)
    @mock.patch('SMBSR.util.pathexists', autospec=True)
    def test_attach_misc_mount_failure(self, mock_pathexists, mock_unlink,
                                       mock_lock, mock_unmount,
                                       mock_mount, mock_checkmount):
        mock_mount.side_effect = KeyError
        mock_checkmount.return_value = False
        mock_pathexists.return_value = False

        smbsr = self.create_smbsr()
        with self.assertRaises(KeyError):
            smbsr.attach('asr_uuid')

        mock_unmount.assert_not_called()
        mock_unlink.assert_not_called()

    #Detach
    @mock.patch('SMBSR.SMBSR.checkmount', return_value=True, autospec=True)
    @mock.patch('SMBSR.SMBSR.unmount', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    @mock.patch('SMBSR.os.chdir', autospec=True)
    @mock.patch('SMBSR.cleanup', autospec=True)
    def test_detach_smbexception_raises_xenerror(
            self, mock_cleanup, mock_chdir, mock_lock,
            mock_unmount, mock_checkmount):
        smbsr = self.create_smbsr()
        mock_unmount.side_effect = SMBSR.SMBException("unmount raised SMBException")
        with self.assertRaises(xs_errors.SROSError) as cm:
            smbsr.detach('asr_uuid')
        # Check that we get the SMBUnMount error from XE_SR_ERRORCODES.xml
        self.assertEqual(cm.exception.errno, 112)

    @mock.patch('SMBSR.SMBSR.checkmount', return_value=False, autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    def test_detach_not_detached_if_not_mounted(self, mock_lock, mock_checkmount):
        smbsr = self.create_smbsr()
        smbsr.attached = True
        mock_checkmount.return_value = False
        smbsr.detach('asr_uuid')
        self.assertTrue(smbsr.attached)

    #Mount
    @mock.patch('SMBSR.util.isdir', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    @mock.patch('SMBSR.util.time', autospec=True)
    def test_mount_mountpoint_isdir(self, mock_time, mock_lock, mock_isdir):
        # Not sure that the code rerying in an ioretry loop in the case of a
        # missing dir is correct?
        mock_isdir.side_effect = util.CommandException(
            errno.EIO, "Not a directory")
        smbsr = self.create_smbsr()
        with self.assertRaises(SMBSR.SMBException) as cm:
            smbsr.mount()

    @mock.patch('SMBSR.Lock', autospec=True)
    def test_mount_mountpoint_empty_string(self, mock_lock):
        smbsr = self.create_smbsr()
        self.assertRaises(SMBSR.SMBException, smbsr.mount, "")

    @mock.patch('SMBSR.util.makedirs', autospec=True)
    @mock.patch('SMBSR.util.get_pool_restrictions', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    @mock.patch('SMBSR.os.symlink', autospec=True)
    def test_create_success(self, symlink, lock, restrict, makedirs):
        # Arrange
        smbsr = self.create_smbsr()
        smbsr.session = mock.MagicMock(spec=XenAPI)
        restrict.return_value = []
        sr_uuid = str(uuid.uuid4())
        self.mock_list_dir.return_value = []

        # Act
        smbsr.create(sr_uuid, 10 * 1024 * 1024 * 1024)

        # Assert
        self.mock_pread.assert_called_with(
            ['mount.cifs', '\\aServer', "/run/sr-mount/SMB/Server/asr_uuid",
             '-o', 'cache=loose,vers=3.0,actimeo=0'],
            new_env={'USER': 'aUsername', 'PASSWD': 'aPassword'})

    @mock.patch('SMBSR.util.makedirs', autospec=True)
    @mock.patch('SMBSR.util.get_pool_restrictions', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    @mock.patch('SMBSR.os.symlink', autospec=True)
    def test_create_read_only(self, symlink, lock, restrict, makedirs):
        # Arrange
        smbsr = self.create_smbsr()
        smbsr.session = mock.MagicMock(spec=XenAPI)
        restrict.return_value = []
        sr_uuid = str(uuid.uuid4())
        self.mock_list_dir.return_value = []

        def mock_makedirs(path):
            if path == '/run/sr-mount/SMB/Server/asr_uuid':
                return

            raise util.CommandException(errno.EACCES)

        makedirs.side_effect = mock_makedirs

        # Act
        with self.assertRaises(xs_errors.SROSError) as srose:
            smbsr.create(sr_uuid, 10 * 1024 * 1024 * 1024)

        # Assert
        self.assertEqual(srose.exception.errno, 461)
        symlink.assert_not_called()


    @mock.patch('SMBSR.util.makedirs', autospec=True)
    @mock.patch('SMBSR.util.get_pool_restrictions', autospec=True)
    @mock.patch('SMBSR.Lock', autospec=True)
    @mock.patch('SMBSR.os.symlink', autospec=True)
    def test_create_nospace(self, symlink, lock, restrict, makedirs):
        # Arrange
        smbsr = self.create_smbsr()
        smbsr.session = mock.MagicMock(spec=XenAPI)
        restrict.return_value = []
        sr_uuid = str(uuid.uuid4())
        self.mock_list_dir.return_value = []

        def mock_makedirs(path):
            if path == '/run/sr-mount/SMB/Server/asr_uuid':
                return

            raise util.CommandException(errno.ENOSPC)

        makedirs.side_effect = mock_makedirs

        # Act
        with self.assertRaises(xs_errors.SROSError) as srose:
            smbsr.create(sr_uuid, 10 * 1024 * 1024 * 1024)

        # Assert
        self.assertEqual(srose.exception.errno, 116)
        symlink.assert_not_called()
