import errno
import unittest.mock as mock
from sm import nfs
import NFSSR
from sm import SR
import unittest
from uuid import uuid4

from sm.core import util
from sm.core import xs_errors


class FakeNFSSR(NFSSR.NFSSR):
    uuid = None
    sr_ref = None
    session = None
    srcmd = None
    other_config = {}

    def __init__(self, srcmd, none):
        self.dconf = srcmd.dconf
        self.srcmd = srcmd

@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
class TestNFSSR(unittest.TestCase):

    def create_nfssr(self, server='aServer', serverpath='/aServerpath',
                     sr_uuid='asr_uuid', nfsversion=None, useroptions=''):
        srcmd = mock.Mock()
        srcmd.dconf = {
            'server': server,
            'serverpath': serverpath
        }
        if nfsversion:
            srcmd.dconf.update({'nfsversion': nfsversion})
        if useroptions:
            srcmd.dconf.update({'options': useroptions})
        srcmd.params = {
            'command': 'some_command',
            'device_config': {}
        }
        nfssr = FakeNFSSR(srcmd, None)
        nfssr.load(sr_uuid)
        return nfssr

    @mock.patch('NFSSR.Lock', autospec=True)
    def test_load(self, Lock):
        self.create_nfssr()

    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_called(self, validate_nfsversion, Lock):
        nfssr = self.create_nfssr(nfsversion='aNfsversion')

        validate_nfsversion.assert_called_once_with('aNfsversion')

    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_returnused(self, validate_nfsversion,
                                                 Lock):
        validate_nfsversion.return_value = 'aNfsversion'

        self.assertEqual(self.create_nfssr().nfsversion, "aNfsversion")

    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_exceptionraised(self,
                                                      validate_nfsversion,
                                                      Lock):
        validate_nfsversion.side_effect = nfs.NfsException('aNfsException')

        self.assertRaises(nfs.NfsException, self.create_nfssr)

    @mock.patch('NFSSR.util.makedirs')
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.soft_mount')
    @mock.patch('NFSSR.util._testHost')
    @mock.patch('sm.nfs.check_server_tcp')
    @mock.patch('sm.nfs.validate_nfsversion')
    def test_sr_create(self, validate_nfsversion, check_server_tcp, _testhost,
                       soft_mount, Lock, makedirs):
        nfssr = self.create_nfssr(server='aServer', serverpath='/aServerpath',
                                  sr_uuid='UUID', useroptions='options')

        sr_uuid = str(uuid4())
        size = 100
        nfssr.create(sr_uuid, size)

    @mock.patch('NFSSR.util.makedirs')
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.soft_mount')
    @mock.patch('NFSSR.util._testHost')
    @mock.patch('sm.nfs.check_server_tcp')
    @mock.patch('sm.nfs.validate_nfsversion')
    def test_sr_create_readonly(self, validate_nfsversion, check_server_tcp, _testhost,
                       soft_mount, Lock, makedirs):
        # Arrange
        nfssr = self.create_nfssr(server='aServer', serverpath='/aServerpath',
                                  sr_uuid='UUID', useroptions='options')

        sr_uuid = str(uuid4())
        size = 100

        def mock_makedirs(path):
            raise util.CommandException(errno.EROFS)

        makedirs.side_effect = mock_makedirs

        # Act
        with self.assertRaises(xs_errors.SROSError) as srose:
            nfssr.create(sr_uuid, size)

        self.assertEqual(srose.exception.errno, 461)

    @mock.patch('NFSSR.util.makedirs')
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.soft_mount')
    @mock.patch('NFSSR.util._testHost')
    @mock.patch('sm.nfs.check_server_tcp')
    @mock.patch('sm.nfs.validate_nfsversion')
    def test_sr_create_noperm(self, validate_nfsversion, check_server_tcp, _testhost,
                       soft_mount, Lock, makedirs):
        # Arrange
        nfssr = self.create_nfssr(server='aServer', serverpath='/aServerpath',
                                  sr_uuid='UUID', useroptions='options')

        sr_uuid = str(uuid4())
        size = 100

        def mock_makedirs(path):
            raise util.CommandException(errno.EPERM)


        makedirs.side_effect = mock_makedirs

        # Act
        with self.assertRaises(xs_errors.SROSError) as srose:
            nfssr.create(sr_uuid, size)

        self.assertEqual(srose.exception.errno, 88)


    @mock.patch('NFSSR.os.rmdir')
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.soft_mount')
    @mock.patch('NFSSR.util._testHost')
    @mock.patch('sm.nfs.check_server_tcp')
    @mock.patch('sm.nfs.validate_nfsversion')
    def test_sr_create_mount_error(
            self, validate_nfsversion, check_server_tcp, _testhost,
            soft_mount, Lock, mock_rmdir):

        validate_nfsversion.return_value = '3'

        nfssr = self.create_nfssr(server='aServer', serverpath='/aServerpath',
                                  sr_uuid='UUID', useroptions='options')

        check_server_tcp.side_effect = nfs.NfsException("Failed to detect NFS Server")

        sr_uuid = str(uuid4())
        size = 100
        with self.assertRaises(xs_errors.SROSError):
            nfssr.create(sr_uuid, size)

    @mock.patch('sm.drivers.FileSR.SharedFileSR._check_writable', autospec=True)
    @mock.patch('sm.drivers.FileSR.SharedFileSR._check_hardlinks', autospec=True)
    @mock.patch('NFSSR.util.makedirs', autospec=True)
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.soft_mount', autospec=True)
    @mock.patch('NFSSR.util._testHost', autospec=True)
    @mock.patch('sm.nfs.check_server_tcp', autospec=True)
    @mock.patch('sm.nfs.validate_nfsversion', autospec=True)
    def test_attach(self, validate_nfsversion, check_server_tcp, _testhost,
                    soft_mount, Lock, makedirs, mock_checklinks,
                    mock_checkwritable):
        validate_nfsversion.return_value = "aNfsversionChanged"
        nfssr = self.create_nfssr(server='aServer', serverpath='/aServerpath',
                                  sr_uuid='UUID', useroptions='options')

        nfssr.attach(None)

        check_server_tcp.assert_called_once_with('aServer', 'tcp',
                                                 'aNfsversionChanged')
        soft_mount.assert_called_once_with('/run/sr-mount/UUID',
                                           'aServer',
                                           '/aServerpath/UUID',
                                           'tcp',
                                           useroptions='options',
                                           timeout=200,
                                           nfsversion='aNfsversionChanged',
                                           retrans=4)

    @mock.patch('NFSSR.util.makedirs', autospec=True)
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.soft_mount', autospec=True)
    @mock.patch('sm.nfs.unmount', autospec=True)
    @mock.patch('NFSSR.util._testHost', autospec=True)
    @mock.patch('sm.nfs.check_server_tcp', autospec=True)
    @mock.patch('sm.nfs.validate_nfsversion', autospec=True)
    def test_attach_failure(self, validate_nfsversion, check_server_tcp,
                            _testhost, unmount, soft_mount, Lock, makedirs):
        soft_mount.side_effect = xs_errors.SRException("aFailure")

        nfssr = self.create_nfssr()

        with self.assertRaises(xs_errors.SRException):
            nfssr.attach(None)

        unmount.assert_not_called()

    @mock.patch('sm.drivers.FileSR.SharedFileSR._checkmount', autospec=True)
    @mock.patch('NFSSR.util.makedirs', autospec=True)
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.soft_mount', autospec=True)
    def test_attach_already_mounted(self, soft_mount, Lock, makedirs,
                                    mock_checkmount):
        mock_checkmount.return_value = True

        nfssr = self.create_nfssr()

        nfssr.attach(None)

        soft_mount.assert_not_called()

    @mock.patch('sm.drivers.FileSR.SharedFileSR._checkmount', autospec=True)
    @mock.patch('sm.drivers.FileSR.SharedFileSR._check_writable', autospec=True)
    @mock.patch('sm.drivers.FileSR.SharedFileSR._check_hardlinks', autospec=True)
    @mock.patch('NFSSR.util.makedirs', autospec=True)
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('sm.nfs.soft_mount', autospec=True)
    @mock.patch('sm.nfs.unmount', autospec=True)
    @mock.patch('NFSSR.util._testHost', autospec=True)
    @mock.patch('sm.nfs.check_server_tcp', autospec=True)
    @mock.patch('sm.nfs.validate_nfsversion', autospec=True)
    def test_attach_not_writable(self, validate_nfsversion, check_server_tcp,
                                 _testhost, unmount, soft_mount, Lock, makedirs,
                                 mock_checklinks, mock_checkwritable,
                                 mock_checkmount):
        events = []

        def is_mounted():
            return len(events) % 2 == 1

        def fake_soft_mount(*args, **kwargs):
            assert not is_mounted()
            events.append("mount")

        def fake_unmount(*args, **kwargs):
            assert is_mounted()
            events.append("unmount")

        mock_checkmount.side_effect = lambda *args: is_mounted()
        soft_mount.side_effect = fake_soft_mount
        unmount.side_effect = fake_unmount
        mock_checkwritable.side_effect = xs_errors.SRException("aFailure")

        nfssr = self.create_nfssr(sr_uuid='UUID')

        with self.assertRaises(xs_errors.SRException):
            nfssr.attach(None)

        soft_mount.assert_called_once()
        unmount.assert_called_once_with('/run/sr-mount/UUID', True)

    @mock.patch('NFSSR.Lock', autospec=True)
    def test_load_ipv6(self, mock_lock):
        nfssr = self.create_nfssr(server='::1')
        self.assertEqual(nfssr.transport, 'tcp6')

    @mock.patch('NFSSR.Lock', autospec=True)
    def test_load_no_server(self, mock_lock):
        """
        As called by on_slave.is_open
        """
        nfssr = self.create_nfssr(server=None)

        self.assertIsNotNone(nfssr)
