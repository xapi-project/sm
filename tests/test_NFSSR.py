import unittest.mock as mock
import nfs
import NFSSR
import SR
import unittest
from uuid import uuid4


class FakeNFSSR(NFSSR.NFSSR):
    uuid = None
    sr_ref = None
    session = None
    srcmd = None
    other_config = {}

    def __init__(self, srcmd, none):
        self.dconf = srcmd.dconf
        self.srcmd = srcmd


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
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_called(self, validate_nfsversion, Lock):
        nfssr = self.create_nfssr(nfsversion='aNfsversion')

        validate_nfsversion.assert_called_once_with('aNfsversion')

    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_returnused(self, validate_nfsversion,
                                                 Lock):
        validate_nfsversion.return_value = 'aNfsversion'

        self.assertEquals(self.create_nfssr().nfsversion, "aNfsversion")

    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_exceptionraised(self,
                                                      validate_nfsversion,
                                                      Lock):
        validate_nfsversion.side_effect = nfs.NfsException('aNfsException')

        self.assertRaises(nfs.NfsException, self.create_nfssr)

    @mock.patch('util.makedirs')
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('nfs.soft_mount')
    @mock.patch('util._testHost')
    @mock.patch('nfs.check_server_tcp')
    @mock.patch('nfs.validate_nfsversion')
    def test_sr_create(self, validate_nfsversion, check_server_tcp, _testhost,
                       soft_mount, Lock, makedirs):
        nfssr = self.create_nfssr(server='aServer', serverpath='/aServerpath',
                                  sr_uuid='UUID', useroptions='options')

        sr_uuid = str(uuid4())
        size = 100
        nfssr.create(sr_uuid, size)

    @mock.patch('NFSSR.os.rmdir')
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('nfs.soft_mount')
    @mock.patch('util._testHost')
    @mock.patch('nfs.check_server_tcp')
    @mock.patch('nfs.validate_nfsversion')
    @mock.patch('NFSSR.xs_errors.XML_DEFS',
                'drivers/XE_SR_ERRORCODES.xml')
    def test_sr_create_mount_error(
            self, validate_nfsversion, check_server_tcp, _testhost,
            soft_mount, Lock, mock_rmdir):

        validate_nfsversion.return_value = '3'

        nfssr = self.create_nfssr(server='aServer', serverpath='/aServerpath',
                                  sr_uuid='UUID', useroptions='options')

        check_server_tcp.side_effect = nfs.NfsException("Failed to detect NFS Server")

        sr_uuid = str(uuid4())
        size = 100
        with self.assertRaises(SR.SROSError):
            nfssr.create(sr_uuid, size)

    @mock.patch('FileSR.SharedFileSR._check_hardlinks', autospec=True)
    @mock.patch('util.makedirs', autospec=True)
    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('nfs.soft_mount', autospec=True)
    @mock.patch('util._testHost', autospec=True)
    @mock.patch('nfs.check_server_tcp', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    def test_attach(self, validate_nfsversion, check_server_tcp, _testhost,
                    soft_mount, Lock, makedirs, mock_checklinks):
        validate_nfsversion.return_value = "aNfsversionChanged"
        nfssr = self.create_nfssr(server='aServer', serverpath='/aServerpath',
                                  sr_uuid='UUID', useroptions='options')

        nfssr.attach(None)

        check_server_tcp.assert_called_once_with('aServer',
                                                 'aNfsversionChanged')
        soft_mount.assert_called_once_with('/var/run/sr-mount/UUID',
                                           'aServer',
                                           '/aServerpath/UUID',
                                           'tcp',
                                           useroptions='options',
                                           timeout=100,
                                           nfsversion='aNfsversionChanged',
                                           retrans=3)

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
