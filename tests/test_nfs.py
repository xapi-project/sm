import unittest
import nfs
import mock
import sys


class Test_nfs(unittest.TestCase):

    @mock.patch('util.pread')
    def test_check_server_tcp(self, pread):
        nfs.check_server_tcp('aServer')

        pread.assert_called_once_with(['/usr/sbin/rpcinfo', '-p', 'aServer'], quiet=False)

    @mock.patch('util.pread')
    def test_check_server_tcp_nfsversion(self, pread):
        nfs.check_server_tcp('aServer', 'aNfsversion')

        pread.assert_called_once_with(['/usr/sbin/rpcinfo', '-p', 'aServer'], quiet=False)

    @mock.patch('util.pread')
    def test_check_server_service(self, pread):
        nfs.check_server_service('aServer')

        pread.assert_called_with(['/usr/sbin/rpcinfo', '-s', 'aServer'])

    def get_soft_mount_pread(self, binary, vers):
        return ([binary, 'remoteserver:remotepath', 'mountpoint', '-o',
                 'soft,proto=transport,vers=%s,acdirmin=0,acdirmax=0' % vers])

    @mock.patch('util.makedirs')
    @mock.patch('nfs.check_server_service')
    @mock.patch('util.pread')
    def test_soft_mount(self, pread, check_server_service, makedirs):
        nfs.soft_mount('mountpoint', 'remoteserver', 'remotepath', 'transport',
                       timeout=None)

        check_server_service.assert_called_once_with('remoteserver')
        pread.assert_called_once_with(self.get_soft_mount_pread('mount.nfs',
                                                                '3'))

    @mock.patch('util.makedirs')
    @mock.patch('nfs.check_server_service')
    @mock.patch('util.pread')
    def test_soft_mount_nfsversion_3(self, pread, 
                                     check_server_service, makedirs):
        nfs.soft_mount('mountpoint', 'remoteserver', 'remotepath', 'transport',
                       timeout=None, nfsversion='3')

        check_server_service.assert_called_once_with('remoteserver')
        pread.assert_called_with(self.get_soft_mount_pread('mount.nfs',
                                                                '3'))

    @mock.patch('util.makedirs')
    @mock.patch('nfs.check_server_service')
    @mock.patch('util.pread')
    def test_soft_mount_nfsversion_4(self, pread, 
                                     check_server_service, makedirs):
        nfs.soft_mount('mountpoint', 'remoteserver', 'remotepath', 'transport',
                       timeout=None, nfsversion='4')

        check_server_service.assert_called_once_with('remoteserver')
        pread.assert_called_with(self.get_soft_mount_pread('mount.nfs4',
                                                                '4'))

    def test_validate_nfsversion_invalid(self):
        for thenfsversion in ['2', '4.1']:
            self.assertRaises(nfs.NfsException, nfs.validate_nfsversion,
                              thenfsversion)

    def test_validate_nfsversion_default(self):
        for thenfsversion in ['', None]:
            self.assertEquals(nfs.validate_nfsversion(thenfsversion), '3')

    def test_validate_nfsversion_valid(self):
        for thenfsversion in ['3', '4']:
            self.assertEquals(nfs.validate_nfsversion(thenfsversion),
                              thenfsversion)
