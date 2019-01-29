import errno
import mock
import nfs
import sys
import unittest
import util

class Test_nfs(unittest.TestCase):

    @mock.patch('util.pread', autospec=True)
    def test_check_server_tcp(self, pread):
        nfs.check_server_tcp('aServer')

        pread.assert_called_once_with(['/usr/sbin/rpcinfo', '-p', 'aServer'], quiet=False)

    @mock.patch('util.pread', autospec=True)
    def test_check_server_tcp_nfsversion(self, pread):
        nfs.check_server_tcp('aServer', 'aNfsversion')

        pread.assert_called_once_with(['/usr/sbin/rpcinfo', '-p', 'aServer'], quiet=False)

    @mock.patch('time.sleep', autospec=True)
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('util.pread')
    def test_check_server_service(self, pread, sleep):
        pread.side_effect=["    100003  4,3,2     udp6,tcp6,udp,tcp                nfs         superuser"]
        service_found = nfs.check_server_service('aServer')

        self.assertTrue(service_found)
        self.assertEqual(len(pread.mock_calls), 1)
        pread.assert_called_with(['/usr/sbin/rpcinfo', '-s', 'aServer'])
        # Mock==1.0.1 (all that is available in Epel) doesn't suport this
        #sleep.assert_not_called()

    @mock.patch('time.sleep', autospec=True)
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('util.pread')
    def test_check_server_service_with_retries(self, pread, sleep):
        pread.side_effect=["",
                           "",
                           "    100003  4,3,2     udp6,tcp6,udp,tcp                nfs         superuser"]
        service_found = nfs.check_server_service('aServer')

        self.assertTrue(service_found)
        self.assertEqual(len(pread.mock_calls), 3)
        pread.assert_called_with(['/usr/sbin/rpcinfo', '-s', 'aServer'])

    @mock.patch('time.sleep', autospec=True)
    @mock.patch('util.pread', autospec=True)
    def test_check_server_service_not_available(self, pread, sleep):
        pread.return_value=""

        service_found = nfs.check_server_service('aServer')

        self.assertFalse(service_found)

    @mock.patch('time.sleep', autospec=True)
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('util.pread')
    def test_check_server_service_exception(self, pread, sleep):
        pread.side_effect=[util.CommandException(errno.ENOMEM)]
        with self.assertRaises(util.CommandException):
            nfs.check_server_service('aServer')


    @mock.patch('time.sleep', autospec=True)
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('util.pread')
    def test_check_server_service_first_call_exception(self, pread, sleep):
        pread.side_effect=[util.CommandException(errno.EPIPE),
                            "    100003  4,3,2     udp6,tcp6,udp,tcp                nfs         superuser"]
        service_found = nfs.check_server_service('aServer')

        self.assertTrue(service_found)
        self.assertEqual(len(pread.mock_calls), 2)

    def get_soft_mount_pread(self, binary, vers):
        return ([binary, 'remoteserver:remotepath', 'mountpoint', '-o',
                 'soft,proto=transport,vers=%s,acdirmin=0,acdirmax=0' % vers])

    @mock.patch('util.makedirs', autospec=True)
    @mock.patch('nfs.check_server_service', autospec=True)
    @mock.patch('util.pread', autospec=True)
    def test_soft_mount(self, pread, check_server_service, makedirs):
        nfs.soft_mount('mountpoint', 'remoteserver', 'remotepath', 'transport',
                       timeout=None)

        check_server_service.assert_called_once_with('remoteserver')
        pread.assert_called_once_with(self.get_soft_mount_pread('mount.nfs',
                                                                '3'))

    @mock.patch('util.makedirs', autospec=True)
    @mock.patch('nfs.check_server_service', autospec=True)
    @mock.patch('util.pread', autospec=True)
    def test_soft_mount_nfsversion_3(self, pread, 
                                     check_server_service, makedirs):
        nfs.soft_mount('mountpoint', 'remoteserver', 'remotepath', 'transport',
                       timeout=None, nfsversion='3')

        check_server_service.assert_called_once_with('remoteserver')
        pread.assert_called_with(self.get_soft_mount_pread('mount.nfs',
                                                                '3'))

    @mock.patch('util.makedirs', autospec=True)
    @mock.patch('nfs.check_server_service', autospec=True)
    @mock.patch('util.pread', autospec=True)
    def test_soft_mount_nfsversion_4(self, pread, 
                                     check_server_service, makedirs):
        nfs.soft_mount('mountpoint', 'remoteserver', 'remotepath', 'transport',
                       timeout=None, nfsversion='4')

        check_server_service.assert_called_once_with('remoteserver')
        pread.assert_called_with(self.get_soft_mount_pread('mount.nfs4',
                                                                '4'))

    def test_validate_nfsversion_invalid(self):
        for thenfsversion in ['0', '5']:
            self.assertRaises(nfs.NfsException, nfs.validate_nfsversion,
                              thenfsversion)

    def test_validate_nfsversion_unsupported(self):
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
