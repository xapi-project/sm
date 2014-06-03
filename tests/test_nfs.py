import unittest
import nfs
import mock
import sys


class Test_nfs(unittest.TestCase):

    class AnyArrayWith(str):
        def __eq__(needle, haystack):
            return needle in haystack

    @mock.patch('util.makedirs')
    @mock.patch('util.pread')
    def test_soft_mount(self, pread, makedirs):
        nfs.soft_mount('mountpoint', 'remoteserver', 'remotepath', 'transport',
                       nfsversion='nfsversion', timeout=0)

        pread.assert_called_with(self.AnyArrayWith('mount.nfs'))
        pread.assert_called_with(self.AnyArrayWith('remoteserver:remotepath'))
        pread.assert_called_with(self.AnyArrayWith('mountpoint'))
        pread.assert_called_with(self.AnyArrayWith('-o'))

    @mock.patch('util.makedirs')
    @mock.patch('util.pread')
    def test_soft_mount_nfsversion_4(self, pread, makedirs):
        nfs.soft_mount('mountpoint', 'remoteserver', 'remotepath', 'transport',
                       nfsversion='4', timeout=0)

        pread.assert_called_with(self.AnyArrayWith('mount.nfs4'))

    def test_validate_nfsversion_invalid(self):
            self.assertRaises(nfs.NfsException, nfs.validate_nfsversion,
                              thenfsversion)

    def test_validate_nfsversion_default(self):
        for thenfsversion in ['', None]:
            self.assertEquals(nfs.validate_nfsversion(thenfsversion), '3')

    def test_validate_nfsversion_invalid(self):
        for thenfsversion in ['3', '4']:
            self.assertEquals(nfs.validate_nfsversion(thenfsversion),
                              thenfsversion)
