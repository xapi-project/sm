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
                       timeout=0)

        pread.assert_called_with(self.AnyArrayWith('mount.nfs'))
        pread.assert_called_with(self.AnyArrayWith('remoteserver:remotepath'))
        pread.assert_called_with(self.AnyArrayWith('mountpoint'))
        pread.assert_called_with(self.AnyArrayWith('-o'))
