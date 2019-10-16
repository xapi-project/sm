from __future__ import print_function
import unittest
import mock
import LVHDSR
import journaler
import lvhdutil


class SMLog(object):
    def __call__(self, *args):
        print(args)


class Stubs(object):
    def init_stubs(self):
        self._stubs = []

    def stubout(self, *args, **kwargs):
        patcher = mock.patch(*args, **kwargs)
        self._stubs.append(patcher)
        patcher.start()

    def remove_stubs(self):
        for patcher in self._stubs:
            patcher.stop()


class TestLVHDSR(unittest.TestCase, Stubs):

    def setUp(self):
        self.init_stubs()

    def tearDown(self):
        self.remove_stubs()

    def create_LVHDSR(self):
        srcmd = mock.Mock()
        srcmd.dconf = {'device': '/dev/bar'}
        srcmd.params = {'command': 'foo', 'session_ref': 'some session ref'}
        return LVHDSR.LVHDSR(srcmd, "some SR UUID")

    @mock.patch('lvhdutil.getVDIInfo', autospec=True)
    @mock.patch('LVHDSR.Lock', autospec=True)
    @mock.patch('SR.XenAPI')
    def test_loadvids(self, mock_xenapi, mock_lock, mock_getVDIInfo):
        """sr.allVDIs populated by _loadvdis"""

        vdi_uuid = 'some VDI UUID'
        mock_getVDIInfo.return_value = {vdi_uuid: lvhdutil.VDIInfo(vdi_uuid)}
        sr = self.create_LVHDSR()

        sr._loadvdis()

        self.assertEquals([vdi_uuid], sr.allVDIs.keys())

    @mock.patch('lvhdutil.lvRefreshOnAllSlaves', autospec=True)
    @mock.patch('lvhdutil.getVDIInfo', autospec=True)
    @mock.patch('journaler.Journaler.getAll', autospec=True)
    @mock.patch('LVHDSR.Lock', autospec=True)
    @mock.patch('SR.XenAPI')
    def test_undoAllInflateJournals(
            self,
            mock_xenapi,
            mock_lock,
            mock_getAll,
            mock_getVDIInfo,
            mock_lvhdutil_lvRefreshOnAllSlaves):
        """No LV refresh on slaves when Cleaning up local LVHD SR's journal"""

        self.stubout('journaler.Journaler.remove')
        self.stubout('util.zeroOut')
        self.stubout('lvhdutil.deflate')
        self.stubout('util.SMlog', new_callable=SMLog)
        self.stubout('lvmcache.LVMCache')

        vdi_uuid = 'some VDI UUID'

        mock_getAll.return_value = {vdi_uuid: '0'}
        mock_getVDIInfo.return_value = {vdi_uuid: lvhdutil.VDIInfo(vdi_uuid)}

        sr = self.create_LVHDSR()

        sr._undoAllInflateJournals()
        self.assertEquals(0, mock_lvhdutil_lvRefreshOnAllSlaves.call_count)
