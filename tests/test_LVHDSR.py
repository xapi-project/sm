from __future__ import print_function
import unittest
import mock
import lvutil
import LVHDSR
import journaler
import lvhdutil
import vhdutil

class SMLog(object):
    def __call__(self, *args):
        print(args)


class Stubs(object):
    def init_stubs(self):
        self._stubs = []

    def stubout(self, *args, **kwargs):
        patcher = mock.patch( * args, ** kwargs)
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

class TestLVHDVDI(unittest.TestCase, Stubs):

    def setUp(self):
        self.init_stubs()

        lvhdutil_patcher = mock.patch('LVHDSR.lvhdutil', autospec=True)
        self.mock_lvhdutil = lvhdutil_patcher.start()
        vhdutil_patcher = mock.patch('LVHDSR.vhdutil', autospec=True)
        self.mock_vhdutil = vhdutil_patcher.start()
        lvutil_patcher = mock.patch('LVHDSR.lvutil', autospec=True)
        self.mock_lvutil = lvutil_patcher.start()
        vdi_util_patcher = mock.patch('VDI.util', autospec=True)
        self.mock_vdi_util = vdi_util_patcher.start()
        sr_util_patcher = mock.patch('LVHDSR.util', autospec=True)
        self.mock_sr_util = sr_util_patcher.start()
        xmlrpclib_patcher = mock.patch('VDI.xmlrpclib', autospec=True)
        self.mock_xmlrpclib = xmlrpclib_patcher.start()
        cbtutil_patcher = mock.patch('VDI.cbtutil', autospec=True)
        self.mock_cbtutil = cbtutil_patcher.start()
        doexec_patcher = mock.patch('util.doexec', autospec=True)
        self.mock_doexec = doexec_patcher.start()

        self.stubout('lvmcache.LVMCache')
        self.stubout('LVHDSR.LVHDSR._ensureSpaceAvailable')
        self.stubout('journaler.Journaler.create')
        self.stubout('journaler.Journaler.remove')
        self.stubout('LVHDSR.RefCounter.set')
        self.stubout('LVHDSR.RefCounter.put')
        self.stubout('LVHDSR.LVMMetadataHandler')

        self.addCleanup(mock.patch.stopall)

    def tearDown(self):
        self.remove_stubs()

    def create_LVHDSR(self):
        srcmd = mock.Mock()
        srcmd.dconf = {'device': '/dev/bar'}
        srcmd.params = {'command': 'foo', 'session_ref': 'some session ref'}
        return LVHDSR.LVHDSR(srcmd, "some SR UUID")

    def get_dummy_vdi(self, vdi_uuid):
        self.mock_lvhdutil.getVDIInfo.return_value = {
            vdi_uuid: lvhdutil.VDIInfo(vdi_uuid)}

        mock_lv =  lvutil.LVInfo('test-lv')
        mock_lv.size = 10240
        mock_lv.active = True
        mock_lv.hidden = False
        mock_lv.vdiType = vhdutil.VDI_TYPE_VHD

        self.mock_lvhdutil.getLVInfo.return_value = {
            vdi_uuid: mock_lv}

        return mock_lv

    def get_dummy_vhd(self, vdi_uuid, hidden):
        test_vhdInfo = vhdutil.VHDInfo(vdi_uuid)
        test_vhdInfo.hidden = hidden
        self.mock_vhdutil.getVHDInfo.return_value = test_vhdInfo

    @mock.patch('LVHDSR.lvutil.LvmLockContext', autospec=True)
    @mock.patch('LVHDSR.Lock', autospec=True)
    @mock.patch('SR.XenAPI')
    def test_clone_success(self, mock_xenapi, mock_lock,
                           mock_lock_context):
        """
        Successfully create clone
        """

        # Arrange

        vdi_uuid = 'some VDI UUID'
        mock_lv = self.get_dummy_vdi(vdi_uuid)
        self.get_dummy_vhd(vdi_uuid, False)

        sr = self.create_LVHDSR()
        sr.isMaster = True
        sr.legacyMode = False
        sr.srcmd.params = {'vdi_ref': 'test ref'}

        vdi = sr.vdi('some VDI UUID')
        self.mock_sr_util.pathexists.return_value = True

        # Act
        clone = vdi.clone(sr.uuid, 'some VDI UUID')

        # Assert
        self.assertIsNotNone(clone)

    @mock.patch('LVHDSR.lvutil.LvmLockContext', autospec=True)
    @mock.patch('LVHDSR.Lock', autospec=True)
    @mock.patch('SR.XenAPI')
    def test_snapshot_attached_success(
            self, mock_xenapi,  mock_lock, mock_lock_context):
        """
        LVHDSR.snapshot, attached on host, no CBT
        """
        # Arrange
        vdi_uuid = 'some VDI UUID'
        mock_lv = self.get_dummy_vdi(vdi_uuid)
        self.get_dummy_vhd(vdi_uuid, False)

        sr = self.create_LVHDSR()
        sr.isMaster = True
        sr.legacyMode = False
        sr.srcmd.params = {
            'vdi_ref': 'test ref',
            'driver_params': {
                'type': 'double'}
            }
        sr.cmd = "vdi_snapshot"

        vdi = sr.vdi('some VDI UUID')
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD
        self.mock_sr_util.pathexists.return_value = True
        self.mock_sr_util.get_hosts_attached_on.return_value = ["hostref2"]
        self.mock_sr_util.get_this_host_ref.return_value = ["hostref1"]

        # Act
        snap = vdi.snapshot(sr.uuid, "Dummy UUID")

        # Assert
        self.assertIsNotNone(snap)

    @mock.patch('LVHDSR.lvutil.LvmLockContext', autospec=True)
    @mock.patch('LVHDSR.Lock', autospec=True)
    @mock.patch('SR.XenAPI')
    def test_snapshot_attached_cbt_success(
            self, mock_xenapi,  mock_lock, mock_lock_context):
        """
        LVHDSR.snapshot, attached on host, with CBT
        """
        # Arrange
        vdi_uuid = 'some VDI UUID'
        mock_lv = self.get_dummy_vdi(vdi_uuid)
        self.get_dummy_vhd(vdi_uuid, False)

        sr = self.create_LVHDSR()
        sr.isMaster = True
        sr.legacyMode = False
        sr.srcmd.params = {
            'vdi_ref': 'test ref',
            'driver_params': {
                'type': 'double'}
            }
        sr.cmd = "vdi_snapshot"

        vdi = sr.vdi('some VDI UUID')
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD
        self.mock_sr_util.pathexists.return_value = True
        self.mock_sr_util.get_hosts_attached_on.return_value = ["hostref2"]
        self.mock_sr_util.get_this_host_ref.return_value = ["hostref1"]
        self.mock_vdi_util.sr_get_capability.return_value = {
            'VDI_CONFIG_CBT'}

        # Act
        with mock.patch('lock.Lock'):
            snap = vdi.snapshot(sr.uuid, "Dummy UUID")

        # Assert
        self.assertIsNotNone(snap)
        self.assertEqual(self.mock_cbtutil.set_cbt_child.call_count, 3)
