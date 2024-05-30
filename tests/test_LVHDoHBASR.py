import unittest.mock as mock
import LVHDoHBASR
import unittest
import xmlrpc.client
import SRCommand
import xs_errors

from uuid import uuid4


def mock_init(self, sr, sr_uuid):
    self.sr = sr
    self.sr_uuid = sr_uuid


class TestLVHDoHBAVDI(unittest.TestCase):

    @mock.patch('LVHDoHBASR.LVHDoHBASR', autospec=True)
    @mock.patch('LVHDoHBASR.LVHDoHBAVDI.__init__', mock_init)
    @mock.patch('LVHDoHBASR.lvutil._checkLV', autospec=True)
    def test_generate_config(self,
                             mock_checkLV,
                             mock_SR):
        mock_checkLV.return_value = True
        sr_uuid = 1234
        vdi_uuid = 5678
        mpath_handle = 999
        mpathing = True
        sr = mock_SR.return_value
        sr.dconf = {}
        sr.mpath = mpathing
        sr.mpathhandle = mpath_handle
        sr.lock = "blah"

        vdi = LVHDoHBASR.LVHDoHBAVDI(sr, sr_uuid)
        vdi.path = "blahblah"
        stuff = vdi.generate_config(sr_uuid, vdi_uuid)

        load_object = xmlrpc.client.loads(stuff)
        load_object = xmlrpc.client.loads(load_object[0][0])

        self.assertEqual(load_object[0][0]["sr_uuid"], sr_uuid)
        self.assertEqual(load_object[0][0]["vdi_uuid"], vdi_uuid)
        self.assertEqual(load_object[0][0]["device_config"]["multipathing"],
                         mpathing)
        self.assertEqual(load_object[0][0]["device_config"]["multipathhandle"],
                         mpath_handle)

    @mock.patch('LVHDoHBASR.LVHDoHBASR', autospec=True)
    @mock.patch('LVHDoHBASR.LVHDoHBAVDI.__init__', mock_init)
    @mock.patch('LVHDoHBASR.lvutil._checkLV', autospec=True)
    def test_generate_config_bad_path_assert(self,
                                             mock_checkLV,
                                             mock_SR):
        mock_checkLV.return_value = False
        sr_uuid = 1234
        vdi_uuid = 5678
        mpath_handle = 999
        mpathing = True
        sr = mock_SR.return_value
        sr.dconf = {}
        sr.mpath = mpathing
        sr.mpathhandle = mpath_handle

        vdi = LVHDoHBASR.LVHDoHBAVDI(sr, sr_uuid)
        vdi.path = "blahblah"

        with self.assertRaises(xs_errors.SROSError) as cm:
            stuff = vdi.generate_config(sr_uuid, vdi_uuid)

        self.assertEqual(str(cm.exception), "The VDI is not available")


class TestLVHDoHBASR(unittest.TestCase):

    def setUp(self):
        self.host_ref = str(uuid4())
        self.session_ref = str(uuid4())
        self.sr_ref = str(uuid4())
        self.sr_uuid = str(uuid4())
        self.scsi_id = '360a98000534b4f4e46704c76692d6d33'

        lock_patcher = mock.patch('LVHDSR.Lock', autospec=True)
        self.mock_lock = lock_patcher.start()
        lvhdsr_patcher = mock.patch('LVHDoHBASR.LVHDSR')
        self.mock_lvhdsr = lvhdsr_patcher.start()
        util_patcher = mock.patch('LVHDoHBASR.util', autospec=True)
        self.mock_util = util_patcher.start()
        lc_patcher = mock.patch('LVHDSR.lvmcache.lvutil.Fairlock', autospec=True)
        self.mock_lc = lc_patcher.start()
        xenapi_patcher = mock.patch('SR.XenAPI')
        self.mock_xapi = xenapi_patcher.start()

        self.addCleanup(mock.patch.stopall)

    def create_sr_cmd(self, cmd):
        host_ref = self.host_ref
        scsi_id = self.scsi_id
        session_ref = self.session_ref
        sr_ref = self.sr_ref
        sr_uuid = self.sr_uuid

        def mock_parse(self):
            self.cmd = cmd
            device_config = {
                'SCSIid': scsi_id,
                'SRmaster': 'true'
            }
            self.params = {
                'command': cmd,
                'device_config': device_config,
                'host_ref': host_ref,
                'session_ref': session_ref,
                'sr_ref': sr_ref,
                'sr_uuid': sr_uuid
            }
            self.sr_uuid = sr_uuid
            self.dconf = device_config

        with mock.patch('SRCommand.SRCommand.parse', autospec=True) as parse:
            parse.side_effect = mock_parse
            srcmd = SRCommand.SRCommand({})
            srcmd.parse()
        return srcmd

    @mock.patch("builtins.open", new_callable=mock.mock_open())
    @mock.patch('LVHDoHBASR.glob.glob', autospec=True)
    def test_sr_delete_no_multipath(self, mock_glob, mock_open):
        # Arrange
        srcmd = self.create_sr_cmd("sr_delete")

        sr = LVHDoHBASR.LVHDoHBASR(srcmd, self.sr_uuid)

        mock_glob.return_value = ['/dev/sdd', '/dev/sde',
                                  '/dev/sdi', '/dev/sdh']

        # Act
        srcmd.run(sr)

        # Assert
        mock_open.assert_has_calls([
            mock.call('/sys/block/sdd/device/delete', 'w'),
            mock.call('/sys/block/sde/device/delete', 'w'),
            mock.call('/sys/block/sdi/device/delete', 'w'),
            mock.call('/sys/block/sdh/device/delete', 'w')],
            any_order=True)
