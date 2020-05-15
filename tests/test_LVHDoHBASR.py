import mock
import LVHDoHBASR
import unittest
import xmlrpclib
import SR


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

        load_object = xmlrpclib.loads(stuff)
        load_object = xmlrpclib.loads(load_object[0][0])

        self.assertEqual(load_object[0][0]["sr_uuid"], sr_uuid)
        self.assertEqual(load_object[0][0]["vdi_uuid"], vdi_uuid)
        self.assertEqual(load_object[0][0]["device_config"]["multipathing"],
                         mpathing)
        self.assertEqual(load_object[0][0]["device_config"]["multipathhandle"],
                         mpath_handle)

    @mock.patch('LVHDoHBASR.LVHDoHBASR', autospec=True)
    @mock.patch('LVHDoHBASR.xs_errors.XML_DEFS',
                "drivers/XE_SR_ERRORCODES.xml")
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

        with self.assertRaises(SR.SROSError) as cm:
            stuff = vdi.generate_config(sr_uuid, vdi_uuid)

        self.assertEqual(str(cm.exception), "The VDI is not available")
