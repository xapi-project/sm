import unittest.mock as mock
import LVHDoFCoESR
import SR
import unittest
import testlib


class FakeFCoESR(LVHDoFCoESR.LVHDoFCoESR):
    uuid = None
    sr_ref = None
    session = None
    srcmd = None
    host_ref = None

    def __init__(self, srcmd, session, sr_ref, host_ref):
        self.dconf = srcmd.dconf
        self.srcmd = srcmd
        self.original_srcmd = srcmd
        self.session = session
        self.sr_ref = sr_ref
        self.host_ref = host_ref


class TestFCoESR(unittest.TestCase):

    def create_fcoesr(self, path="/dev/example", SCSIid="abcd",
                      sr_uuid='asr_uuid', type="None", session="sesion",
                      sr_ref="sr", host_ref="host", params={}):
        srcmd = mock.Mock()
        srcmd.params = params
        srcmd.dconf = {
            'path': path,
            'SCSIid': SCSIid,
            'type': type
        }

        fcoesr = FakeFCoESR(srcmd, session, sr_ref, host_ref)
        fcoesr.load(sr_uuid)
        return fcoesr

    @mock.patch('SR.driver', autospec=True)
    @mock.patch('util.find_my_pbd', autospec=True)
    @mock.patch('LVHDoFCoESR.LVHDoHBASR.HBASR.HBASR.print_devs', autospec=True)
    @testlib.with_context
    def test_load_no_scsiid(self, context, print_devs, find_my_pbd, driver):
        context.setup_error_codes()
        find_my_pbd.return_value = ['pbd_ref', 'pbd']
        parameters = {}
        parameters['device_config'] = ""
        self.assertRaises(SR.SROSError, self.create_fcoesr, SCSIid="", params=parameters)

    @mock.patch('SR.driver', autospec=True)
    @mock.patch('util.find_my_pbd', autospec=True)
    @mock.patch('SR.SR._pathrefresh', autospec=True)
    @mock.patch('LVHDoFCoESR.LVHDSR.LVHDSR.load', autospec=True)
    def test_load_scsiid(self, lvhdsrload, pathrefresh, find_my_pbd, driver):
        find_my_pbd.return_value = ['pbd_ref', 'pbd']
        parameters = {}
        parameters['device_config'] = ""
        self.create_fcoesr(params=parameters)

    @mock.patch('SR.driver', autospec=True)
    @mock.patch('util.find_my_pbd', autospec=True)
    @mock.patch('SR.SR._pathrefresh', autospec=True)
    @mock.patch('LVHDoFCoESR.LVHDSR.LVHDSR.load', autospec=True)
    def test_load_pbd_exception(self, lvhdsrload, pathrefresh, find_my_pbd, driver):
        find_my_pbd.side_effect = Exception('exception raised')
        parameters = {}
        parameters['device_config'] = ""
        self.create_fcoesr(params=parameters)

    @mock.patch('SR.driver', autospec=True)
    @mock.patch('util.find_my_pbd', autospec=True)
    @mock.patch('SR.SR._pathrefresh', autospec=True)
    @mock.patch('LVHDoFCoESR.LVHDSR.LVHDSR.load', autospec=True)
    def test_vdi(self, lvhdsrload, pathrefresh, find_my_pbd, driver):
        sr_uuid = 'bsr_uuid'
        find_my_pbd.return_value = ['pbd_ref', 'pbd']
        parameters = {}
        parameters['device_config'] = ""
        fcoesr = self.create_fcoesr(params=parameters)

        def mock_init(self, sr, sr_uuid):
            pass
        with mock.patch('LVHDoFCoESR.LVHDoHBASR.LVHDoHBAVDI.__init__', new=mock_init):
            fcoesr.vdi(sr_uuid)
