import unittest.mock as mock
import HBASR
import unittest
import SR
import xml.dom.minidom
from sm.core import util
from sm.core import xs_errors


def mock_init(self):
    pass


def imp_fake_probe():
    dom = xml.dom.minidom.Document()
    hbalist = dom.createElement("HBAInfoList")
    dom.appendChild(hbalist)

    for host in ["host1", "host2"]:
        hbainfo = dom.createElement("HBAInfo")
        hbalist.appendChild(hbainfo)

        sname = "nvme_special"
        entry = dom.createElement("model")
        hbainfo.appendChild(entry)
        textnode = dom.createTextNode(sname)
        entry.appendChild(textnode)

        nname = "0x200000e08b18208b"
        nname = util.make_WWN(nname)
        entry = dom.createElement("nodeWWN")
        hbainfo.appendChild(entry)
        textnode = dom.createTextNode(nname)
        entry.appendChild(textnode)

        port = dom.createElement("Port")
        hbainfo.appendChild(port)

        pname = "0x500143802426baf4"
        pname = util.make_WWN(pname)
        entry = dom.createElement("portWWN")
        port.appendChild(entry)
        textnode = dom.createTextNode(pname)
        entry.appendChild(textnode)

        state = "toast"
        entry = dom.createElement("state")
        port.appendChild(entry)
        textnode = dom.createTextNode(state)
        entry.appendChild(textnode)

        entry = dom.createElement("deviceName")
        port.appendChild(entry)
        textnode = dom.createTextNode("/sys/class/scsi_host/%s" % host)
        entry.appendChild(textnode)

    return dom.toxml()


def fake_probe(self):
    return imp_fake_probe()


@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
class TestHBASR(unittest.TestCase):

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    def test_handles(self):

        sr = HBASR.HBASR()

        self.assertFalse(sr.handles("blah"))
        self.assertTrue(sr.handles("hba"))

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    def test_load(self):

        sr_uuid = 123
        sr = HBASR.HBASR()
        sr.dconf = {}
        sr.load(sr_uuid)

        self.assertEqual(sr.sr_vditype, 'phy')
        self.assertEqual(sr.type, 'any')
        self.assertFalse(sr.attached)
        self.assertEqual(sr.procname, "")
        self.assertEqual(sr.devs, {})

        sr.dconf = {"type": None}
        sr.load(sr_uuid)

        self.assertEqual(sr.sr_vditype, 'phy')
        self.assertEqual(sr.type, 'any')
        self.assertFalse(sr.attached)
        self.assertEqual(sr.procname, "")
        self.assertEqual(sr.devs, {})

        sr.dconf = {"type": "blah"}
        sr.load(sr_uuid)

        self.assertEqual(sr.sr_vditype, 'phy')
        self.assertEqual(sr.type, 'blah')
        self.assertFalse(sr.attached)
        self.assertEqual(sr.procname, "")
        self.assertEqual(sr.devs, {})

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.devscan.adapters', autospec=True)
    @mock.patch('HBASR.scsiutil.cacheSCSIidentifiers', autospec=True)
    def test__intit_bhadict_already_init(self, mock_cacheSCSIidentifiers,
                                         mock_devscan_adapters):
        sr = HBASR.HBASR()
        sr.hbas = {"Pitt": "The elder"}
        sr._init_hbadict()
        self.assertEqual(mock_cacheSCSIidentifiers.call_count, 0)
        self.assertEqual(mock_devscan_adapters.call_count, 0)

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.devscan.adapters', autospec=True)
    @mock.patch('HBASR.scsiutil.cacheSCSIidentifiers', autospec=True)
    def test__init_hbadict(self, mock_cacheSCSIidentifiers,
                           mock_devscan_adapters):
        sr = HBASR.HBASR()
        sr.type = "foo"
        mock_devscan_adapters.return_value = {"devs": "toaster", "adt": []}
        sr._init_hbadict()
        mock_devscan_adapters.assert_called_with(filterstr="foo")
        self.assertEqual(mock_cacheSCSIidentifiers.call_count, 0)
        self.assertEqual(mock_devscan_adapters.call_count, 1)
        self.assertEqual(sr.hbas, [])
        self.assertEqual(sr.hbadict, "toaster")

        mock_cacheSCSIidentifiers.call_count = 0
        mock_devscan_adapters.call_count = 0
        mock_cacheSCSIidentifiers.return_value = "123445"
        sr2 = HBASR.HBASR()
        sr2.type = "foo"
        mock_devscan_adapters.return_value = {"devs": "toaster",
                                              "adt": ["dev1", "dev2"]}
        sr2._init_hbadict()
        self.assertEqual(mock_cacheSCSIidentifiers.call_count, 1)
        self.assertEqual(mock_devscan_adapters.call_count, 1)
        self.assertEqual(sr2.hbas, ["dev1", "dev2"])
        self.assertEqual(sr2.hbadict, "toaster")
        self.assertTrue(sr2.attached)
        self.assertEqual(sr2.devs, "123445")

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.HBASR._probe_hba', autospec=True)
    @mock.patch('HBASR.xml.dom.minidom.parseString', autospec=True)
    def test__init_hbahostname_assert(self, mock_parseString, mock_probe_hba):
        sr = HBASR.HBASR()
        mock_probe_hba.return_value = "blah"
        mock_parseString.side_effect = Exception("bad xml")
        with self.assertRaises(xs_errors.SROSError) as cm:
            sr._init_hba_hostname()
        self.assertEqual(str(cm.exception),
                         "Unable to parse XML "
                         "[opterr=HBA Host WWN scanning failed]")

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.HBASR._probe_hba', fake_probe)
    def test__init_hbahostname(self):
        sr = HBASR.HBASR()
        res = sr._init_hba_hostname()
        self.assertEqual(res, "20-00-00-e0-8b-18-20-8b")

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.HBASR._probe_hba', autospec=True)
    @mock.patch('HBASR.xml.dom.minidom.parseString', autospec=True)
    def test__init_hbas_assert(self, mock_parseString, mock_probe_hba):
        sr = HBASR.HBASR()
        mock_probe_hba.return_value = "blah"
        mock_parseString.side_effect = Exception("bad xml")
        with self.assertRaises(xs_errors.SROSError) as cm:
            sr._init_hbas()
        self.assertEqual(str(cm.exception),
                         "Unable to parse XML "
                         "[opterr=HBA scanning failed]")

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.HBASR._probe_hba', fake_probe)
    def test__init_hbas(self):
        sr = HBASR.HBASR()
        res = sr._init_hbas()
        self.assertEqual(res, {'host2': '50-01-43-80-24-26-ba-f4',
                               'host1': '50-01-43-80-24-26-ba-f4'})

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.util.pread', autospec=True)
    def test__probe_hba_assert(self, mock_pread):
        sr = HBASR.HBASR()
        mock_pread.side_effect = Exception("bad")
        with self.assertRaises(xs_errors.SROSError) as cm:
            sr._probe_hba()
        self.assertEqual(str(cm.exception),
                         "Unable to parse XML "
                         "[opterr=HBA probe failed]")

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.util.pread', autospec=True)
    @mock.patch('HBASR.util.listdir', autospec=True)
    def test__probe_hba(self, mock_listdir, mock_pread):
        sr = HBASR.HBASR()
        mock_listdir.return_value = iter(["host1", "host2"])
        # Output of preads sliced by _probe_hba to remove newlines.
        mock_pread.side_effect = iter(["nvme_special\n",
                                  "0x200000e08b18208b\n",
                                  "0x500143802426baf4\n",
                                  "toast\n",
                                  "nvme_special\n",
                                  "0x200000e08b18208b\n",
                                  "0x500143802426baf4\n",
                                  "toast\n"])
        res = sr._probe_hba()
        self.assertEqual(res, imp_fake_probe())

    @mock.patch('HBASR.HBASR.__init__', mock_init)
    @mock.patch('HBASR.HBASR._mpathHandle', autospec=True)
    def test_attach(self, mock_mpath):
        sr = HBASR.HBASR()
        sr.attach(1234)
        self.assertEqual(mock_mpath.call_count, 1)
