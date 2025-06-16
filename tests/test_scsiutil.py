import unittest
import unittest.mock as mock

import io

from sm.core import scsiutil

import testlib


class Test_sg_readcap(unittest.TestCase):

    def verify_sg_readcap(self, doexec, expected_result):
        result = scsiutil.sg_readcap('/dev/sda')
        doexec.assert_called_with(['/usr/bin/sg_readcap', '-b', '/dev/sda'])
        self.assertEqual(result, expected_result)

    @mock.patch('sm.core.scsiutil.util.doexec', autospec=True)
    def test_sg_readcap_10(self, doexec):
        fake_out = "0x3a376030 0x200\n"
        doexec.return_value = (0, fake_out, '')
        self.verify_sg_readcap(doexec, 500074307584)

    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('sm.core.scsiutil.util.doexec')
    def test_capacity_data_changed_rc6(self, doexec):
        fake_out = "0x3a376030 0x200\n"
        doexec.side_effect = [(6, 'something else', ''), (0, fake_out, '')]
        self.verify_sg_readcap(doexec, 500074307584)

    @mock.patch('sm.core.scsiutil.util.doexec', autospec=True)
    def test_sg_readcap_16(self, doexec):
        fake_out = ("READ CAPACITY (10) indicates device capacity too large\n"
                    "now trying 16 byte cdb variant\n"
                    "0x283d8e000 0x200\n")
        doexec.return_value = (0, fake_out, '')
        self.verify_sg_readcap(doexec, 5530605060096)

    @testlib.with_context
    def test_refreshdev(self, context):
        adapter = context.add_adapter(testlib.SCSIAdapter())
        adapter.add_disk()

        scsiutil.refreshdev(["/dev/sda"])

class TestScsiUtil(unittest.TestCase):

    def setUp(self):
        exists_patcher = mock.patch(
            'sm.core.scsiutil.os.path.exists', autospece=True)
        self.mock_exists = exists_patcher.start()
        self.mock_exists.side_effect = self.exists
        self.path_map = {}
        realpath_patcher = mock.patch(
            'sm.core.scsiutil.os.path.realpath', autospec=True)
        self.mock_realpath = realpath_patcher.start()
        self.mock_realpath.side_effect = self.realpath

        self.mock_files = {}
        listdir_patcher = mock.patch(
            'sm.core.scsiutil.os.listdir', autospec=True)
        self.mock_listdir = listdir_patcher.start()
        self.mock_listdir.side_effect = self.listdir

        glob_patcher = mock.patch('sm.core.scsiutil.glob.glob', autospec=True)
        self.mock_glob = glob_patcher.start()

        self.addCleanup(mock.patch.stopall)

    def realpath(self, path):
        return self.path_map.get(path, path)

    def listdir(self, path):
        return self.path_map.get(path, [])

    def add_file_data(self, mock_file_data):
        self.file_data = mock_file_data
        open_patcher = mock.patch('builtins.open', autospec=True)
        self.mock_open = open_patcher.start()
        self.mock_open.side_effect = self.open

    def open(self, file_name, mode):
        assert(mode == 'r')
        mock_file = mock.MagicMock(spec=io.TextIOBase, name=file_name)
        file_data = self.file_data[file_name]
        mock_file.read.return_value = file_data
        lines = str.splitlines(file_data)
        mock_file.return_value.readlines.return_value = lines
        mock_file.return_value.readline.return_value = lines[0]
        mock_file.__enter__ = mock_file
        mock_file.__exit__ = lambda x, y, z, a: None
        self.mock_files[file_name] = mock_file
        return mock_file

    def exists(self, path):
        return path in self.path_map

    def test_get_size_exists_success(self):
        self.path_map = {
            '/sys/block/sda/size': True
        }

        with mock.patch("builtins.open",
                        new_callable=mock.mock_open, read_data='976773168') as m:
            # Nastiness due to python2 mock_open not supporting readline
            m.return_value.readline.side_effect = [ "976773168" ]
            size = scsiutil.getsize("/dev/sda")

        self.assertEqual(500107862016, size)
        m.assert_called_with("/sys/block/sda/size", "r")

    def test_get_size_mapper_exists_success(self):
        self.path_map = {
            '/sys/block/sde/size': True,
            "/dev/disk/by-id/scsi-360a98000534b4f4e46704c76692d6d33": "/dev/sde"}

        with mock.patch("builtins.open",
                        new_callable=mock.mock_open, read_data='976773168') as m:
            # Nastiness due to python2 mock_open not supporting readline
            m.return_value.readline.side_effect = [ "976773168" ]
            size = scsiutil.getsize("/dev/mapper/360a98000534b4f4e46704c76692d6d33")

        self.assertEqual(500107862016, size)
        m.assert_called_with("/sys/block/sde/size", "r")

    def test_get_size_not_exists_0(self):
        with mock.patch("builtins.open",
                        new_callable=mock.mock_open, read_data='976773168') as m:
            # Nastiness due to python2 mock_open not supporting readline
            m.return_value.readline.side_effect = [ "976773168" ]
            size = scsiutil.getsize("/dev/sda")

        self.assertEqual(0, size)

    def test_lun_is_not_thin_provisioned(self):
        # Arrange
        self.path_map = {
            '/dev/disk/by-scsid/360a98000534b4f4e46704c76692d6d33': ['sde'],
        }
        sde_mode = '/sys/block/sde/device/scsi_disk/2:0:0:0/provisioning_mode'
        self.mock_glob.return_value = [sde_mode]
        self.add_file_data({sde_mode: 'full\n'})

        # Act
        thin_provisioned = scsiutil.device_is_thin_provisioned('360a98000534b4f4e46704c76692d6d33')

        #  Assert
        self.assertFalse(thin_provisioned)

    def test_lun_is_thin_provisioned(self):
        # Arrange
        self.path_map = {
            '/dev/disk/by-scsid/360a98000534b4f4e46704c76692d6d33': ['mapper', 'sde'],
        }
        sde_mode = '/sys/block/sde/device/scsi_disk/2:0:0:0/provisioning_mode'
        self.mock_glob.return_value = [sde_mode]
        self.add_file_data({sde_mode: 'unmap\n'})

        # Act
        thin_provisioned = scsiutil.device_is_thin_provisioned('360a98000534b4f4e46704c76692d6d33')

        #  Assert
        self.assertTrue(thin_provisioned)

    def test_lun_is_thin_provisioned_not_found(self):
        self.path_map = {}

        # Act
        thin_provisioned = scsiutil.device_is_thin_provisioned('360a98000534b4f4e46704c76692d6d33')

        # Assert
        self.assertFalse(thin_provisioned)
