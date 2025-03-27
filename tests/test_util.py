import copy
import errno
import io
import os
import socket
import subprocess
import unittest
import unittest.mock as mock
import uuid
import xmlrpc.client

from sm.core import util
from sm.core import xs_errors
from sm.core import f_exceptions

DD_CMD = "/bin/dd"

TEST_HOST_IP = "192.168.13.67"
ISCSI_REFDIR = '/run/sr-ref'

# Sample Driver Info
CAPABILITIES = ["SR_PROBE", "SR_UPDATE", "SR_SUPPORTS_LOCAL_CACHING",
               "VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH",
               "VDI_UPDATE", "VDI_CLONE", "VDI_SNAPSHOT", "VDI_RESIZE",
               "VDI_MIRROR", "VDI_GENERATE_CONFIG", "VDI_RESET_ON_BOOT/2",
               "VDI_CONFIG_CBT", "THIN_PROVISIONING", "VDI_READ_CACHING"]

CONFIGURATION = [['device', 'local device path (required) (e.g. /dev/sda3)']]

DRIVER_INFO = {
    'name': 'Local EXT3 VHD',
    'description': 'SR plugin which represents disks as VHD files stored on a local EXT3 filesystem, created inside an LVM volume',  # noqa E501
    'vendor': 'Citrix Systems Inc',
    'copyright': '(C) 2008 Citrix Systems Inc',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
    }

TEST_IQN = "iqn.2009-09.com.example.test"


@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
class TestSMUtil(unittest.TestCase):
    """
    Tests for the util module methods
    """

    def setUp(self):
        # OS Patchers
        statvfs_patcher = mock.patch('sm.core.util.os.statvfs', autospec=True)
        self.mock_statvfs = statvfs_patcher.start()
        exists_patcher = mock.patch('sm.core.util.os.path.exists', autospec=True)
        self.mock_exists = exists_patcher.start()
        mkdir_patcher = mock.patch('sm.core.util.os.mkdir', autospec=True)
        self.mock_mkdir = mkdir_patcher.start()
        unlink_patcher = mock.patch('sm.core.util.os.unlink', autospec=True)
        self.mock_unlink = unlink_patcher.start()
        self.dir_contents = {}
        listdir_patcher = mock.patch('sm.core.util.os.listdir', autospec=True)
        self.mock_listdir = listdir_patcher.start()
        self.mock_listdir.side_effect = self.list_dir
        readlink_patcher = mock.patch('sm.core.util.os.readlink', autospec=True)
        self.mock_readlink = readlink_patcher.start()
        self.mock_readlink.side_effect = self.readlink

        socket_patcher = mock.patch('sm.core.util.socket', autospec=True)
        self.mock_socket = socket_patcher.start()
        self.mock_socket.AF_INET = socket.AF_INET
        self.mock_socket.SOCK_STREAM = socket.SOCK_STREAM
        self.mock_socket.IPPROTO_TCP = socket.IPPROTO_TCP
        self.mock_socket.error = socket.error
        self.mock_socket.gaierror = socket.gaierror

        sleep_patcher = mock.patch("sm.core.util.time.sleep", autospec=True)
        self.mock_sleep = sleep_patcher.start()

        xenapi_patcher = mock.patch("sm.core.util.XenAPI")
        self.mock_xenapi = xenapi_patcher.start()
        self.mock_session = mock.MagicMock()
        self.mock_xenapi.xapi_local.return_value = self.mock_session

        self.processes = {}
        popen_patcher = mock.patch('sm.core.util.subprocess.Popen', autospec=True)
        self.mock_popen = popen_patcher.start()
        self.mock_popen.side_effect = self.popen

        self.mock_files = {}

        self.addCleanup(mock.patch.stopall)

    def open(self, file_name, mode):
        assert(mode == 'r')
        mock_file = mock.MagicMock(spec=io.TextIOBase, name=file_name)
        file_data = self.file_data[file_name]
        mock_file.read.return_value = file_data
        lines = str.splitlines(file_data)
        mock_file.return_value.readlines.return_value = lines
        mock_file.__enter__ = mock_file
        mock_file.__exit__ = lambda x, y, z, a: None
        self.mock_files[file_name] = mock_file
        return mock_file

    def add_file_data(self, mock_file_data):
        self.file_data = mock_file_data
        open_patcher = mock.patch('builtins.open', autospec=True)
        self.mock_open = open_patcher.start()
        self.mock_open.side_effect = self.open

    def readlink(self, path):
        return path

    def list_dir(self, path):
        return self.dir_contents[path]

    @staticmethod
    def process_key(args):
        return ':'.join(args)

    def popen(self, args, stdin=None, stdout=None, stderr=None, close_fds=False,
              env=None, universal_newlines=None):
        return self.processes[self.process_key(args)]

    def _add_process(self, args, returncode, stdout, stderr):
        proc = mock.MagicMock()
        proc.returncode = returncode
        proc.communicate.return_value = (stdout, stderr)
        self.processes[self.process_key(args)] = proc

    def get_good_statvfs(self):
        stat_result = mock.MagicMock(spec=os.statvfs_result)
        stat_result.f_blocks = 39059200
        stat_result.f_frsize = 4096
        stat_result.f_bfree = 20000
        return stat_result

    def test_ioretry_stat_success_no_retry(self):
        self.mock_statvfs.return_value = self.get_good_statvfs()

        stat = util.ioretry_stat("/test/path/foo")

        self.assertIsNotNone(stat)
        self.mock_statvfs.assert_called_once_with("/test/path/foo")
        self.assertEqual(4096, stat.f_frsize)

    def test_ioretry_stat_retries_success(self):
        stat_fail = mock.MagicMock(spec=os.statvfs_result)
        stat_fail.f_blocks = -1

        stat_result = self.get_good_statvfs()

        side_effect = [stat_fail] * 5
        side_effect.append(stat_result)

        self.mock_statvfs.side_effect = side_effect

        stat = util.ioretry_stat("/test/path/foo")

        self.assertIsNotNone(stat)
        self.mock_statvfs.assert_has_calls([mock.call("/test/path/foo")] * 6)
        self.assertEqual(4096, stat.f_frsize)
        self.mock_sleep.assert_has_calls([mock.call(1)] * 5)

    def test_ioretry_stat_retries_failure(self):
        stat_fail = mock.MagicMock(spec=os.statvfs_result)
        stat_fail.f_blocks = -1
        self.mock_statvfs.return_value = stat_fail

        with self.assertRaises(util.CommandException) as ce:
            util.ioretry_stat("/test/path/foo")

        self.assertEqual(errno.EIO, ce.exception.code)

    def test_iotry_success(self):
        result = util.ioretry(lambda: True)

        self.assertTrue(result)

    def test_io_retry_retries_success(self):
        call_count = 0

        def dummy():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError(errno.EIO, "IO Error")
            elif call_count == 2:
                raise util.CommandException(errno.EIO)

            return True

        result = util.ioretry(dummy)

        self.assertTrue(result)

    def test_io_retry_retries_failure(self):
        def dummy():
            raise OSError(errno.EIO, "IO Error")

        with self.assertRaises(util.CommandException) as ce:
            util.ioretry(dummy)

        self.assertEqual(errno.ETIMEDOUT, ce.exception.code)

    def test_io_return_not_handled_oserror(self):
        def dummy():
            raise OSError(errno.EPERM, "Permission denied")

        with self.assertRaises(util.CommandException) as ce:
            util.ioretry(dummy)

        self.assertEqual(errno.EPERM, ce.exception.code)

    def test_io_return_not_handled_commandexception(self):
        def dummy():
            raise util.CommandException(errno.EPERM)

        with self.assertRaises(util.CommandException) as ce:
            util.ioretry(dummy)

        self.assertEqual(errno.EPERM, ce.exception.code)

    def test_get_sr_capability_none(self):
        # Arrange
        sr_uuid = str(uuid.uuid4())
        xenapi = self.mock_session.xenapi
        xenapi.SR.get_record.return_value = {
            "type": "test_sm"
        }
        sm_records = {}
        xenapi.SM.get_all_records_where.return_value = sm_records

        # Act
        result = util.sr_get_capability(sr_uuid)

        # Assert
        self.assertEqual([], result)

        xenapi.SM.get_all_records_where.assert_called_with(
            'field "type" = "test_sm"')

    def test_get_sr_capability(self):
        # Arrange
        sr_uuid = str(uuid.uuid4())
        xenapi = self.mock_session.xenapi
        xenapi.SR.get_record.return_value = {
            "type": "test_sm"
        }
        sm_records = {
            "OpaqueRef:d64df177-b286-44dd-8dbd-d50f1917a41e": DRIVER_INFO
        }
        xenapi.SM.get_all_records_where.return_value = sm_records

        # Act
        result = util.sr_get_capability(sr_uuid)

        # Assert
        self.assertEqual(17, len(result))

        xenapi.SM.get_all_records_where.assert_called_with(
            'field "type" = "test_sm"')

    def test_return_nil(self):
        result = util.return_nil()

        self.assertEqual(
            "<?xml version='1.0'?>\n<methodResponse>\n<params>\n<param>\n<value><nil/></value></param>\n</params>\n</methodResponse>\n",  # noqa E501
            result)

    def test_get_driver_info_no_atomic_pause(self):
        result = util.sr_get_driver_info(DRIVER_INFO)

        self.assertIsNotNone(result)
        decoded_result = xmlrpc.client.loads(result)[0][0]

        expected_result = copy.deepcopy(DRIVER_INFO)
        # Configuration is changed to a dictionary
        expected_result['configuration'] = [
            {
                "key": "device",
                "description": "local device path (required) (e.g. /dev/sda3)"
            }
        ]
        self.assertDictEqual(expected_result, decoded_result)

    def test_get_fs_size(self):
        self.mock_statvfs.return_value = self.get_good_statvfs()

        result = util.get_fs_size("/test/path/foo")

        self.assertEqual(4096 * 39059200, result)

    def test_get_fs_utilisation(self):
        self.mock_statvfs.return_value = self.get_good_statvfs()

        result = util.get_fs_utilisation("/test/path/foo")

        self.assertEqual(4096 * (39059200 - 20000), result)

    @mock.patch('sm.core.util.get_this_host', autospec=True)
    def test_get_slaves_attached_on_none(self, mock_get_this_host):
        # Arrange
        vdi_uuids = [str(uuid.uuid4())]
        xenapi = self.mock_session.xenapi
        xenapi.VDI.get_sm_config.return_value = {}
        mock_get_this_host.return_value = str(uuid.uuid4())

        # Act
        attached_on = util.get_slaves_attached_on(
            self.mock_session, vdi_uuids)

        # Assert
        self.assertEqual(0, len(attached_on))
        xenapi.VDI.get_by_uuid.assert_called_once_with(vdi_uuids[0])

    @mock.patch('sm.core.util.get_this_host', autospec=True)
    def test_get_slaves_attached_on_master_only(self, mock_get_this_host):
        # Arrange
        master_ref = "OpaqueRef:dc038e5d-4bed-4c9c-b3c6-7af9ed16b339"
        xenapi = self.mock_session.xenapi

        master_uuid = str(uuid.uuid4())
        mock_get_this_host.return_value = master_uuid
        xenapi.host.get_by_uuid.return_value = master_ref
        vdi_uuids = [str(uuid.uuid4())]
        xenapi.VDI.get_sm_config.return_value = {
            f"host_{master_ref}": "RW"}

        # Act
        attached_on = util.get_slaves_attached_on(
            self.mock_session, vdi_uuids)

        # Assert
        self.assertEqual(0, len(attached_on))
        xenapi.VDI.get_by_uuid.assert_called_once_with(vdi_uuids[0])

    @mock.patch('sm.core.util.get_this_host', autospec=True)
    def test_get_slaves_attached(self, mock_get_this_host):
        # Arrange
        master_ref = "OpaqueRef:dc038e5d-4bed-4c9c-b3c6-7af9ed16b339"
        slave_ref = "OpaqueRef:dc038e5d-4bed-4c5c-b6c6-7af9ed16b339"
        xenapi = self.mock_session.xenapi

        master_uuid = str(uuid.uuid4())
        mock_get_this_host.return_value = master_uuid
        xenapi.host.get_by_uuid.return_value = master_ref
        vdi_uuids = [str(uuid.uuid4())]
        xenapi.VDI.get_sm_config.return_value = {
            f"host_{slave_ref}": "RW"}

        # Act
        attached_on = util.get_slaves_attached_on(
            self.mock_session, vdi_uuids)

        # Assert
        self.assertEqual(1, len(attached_on))
        xenapi.VDI.get_by_uuid.assert_called_once_with(vdi_uuids[0])

    @mock.patch('sm.core.util.get_this_host', autospec=True)
    def test_get_all_slaves_none(self, mock_get_this_host):
        # Arrange
        master_ref = "OpaqueRef:dc038e5d-4bed-4c9c-b3c6-7af9ed16b339"
        metrics_ref = "OpaqueRef:9abde890-b0ec-40c0-8cbc-48e179e8a5cc"
        xenapi = self.mock_session.xenapi
        master_uuid = str(uuid.uuid4())
        mock_get_this_host.return_value = master_uuid
        xenapi.host.get_by_uuid.return_value = master_ref
        xenapi.host.get_all_records.return_value = {
            master_ref: {
                "metrics": metrics_ref
            }
        }
        xenapi.host_metrics.get_record.return_value = {
            "live": True
        }

        # Act
        slaves = util.get_all_slaves(self.mock_session)

        # Assert
        self.assertEqual(0, len(slaves))
        xenapi.host_metrics.get_record.assert_called_once_with(metrics_ref)

    @mock.patch('sm.core.util.get_this_host', autospec=True)
    def test_get_all_slaves_one_offline(self, mock_get_this_host):
        # Arrange
        master_ref = "OpaqueRef:dc038e5d-4bed-4c9c-b3c6-7af9ed16b339"
        slave_ref = "OpaqueRef:dc038e5d-4bed-4c5c-b6c6-7af9ed16b339"
        metrics_ref = "OpaqueRef:9abde890-b0ec-40c0-8cbc-48e179e8a5cc"
        slave_metrics_ref = "OpaqueRef:9abde893-b7ec-40c0-8cbc-48e179e8a5cc"
        xenapi = self.mock_session.xenapi
        master_uuid = str(uuid.uuid4())
        mock_get_this_host.return_value = master_uuid
        xenapi.host.get_by_uuid.return_value = master_ref
        xenapi.host.get_all_records.return_value = {
            master_ref: {
                "metrics": metrics_ref
            },
            slave_ref: {
                "metrics": slave_metrics_ref
            }
        }
        metrics = {
            metrics_ref: {
                "live": True
            },
            slave_metrics_ref: {
                "live": False
            }
        }

        def get_metrics(ref):
            return metrics[ref]

        xenapi.host_metrics.get_record.side_effect = get_metrics

        # Act
        slaves = util.get_all_slaves(self.mock_session)

        # Assert
        self.assertEqual(0, len(slaves))
        xenapi.host_metrics.get_record.assert_has_calls([
            mock.call(metrics_ref), mock.call(slave_metrics_ref)],
            any_order=True)

    @mock.patch('sm.core.util.get_this_host', autospec=True)
    def test_get_all_slaves_one_online(self, mock_get_this_host):
        # Arrange
        master_ref = "OpaqueRef:dc038e5d-4bed-4c9c-b3c6-7af9ed16b339"
        slave_ref = "OpaqueRef:dc038e5d-4bed-4c5c-b6c6-7af9ed16b339"
        metrics_ref = "OpaqueRef:9abde890-b0ec-40c0-8cbc-48e179e8a5cc"
        slave_metrics_ref = "OpaqueRef:9abde893-b7ec-40c0-8cbc-48e179e8a5cc"
        xenapi = self.mock_session.xenapi
        master_uuid = str(uuid.uuid4())
        mock_get_this_host.return_value = master_uuid
        xenapi.host.get_by_uuid.return_value = master_ref
        xenapi.host.get_all_records.return_value = {
            master_ref: {
                "metrics": metrics_ref
            },
            slave_ref: {
                "metrics": slave_metrics_ref
            }
        }
        metrics = {
            metrics_ref: {
                "live": True
            },
            slave_metrics_ref: {
                "live": True
            }
        }

        def get_metrics(ref):
            return metrics[ref]

        xenapi.host_metrics.get_record.side_effect = get_metrics

        # Act
        slaves = util.get_all_slaves(self.mock_session)

        # Assert
        self.assertEqual(1, len(slaves))
        xenapi.host_metrics.get_record.assert_has_calls([
            mock.call(metrics_ref), mock.call(slave_metrics_ref)],
            any_order=True)

    def test_incr_iscsi_refcount_no_dir(self):
        # Arrange
        self.mock_exists.return_value = False
        sr_uuid = str(uuid.uuid4())

        file_contents = ""
        # Act
        with mock.patch('builtins.open',
                        new_callable=mock.mock_open(
                            read_data=file_contents)) as mock_file:
            refcount = util._incr_iscsiSR_refcount(TEST_IQN, sr_uuid)

        # Assert
        self.assertEqual(1, refcount)
        mock_file.return_value.seek.assert_called_once_with(0)
        mock_file.return_value.write.assert_called_once_with(f"{sr_uuid}\n")
        self.mock_mkdir.assert_called_once_with(ISCSI_REFDIR)

    def test_incr_iscsi_refcount_one_existing(self):
        # Arrange
        self.mock_exists.return_value = True
        sr_uuid = str(uuid.uuid4())
        other_ref = str(uuid.uuid4())

        # f.readlines() includes the trailing \n
        file_contents = [f"{other_ref}\n"]

        # Act
        with mock.patch('builtins.open') as mock_file:
            mock_file.return_value.readlines.return_value = file_contents
            refcount = util._incr_iscsiSR_refcount(TEST_IQN, sr_uuid)

        # Assert
        self.assertEqual(2, refcount)
        mock_file.return_value.write.assert_called_once_with(f"{sr_uuid}\n")
        self.mock_mkdir.assert_not_called()

    def test_incr_iscsi_refcount_already_present(self):
        # Arrange
        self.mock_exists.return_value = True
        sr_uuid = str(uuid.uuid4())
        other_ref = str(uuid.uuid4())

        file_contents = [f"{other_ref}\n", f"{sr_uuid}\n"]

        # Act
        with mock.patch('builtins.open') as mock_file:
            mock_file.return_value.readlines.return_value = file_contents
            refcount = util._incr_iscsiSR_refcount(TEST_IQN, sr_uuid)

        # Assert
        self.assertEqual(2, refcount)
        mock_file.return_value.write.assert_not_called()
        self.mock_mkdir.assert_not_called()

    def test_decr_iscsi_refcount_to_zero(self):
        # Arrange
        self.mock_exists.return_value = True
        sr_uuid = str(uuid.uuid4())

        file_contents = [f"{sr_uuid}\n"]

        # Act
        with mock.patch('builtins.open') as mock_file:
            mock_file.return_value.readlines.return_value = file_contents
            refcount = util._decr_iscsiSR_refcount(TEST_IQN, sr_uuid)

        self.assertEqual(0, refcount)
        self.mock_unlink.assert_called_once_with(os.path.join(ISCSI_REFDIR, TEST_IQN))
        mock_file.return_value.seek.assert_called_once_with(0)

    def test_decr_iscsi_refcount_one_left(self):
        # Arrange
        self.mock_exists.return_value = True
        sr_uuid = str(uuid.uuid4())
        other_ref = str(uuid.uuid4())

        file_contents = [f"{other_ref}\n", f"{sr_uuid}\n"]

        # Act
        with mock.patch('builtins.open') as mock_file:
            mock_file.return_value.readlines.return_value = file_contents
            refcount = util._decr_iscsiSR_refcount(TEST_IQN, sr_uuid)

        self.assertEqual(1, refcount)
        self.mock_unlink.assert_not_called()
        mock_file.return_value.write.assert_called_once_with(f"{other_ref}\n")

    def test_decr_iscsi_refcount_noref(self):
        # Arrange
        self.mock_exists.return_value = False
        sr_uuid = str(uuid.uuid4())

        # Act
        refcount = util._decr_iscsiSR_refcount(TEST_IQN, sr_uuid)

        self.assertEqual(0, refcount)
        self.mock_unlink.assert_not_called()

    def test_host_success(self):
        # Arrange
        sock_addr = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP,
                     "", ("192.168.1.2", 3260))
        self.mock_socket.getaddrinfo.return_value = [sock_addr]

        # Act
        util._testHost("test-server", 3260, "iSCSI testhost")

        # Assert
        self.mock_socket.getaddrinfo.assert_called_with("test-server", 3260)
        self.mock_socket.socket.assert_called_once_with(
            socket.AF_INET, socket.SOCK_STREAM)
        open_socket = self.mock_socket.socket.return_value
        open_socket.connect.assert_called_with(("192.168.1.2", 3260))
        open_socket.send.assert_called_once_with(b"\n")

    def test_host_dns_lookup_failure(self):
        # Arrange
        self.mock_socket.getaddrinfo.side_effect = socket.gaierror(errno.ENOENT)

        # Act
        with self.assertRaises(xs_errors.SROSError) as sroe:
            util._testHost("test-server", 3260, "ISCSITarget")

        self.assertEqual(140, sroe.exception.errno)

    def test_host_connect_failure(self):
        # Arrange
        sock_addr = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP,
                     "", ("192.168.1.2", 3260))
        self.mock_socket.getaddrinfo.return_value = [sock_addr]
        open_socket = self.mock_socket.socket.return_value
        open_socket.connect.side_effect = socket.error("Timed out")

        # Act
        with self.assertRaises(xs_errors.SROSError) as sroe:
            util._testHost("test-server", 3260, "ISCSITarget")

        # Assert
        self.mock_socket.getaddrinfo.assert_called_with("test-server", 3260)
        self.mock_socket.socket.assert_called_once_with(
            socket.AF_INET, socket.SOCK_STREAM)
        open_socket = self.mock_socket.socket.return_value
        open_socket.connect.assert_called_with(("192.168.1.2", 3260))
        self.assertEqual(141, sroe.exception.errno)

    def test_zero_out_two_aligned_blocks(self):
        # Arrange
        self._add_process(
            [DD_CMD, "if=/dev/zero", "of=/test/path/foo", "bs=4096",
             "seek=0", "count=2"],
            0, b"", b"")
        # Act
        result = util.zeroOut('/test/path/foo', 0, 8192)

        # Assert
        self.assertTrue(result)

    def test_zero_out_misaligned_blocks(self):
        # Arrange
        self._add_process(
            [DD_CMD, "if=/dev/zero", "of=/test/path/foo", "bs=1",
             "seek=1024", "count=3072"],
            0, b"", b"")
        self._add_process(
            [DD_CMD, "if=/dev/zero", "of=/test/path/foo", "bs=1",
             "seek=4096", "count=100"],
            0, b"", b"")

        # Act
        result = util.zeroOut('/test/path/foo', 1024, 3172)

        # Assert
        self.assertTrue(result)

    def test_zero_out_small_block(self):
        # Arrange
        self._add_process(
            [DD_CMD, "if=/dev/zero", "of=/test/path/foo", "bs=1",
             "seek=1024", "count=2048"],
            0, b"", b"")

        # Act
        result = util.zeroOut('/test/path/foo', 1024, 2048)

        # Assert
        self.assertTrue(result)

    def test_find_running_process(self):
        # Arrange
        self.dir_contents['/proc'] = [str(17416), str(17414), str(17417)]
        tapdisk_unix_data = """
00000000f1cc0a81: 00000002 00000000 00000000 0002 01 23755
00000000728fbd2a: 00000002 00000000 00010000 0001 01 14525476 /run/blktap-control/nbd17416.1
00000000a68a75cf: 00000003 00000000 00000000 0001 03 14522812 /run/blktap-control/nbd17405.0
        """

        mock_file_data = {
            '/proc/17416/cmdline': 'tapdisk\x00\n',
            '/proc/17414/cmdline': 'bash\x00\n',
            '/proc/17417/cmdline': 'bash\x00\n',
            '/proc/17416/net/unix': tapdisk_unix_data
        }
        self.add_file_data(mock_file_data)
        self.dir_contents.update(
            {
                '/proc/17414/fd': [],
                '/proc/17417/fd': [],
                '/proc/17416/fd': ['/dev/zero']
            }
        )

        # Act
        retval, links, sockets = util.findRunningProcessOrOpenFile('tapdisk')

        # Assert
        self.assertTrue(retval)
        self.assertSetEqual({'/run/blktap-control/nbd17405.0'}, sockets)

    def test_unictrunc(self):
        # Successive chars in this string have 1, 2, 3, and 4 byte encodings.
        # So the number of bytes required to encode some prefix of it will be
        # a triangle number.
        t = "X\u00f6\u732b\U0001f3f9"
        s = "X\u00f6\u732b\U0001f3f9".encode("utf-8")

        self.assertEqual(util.unictrunc(s, 10), 10)
        self.assertEqual(util.unictrunc(s, 9), 6)
        self.assertEqual(util.unictrunc(s, 8), 6)
        self.assertEqual(util.unictrunc(s, 7), 6)
        self.assertEqual(util.unictrunc(s, 6), 6)
        self.assertEqual(util.unictrunc(s, 5), 3)
        self.assertEqual(util.unictrunc(s, 4), 3)
        self.assertEqual(util.unictrunc(s, 3), 3)
        self.assertEqual(util.unictrunc(s, 2), 1)
        self.assertEqual(util.unictrunc(s, 1), 1)
        self.assertEqual(util.unictrunc(s, 0), 0)

        self.assertEqual(util.unictrunc(t, 10), 4)
        self.assertEqual(util.unictrunc(t, 9), 3)
        self.assertEqual(util.unictrunc(t, 8), 3)
        self.assertEqual(util.unictrunc(t, 7), 3)
        self.assertEqual(util.unictrunc(t, 6), 3)
        self.assertEqual(util.unictrunc(t, 5), 2)
        self.assertEqual(util.unictrunc(t, 4), 2)
        self.assertEqual(util.unictrunc(t, 3), 2)
        self.assertEqual(util.unictrunc(t, 2), 1)
        self.assertEqual(util.unictrunc(t, 1), 1)
        self.assertEqual(util.unictrunc(t, 0), 0)


@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
class TestFistPoints(unittest.TestCase):
    def setUp(self):
        self.addCleanup(mock.patch.stopall)
        sleep_patcher = mock.patch('sm.core.util.time.sleep', autospec=True)
        self.mock_sleep = sleep_patcher.start()

        log_patcher = mock.patch('sm.core.util.SMlog', autospec=True)
        self.mock_log = log_patcher.start()

        exists_patcher = mock.patch('sm.core.util.os.path.exists', autospec=True)
        self.mock_exists = exists_patcher.start()
        self.mock_exists.side_effect = self.exists
        self.existing_files = set()

        xenapi_patcher = mock.patch('sm.core.util.XenAPI', autospec=True)
        patched_xenapi = xenapi_patcher.start()
        self.mock_xenapi = mock.MagicMock()
        patched_xenapi.xapi_local.return_value = self.mock_xenapi

    def exists(self, path):
        return path in self.existing_files

    def test_activate_unknown(self):
        test_uuid = str(uuid.uuid4())
        valid_fp_name = "TestValidFP"
        invalid_fp_name = "TestInvalidFP"

        fistpoints = util.FistPoint([valid_fp_name])

        fistpoints.activate(invalid_fp_name, test_uuid)

        # Assert
        self.assertIn('Unknown fist point: TestInvalidFP',
                      self.mock_log.call_args[0][0])

    def test_activate_not_active(self):
        test_uuid = str(uuid.uuid4())
        valid_fp_name = "TestValidFP"

        fistpoints = util.FistPoint([valid_fp_name])

        fistpoints.activate(valid_fp_name, test_uuid)

        # Assert (no side effect should have happened
        self.mock_xenapi.xenapi.SR.add_to_other_config.assert_not_called()
        self.mock_xenapi.xenapi.SR.remove_from_other_config.assert_not_called()
        self.mock_sleep.assert_not_called()

    def test_activate_not_exit(self):
        test_uuid = str(uuid.uuid4())
        valid_fp_name = "TestValidFP"
        self.existing_files.add(os.path.join('/tmp', f'fist_{valid_fp_name}'))

        fistpoints = util.FistPoint([valid_fp_name])

        fistpoints.activate(valid_fp_name, test_uuid)

        # Assert
        self.mock_xenapi.xenapi.SR.add_to_other_config.assert_called_once_with(
            mock.ANY, valid_fp_name, "active")
        self.mock_xenapi.xenapi.SR.remove_from_other_config.assert_called_once_with(
            mock.ANY, valid_fp_name)
        self.mock_xenapi.xenapi.session.logout.assert_has_calls([mock.call(), mock.call()])
        self.mock_sleep.assert_called_once_with(util.FIST_PAUSE_PERIOD)


@mock.patch('sm.core.xs_errors.XML_DEFS', 'libs/sm/core/XE_SR_ERRORCODES.xml')
class TestCoreUtil(unittest.TestCase):

    def setUp(self):
        syslog_patcher = mock.patch("sm.core.util.syslog", autospec=True)
        self.mock_syslog = syslog_patcher.start()
        sleep_patcher = mock.patch("sm.core.util.time.sleep", autospec=True)
        self.mock_sleep = sleep_patcher.start()
        socket_patcher = mock.patch("sm.core.util.socket", autospec=True)
        self.mock_socket = socket_patcher.start()
        self.mock_socket.error = socket.error
        self.mock_socket.gaierror = socket.gaierror
        subprocess_patcher = mock.patch('sm.core.util.subprocess', autoapec=True)
        self.mock_subprocess = subprocess_patcher.start()
        self.mock_subprocess.PIPE = subprocess.PIPE

        self.addCleanup(mock.patch.stopall)

    def test_retry_success(self):
        util.retry(lambda: True, maxretry=3, period=4)

    def test_retry_success_after_retry(self):
        results = [False, False, True]

        def create_result():
            result = results.pop(0)
            if not result:
                raise Exception("Test failed")

        util.retry(lambda: create_result(), maxretry=3, period=4)

    def test_retry_retry_unsuccessful(self):

        def create_result():
            raise Exception("Test failed")

        with self.assertRaises(Exception):
            util.retry(lambda: create_result(), maxretry=3, period=4)

    def test_test_host_success(self):
        # Arrange

        sock_info = (socket.AF_INET, socket.SOCK_STREAM, 6, '', (TEST_HOST_IP, 3260))
        self.mock_socket.getaddrinfo.return_value = [sock_info, (), ()]

        # Act
        result = util.testHost(TEST_HOST_IP, 3260)

        # Assert
        self.assertTrue(result)
        self.mock_socket.getaddrinfo.assert_called_with(TEST_HOST_IP, 3260)
        self.mock_socket.socket.return_value.settimeout.assert_called_with(10)
        self.mock_socket.socket.return_value.connect.assert_called_with(
            (TEST_HOST_IP, 3260))

    def test_test_host_connect_failure(self):
        # Arrange

        sock_info = (socket.AF_INET, socket.SOCK_STREAM, 6, '', (TEST_HOST_IP, 3260))
        self.mock_socket.getaddrinfo.return_value = [sock_info, (), ()]
        self.mock_socket.socket.return_value.connect.side_effect = socket.error

        # Act
        result = util.testHost(TEST_HOST_IP, 3260)

        # Assert
        self.assertFalse(result)

    def test_test_host_addrinfo_failure(self):
        # Arrange

        sock_info = (socket.AF_INET, socket.SOCK_STREAM, 6, '', (TEST_HOST_IP, 3260))
        self.mock_socket.getaddrinfo.side_effect = socket.gaierror

        # Act
        result = util.testHost(TEST_HOST_IP, 3260)

        # Assert
        self.assertFalse(result)

    def test_doexec_success(self):
        mock_process = mock.create_autospec(subprocess.Popen)
        mock_process.communicate.return_value= (b"Some out", b"Some Err")
        mock_process.returncode = 0
        self.mock_subprocess.Popen.return_value = mock_process

        # Act
        (ret, stdout, stderr) = util.doexec(['/bin/some/cmd'])

        # Assert
        self.mock_subprocess.Popen.assert_called_with(
            ['/bin/some/cmd'], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            close_fds=True, env=None, universal_newlines=True)
        self.assertEqual(0, ret)
        # The ONLY caller of util.doexec() from the old sm-core-libs which is not
        # inside this package is a call which ignores stdout and stderr, so we are
        # safe to alter this unit test.
        self.assertEqual(b"Some out", stdout)
        self.assertEqual(b"Some Err", stderr)
