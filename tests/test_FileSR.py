import errno
import mock
import nfs
import NFSSR # Without this the FileSR won't import
import FileSR
import os
import unittest
import uuid


class FakeFileVDI(FileSR.FileVDI):
     def __init__(self, srcmd, none):
         pass


class TestFileVDI(unittest.TestCase):
    @mock.patch('os.stat')
    def test_find_vhd_path(self, stat):
        vdi_uuid=uuid.uuid4()
        vdi = FakeFileVDI("a command", None)
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi.sr = sr
        stat.side_effect = [os.stat_result((stat.S_IFREF, 0, 0, 0, 0, 0, 1024, 0, 0, 0)) ]

        found = vdi._find_path_with_retries(vdi_uuid)

        self.assertTrue(found)
        expected_path = 'sr_path/%s.vhd' % vdi_uuid
        stat.assert_called_with(expected_path)
        self.assertEqual(vdi.path, expected_path)

    @mock.patch('os.stat')
    def test_find_raw_path(self, stat):
        vdi_uuid=uuid.uuid4()
        vdi = FakeFileVDI("a command", None)
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi.sr = sr
        stat.side_effect = [OSError(errno.ENOENT),
                            os.stat_result((stat.S_IFREF, 0, 0, 0, 0, 0, 1024, 0, 0, 0)) ]

        found = vdi._find_path_with_retries(vdi_uuid)

        self.assertTrue(found)
        expected_path = 'sr_path/%s.raw' % vdi_uuid
        stat.assert_called_with(expected_path)
        self.assertEqual(vdi.path, expected_path)

    @mock.patch('time.sleep')
    @mock.patch('os.stat')
    def test_find_retry_vhd_path(self, stat, sleep):
        vdi_uuid=uuid.uuid4()
        vdi = FakeFileVDI("a command", None)
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi.sr = sr
        stat.side_effect = [OSError(errno.ENOENT),
                            OSError(errno.ENOENT),
                            os.stat_result((stat.S_IFREF, 0, 0, 0, 0, 0, 1024, 0, 0, 0)) ]

        found = vdi._find_path_with_retries(vdi_uuid)

        self.assertTrue(found)
        expected_path = 'sr_path/%s.vhd' % vdi_uuid
        stat.assert_called_with(expected_path)
        self.assertEqual(vdi.path, expected_path)

    @mock.patch('time.sleep')
    @mock.patch('os.stat')
    def test_find_not_found(self, stat, sleep):
        vdi_uuid=uuid.uuid4()
        vdi = FakeFileVDI("a command", None)
        sr = mock.MagicMock()
        sr.path = "sr_path"
        vdi.sr = sr
        stat.side_effect = OSError(errno.ENOENT)

        found = vdi._find_path_with_retries(vdi_uuid)

        self.assertFalse(found)
