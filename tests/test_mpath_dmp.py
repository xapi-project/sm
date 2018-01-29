"""
Unit tests for mpath dmp
"""
import errno
import os
import unittest
import mock
import testlib
import mpath_dmp
import SR


# pylint: disable=W0613; mocks don't need to be accessed
# pylint: disable=R0201; methods must be instance for nose to work
# pylint: disable=W0212; unit tests are permitted to snoop
class TestMpathDmp(unittest.TestCase):
    """
    Unit tests for mpath dmp
    """

    @testlib.with_context
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_is_valid_multipath_device(self, context, util_mod):
        """
        Tests for checking validity of multipath device
        """

        # Setup errors codes
        context.setup_error_codes()

        # Test 'multipath -a' failure
        util_mod.doexec.side_effect = [(1, "out", "err")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        # Test 'multipath -c' with error and empty output
        util_mod.doexec.side_effect = [(0, "out", "err"), (1, "", ""), OSError()]
        with self.assertRaises(SR.SROSError) as exc:
            mpath_dmp._is_valid_multipath_device("xx")
        self.assertEqual(exc.exception.errno, 431)

        # Test 'multipath -c' with error but some output
        util_mod.doexec.side_effect = [(0, "out", "err"), (1, "xx", "")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))
        util_mod.doexec.side_effect = [(0, "out", "err"), (1, "xx", "yy")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))
        util_mod.doexec.side_effect = [(0, "out", "err"), (1, "", "yy")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        # Test when everything is fine
        util_mod.doexec.side_effect = [(0, "out", "err"), (0, "out", "err")]
        self.assertTrue(mpath_dmp._is_valid_multipath_device("fake_dev"))

    @mock.patch('mpath_dmp.mpp_luncheck.is_RdacLun', autospec=True)
    @mock.patch('util.pread2', autospec=True)
    @mock.patch('mpath_dmp.os.mkdir', autospec=True)
    def test_activate_no_exception(self, mock_mkdir, pread2, is_rdaclun):
        """
        Test that activeate MPDev works if directory does not exist
        """
        is_rdaclun.return_value = False
        mpath_dmp.activate_MPdev("sid", "dst")
        pread2.assert_called_with(['ln', '-sf', "dst", os.path.join(mpath_dmp.MP_INUSEDIR, "sid")])

    @mock.patch('mpath_dmp.mpp_luncheck.is_RdacLun', autospec=True)
    @mock.patch('util.pread2', autospec=True)
    @mock.patch('mpath_dmp.os.mkdir', autospec=True)
    def test_activate_exists_success(self, mock_mkdir, pread2, is_rdaclun):
        """
        Test that activeate MPDev works if directory exists
        """
        mock_mkdir.side_effect = [OSError(errno.EEXIST, "Directory exists")]
        is_rdaclun.return_value = False
        mpath_dmp.activate_MPdev("sid", "dst")
        pread2.assert_called_with(['ln', '-sf', "dst", os.path.join(mpath_dmp.MP_INUSEDIR, "sid")])

    @mock.patch('mpath_dmp.os.mkdir', autospec=True)
    def test_activate_permission_denied(self, mock_mkdir):
        """
        Test that activeate MPDev works if mkdir returns permission denied
        """
        mock_mkdir.side_effect = [OSError(errno.EPERM, "Permission denied")]
        with self.assertRaises(OSError) as context:
            mpath_dmp.activate_MPdev("sid", "dst")

        self.assertEqual(errno.EPERM, context.exception.errno)
