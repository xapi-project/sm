"""
Unit tests for mpath dmp
"""
from __future__ import print_function
import errno
import os
import unittest
import mock
import testlib
import util

import mpath_dmp
import SR
from SR import SROSError

from Queue import Queue

import xs_errors


# pylint: disable=W0613; mocks don't need to be accessed
# pylint: disable=R0201; methods must be instance for nose to work
# pylint: disable=W0212; unit tests are permitted to snoop
class TestMpathDmp(unittest.TestCase):
    """
    Unit tests for mpath dmp
    """

    def setUp(self):
        time_patcher = mock.patch('mpath_dmp.time', autospec=True)
        self.mock_time = time_patcher.start()

        mpath_cli_patcher = mock.patch('mpath_dmp.mpath_cli', autospec=True)
        self.mock_mpath_cli = mpath_cli_patcher.start()

        self.addCleanup(mock.patch.stopall)

    @testlib.with_context
    @mock.patch('mpath_dmp.util', autospec=True)
    @mock.patch('mpath_dmp.os', autospec=True)
    def test_is_valid_multipath_device(self, context, mock_os, util_mod):
        """
        Tests for checking validity of multipath device
        """

        # Setup errors codes
        context.setup_error_codes()

        # Test 'multipath -ll' success
        util_mod.doexec.side_effect = [(0, "out", "err")]
        self.assertTrue(mpath_dmp._is_valid_multipath_device("fake_dev"))

        # Test 'multipath -a' failure
        util_mod.doexec.side_effect = [(0, "", ""), (0, "", ""), (1, "out", "err")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        # Test failure when device is not available
        mock_os.path.exists.return_value = False
        util_mod.doexec.side_effect = [(0, "", ""), (0, "", ""), (0, "out", "err")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        # Test 'multipath -c' with error and empty output
        mock_os.path.exists.return_value = True
        mock_os.listdir.side_effect = [['sdc']]
        util_mod.doexec.side_effect = [(0, "", ""), (0, "", ""), (0, "out", "err"),
                                       (1, "", ""), OSError()]
        with self.assertRaises(SR.SROSError) as exc:
            mpath_dmp._is_valid_multipath_device("xx")
        self.assertEqual(exc.exception.errno, 431)

        # Test 'multipath -c' with error but some output
        mock_os.listdir.side_effect = [['sdc']]
        util_mod.doexec.side_effect = [(0, "", ""), (0, "", ""), (0, "out", "err"),
                                       (1, "xx", "")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        mock_os.listdir.side_effect = [['sdc']]
        util_mod.doexec.side_effect = [(0, "", ""), (0, "", ""), (0, "out", "err"),
                                       (1, "xx", "yy")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        mock_os.listdir.side_effect = [['sdc']]
        util_mod.doexec.side_effect = [(0, "", ""), (0, "", ""), (0, "out", "err"),
                                       (1, "", "yy")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        # Test when everything is fine
        mock_os.listdir.side_effect = [['sdc']]
        util_mod.doexec.side_effect = [(0, "", ""), (0, "", ""), (0, "out", "err"),
                                       (0, "out", "err")]
        self.assertTrue(mpath_dmp._is_valid_multipath_device("fake_dev"))

    @mock.patch('util.pread2', autospec=True)
    @mock.patch('mpath_dmp.os.mkdir', autospec=True)
    def test_activate_no_exception(self, mock_mkdir, pread2):
        """
        Test that activate MPDev works if directory does not exist
        """
        mpath_dmp.activate_MPdev("sid", "dst")
        pread2.assert_called_with(['ln', '-sf', "dst", os.path.join(mpath_dmp.MP_INUSEDIR, "sid")])

    @mock.patch('util.pread2', autospec=True)
    @mock.patch('mpath_dmp.os.mkdir', autospec=True)
    def test_activate_exists_success(self, mock_mkdir, pread2):
        """
        Test that activate MPDev works if directory exists
        """
        mock_mkdir.side_effect = [OSError(errno.EEXIST, "Directory exists")]
        mpath_dmp.activate_MPdev("sid", "dst")
        pread2.assert_called_with(['ln', '-sf', "dst", os.path.join(mpath_dmp.MP_INUSEDIR, "sid")])

    @mock.patch('mpath_dmp.os.mkdir', autospec=True)
    def test_activate_permission_denied(self, mock_mkdir):
        """
        Test that activate MPDev works if mkdir returns permission denied
        """
        mock_mkdir.side_effect = [OSError(errno.EPERM, "Permission denied")]
        with self.assertRaises(OSError) as context:
            mpath_dmp.activate_MPdev("sid", "dst")

        self.assertEqual(errno.EPERM, context.exception.errno)

    @testlib.with_context
    @mock.patch('mpath_dmp._is_valid_multipath_device', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    @mock.patch('mpath_dmp.os.path.exists', autospec=True)
    def test_refresh_dmp_success(self, context, mock_exists, mock_util, mock_valid):
        """
        Test refresh DMP in success case
        """
        mock_valid.return_value = True

        test_id = '360871234'

        mock_exists.return_value = True

        with mock.patch('mpath_dmp.activate_MPdev') as mock_activate:
            mpath_dmp._refresh_DMP(test_id, 4)

        # util retry around multipath should not be called
        self.assertEqual(0, mock_util.retry.call_count)

        self.assertTrue(
            mock_util.wait_for_path.call_args_list[0][0][0].endswith(
                '%s/mapper' % test_id),
            msg='wait_for_path not called with expected mapper path')
        mock_activate.assert_called_with(test_id, mock.ANY)

    @testlib.with_context
    @mock.patch('mpath_dmp._is_valid_multipath_device', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_refresh_dmp_device_not_found(self, context, mock_util, mock_valid):
        """
        Test refresh DMP device not found
        """
        # Setup error codes
        context.setup_error_codes()

        mock_valid.return_value = True

        test_id = '360871234'

        with self.assertRaises(SROSError):
            mpath_dmp._refresh_DMP(test_id, 4)

        self.assertEqual(1, mock_util.wait_for_path.call_count)
        self.assertTrue(
            mock_util.wait_for_path.call_args_list[0][0][0].endswith(test_id))

    @mock.patch('mpath_dmp._is_valid_multipath_device', autospec=True)
    @mock.patch('mpath_dmp.util.pread2', autospec=True)
    @mock.patch('mpath_dmp.util.wait_for_path', autospec=True)
    @mock.patch('mpath_dmp.os.path.exists', autospec=True)
    def test_refresh_dmp_reload_required(
            self, mock_exists, mock_wait_for_path, mock_pread, mock_valid):
        """
        Test refresh DMP device reload
        """
        mock_valid.return_value = True

        test_id = '360871234'

        mapper_exists = Queue()
        mapper_exists.put(False)
        mapper_exists.put(True)

        exists_data = {'/dev/mapper/%s' % test_id: mapper_exists}

        def exists(path):
            assert(path in exists_data)
            return exists_data[path].get()

        mock_exists.side_effect = exists

        mock_pread.return_value = 0

        with mock.patch('mpath_dmp.activate_MPdev') as mock_activate:
            mpath_dmp._refresh_DMP(test_id, 4)

        self.assertTrue(
            mock_wait_for_path.call_args_list[0][0][0].endswith(test_id))

    @mock.patch('mpath_dmp.iscsilib', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_activate_noiscsi_success(
            self, mock_util, mock_iscsilib):
        """
        MPATH activate, no iscsi, success
        """
        mock_iscsilib.is_iscsi_daemon_running.return_value = False
        mock_util.doexec.return_value = (0, "", "")
        self.mock_mpath_cli.is_working.side_effect = [False, False, True]

        mpath_dmp.activate()

        self.assertEqual(0, mock_util.pread2.call_count)

    @mock.patch('mpath_dmp.iscsilib', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_activate_noiscsi_start_mpath(
            self, mock_util, mock_iscsilib):
        """
        MPATH activate, no iscsi, start mpath
        """
        mock_iscsilib.is_iscsi_daemon_running.return_value = False
        mock_util.doexec.return_value = (1, "", "")
        self.mock_mpath_cli.is_working.side_effect = [False, False, True]

        mpath_dmp.activate()

        self.assertEqual(1, mock_util.pread2.call_count)
        mock_util.pread2.assert_called_once_with(
            ['service', 'multipathd', 'start'])

    @testlib.with_context
    @mock.patch('mpath_dmp.iscsilib', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_activate_noiscsi_mpath_not_working(
            self, context, mock_util, mock_iscsilib):
        """
        MPATH activate, mpath not running
        """
        # Setup error codes
        context.setup_error_codes()

        mock_iscsilib.is_iscsi_daemon_running.return_value = False
        mock_util.doexec.return_value = (0, "", "")
        self.mock_mpath_cli.is_working.side_effect = [False] * 120

        with self.assertRaises(SROSError) as soe:
            mpath_dmp.activate()

        self.assertEqual(430, soe.exception.errno)

    @mock.patch('mpath_dmp.iscsilib', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_activate_active_iscsi_success(
            self, mock_util, mock_iscsilib):
        """
        MPATH activate, active iscsi, success
        """
        mock_iscsilib.is_iscsi_daemon_running.return_value = True
        mock_iscsilib._checkAnyTGT.return_value = True
        mock_util.doexec.return_value = (0, "", "")
        self.mock_mpath_cli.is_working.side_effect = [False, False, True]

        mpath_dmp.activate()

        self.assertEqual(0, mock_iscsilib.restart_daemon.call_count)

    @mock.patch('mpath_dmp.iscsilib', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_activate_iscsi_no_targets_success(
            self, mock_util, mock_iscsilib):
        """
        MPATH activate, iscsi, no_targets, success
        """
        mock_iscsilib.is_iscsi_daemon_running.return_value = True
        mock_iscsilib._checkAnyTGT.return_value = False
        mock_util.doexec.return_value = (0, "", "")
        self.mock_mpath_cli.is_working.side_effect = [False, False, True]

        mpath_dmp.activate()

        self.assertEqual(0, mock_util.pread2.call_count)
        self.assertEqual(1, mock_iscsilib.restart_daemon.call_count)

    @mock.patch('mpath_dmp.glob.glob', autospec=True)
    @mock.patch('mpath_dmp.os.path.realpath', autospec=True)
    @mock.patch('mpath_dmp.iscsilib', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_deactivate_mpath_running(
            self, mock_util, mock_iscsilib, mock_realpath, mock_glob):
        """
        MPATH deactivate, running, success
        """
        mock_util.doexec.return_value = (0, "", "")
        self.mock_mpath_cli.list_maps.return_value = [
            '360a98000534b4f4e46704f5270674d70',
            '3600140582622313e8dc4270a4a897b4e']
        mock_realpath.return_value = '/dev/disk/by-id/scsi-34564'
        mock_util.retry.side_effect = util.retry

        mpath_dmp.deactivate()

        # Check that the mpath maps were removed
        mock_util.pread2.assert_has_calls([
            mock.call(['/usr/sbin/multipath', '-f', '360a98000534b4f4e46704f5270674d70']),
            mock.call(['/usr/sbin/multipath', '-W']),
            mock.call(['/usr/sbin/multipath', '-f', '3600140582622313e8dc4270a4a897b4e']),
            mock.call(['/usr/sbin/multipath', '-W'])])

    @mock.patch('mpath_dmp.glob.glob', autospec=True)
    @mock.patch('mpath_dmp.os.path.realpath', autospec=True)
    @mock.patch('mpath_dmp.iscsilib', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_deactivate_mpath_root(
            self, mock_util, mock_iscsilib, mock_realpath, mock_glob):
        """
        MPATH deactivate, mpathed root
        """
        mock_util.doexec.return_value = (0, "", "")
        self.mock_mpath_cli.list_maps.return_value = [
            '360a98000534b4f4e46704f5270674d70',
            '3600140582622313e8dc4270a4a897b4e']
        mock_realpath.side_effect = iter(
            ['/dev/mapper/34564', '/dev/mapper/34564',
             '/dev/mapper/360a98000534b4f4e46704f5270674d70'])
        mock_glob.return_value = [
            '/dev/mapper/34564',
            '/dev/mapper/360a98000534b4f4e46704f5270674d70']
        mock_util.retry.side_effect = util.retry

        mpath_dmp.deactivate()

        # Check that the mpath maps were removed
        mock_util.pread2.assert_has_calls([
            mock.call(['/usr/sbin/multipath', '-f', '360a98000534b4f4e46704f5270674d70']),
            mock.call(['/usr/sbin/multipath', '-W']),
            mock.call(['/usr/sbin/multipath', '-f', '3600140582622313e8dc4270a4a897b4e']),
            mock.call(['/usr/sbin/multipath', '-W'])])

    @mock.patch('mpath_dmp.glob.glob', autospec=True)
    @mock.patch('mpath_dmp.os.path.realpath', autospec=True)
    @mock.patch('mpath_dmp.iscsilib', autospec=True)
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_deactivate_mpath_no_iscsi_targets(
            self, mock_util, mock_iscsilib, mock_realpath, mock_glob):
        """
        MPATH deactivate, running, success
        """
        mock_util.doexec.return_value = (0, "", "")
        self.mock_mpath_cli.list_maps.return_value = [
            '360a98000534b4f4e46704f5270674d70',
            '3600140582622313e8dc4270a4a897b4e']
        mock_realpath.return_value = '/dev/disk/by-id/scsi-34564'
        mock_util.retry.side_effect = util.retry
        mock_iscsilib.is_iscsi_daemon_running.return_value = True
        mock_iscsilib._checkAnyTGT.return_value = False

        mpath_dmp.deactivate()

        # Check that the mpath maps were removed
        mock_util.pread2.assert_has_calls([
            mock.call(['/usr/sbin/multipath', '-f', '360a98000534b4f4e46704f5270674d70']),
            mock.call(['/usr/sbin/multipath', '-W']),
            mock.call(['/usr/sbin/multipath', '-f', '3600140582622313e8dc4270a4a897b4e']),
            mock.call(['/usr/sbin/multipath', '-W'])])

        self.assertEqual(1, mock_iscsilib.restart_daemon.call_count)

    @testlib.with_context
    def test_refresh_no_sid(self, context):
        # Setup error codes
        context.setup_error_codes()

        with self.assertRaises(SR.SROSError):
            mpath_dmp.refresh("", 0)

    @mock.patch('mpath_dmp._refresh_DMP', autospec=True)
    @mock.patch('mpath_dmp.os.path.exists', autospec=True)
    def test_refresh_path_exists(self, mock_exists, mock_refresh):

        mock_exists.return_value = True

        mpath_dmp.refresh('360a98000534b4f4e46704f5270674d70', 0)

        mock_refresh.assert_called_once_with(
            '360a98000534b4f4e46704f5270674d70', 0)

        mock_exists.assert_called_once_with(
            '/dev/disk/by-id/scsi-360a98000534b4f4e46704f5270674d70')

    @mock.patch('mpath_dmp.util.wait_for_path', autospec=True)
    @mock.patch('mpath_dmp.scsiutil', autospec=True)
    @mock.patch('mpath_dmp._refresh_DMP', autospec=True)
    @mock.patch('mpath_dmp.os.path.exists', autospec=True)
    def test_refresh_refresh_scsi(
            self, mock_exists, mock_refresh, mock_scsiutil, mock_wait):

        mock_exists.return_value = False
        mock_wait.return_value = True

        mpath_dmp.refresh('360a98000534b4f4e46704f5270674d70', 0)

        mock_refresh.assert_called_once_with(
            '360a98000534b4f4e46704f5270674d70', 0)

        mock_exists.assert_called_once_with(
            '/dev/disk/by-id/scsi-360a98000534b4f4e46704f5270674d70')

    @testlib.with_context
    @mock.patch('mpath_dmp.util.wait_for_path', autospec=True)
    @mock.patch('mpath_dmp.scsiutil', autospec=True)
    @mock.patch('mpath_dmp.os.path.exists', autospec=True)
    def test_refresh_refresh_error(
            self, context, mock_exists, mock_scsiutil, mock_wait):

        # Setup error codes
        context.setup_error_codes()

        def exists(path):
            print('Exists %s' % path)
            if path.startswith('/dev/'):
                return False

            return True

        mock_exists.side_effect = exists
        mock_wait.return_value = False

        with self.assertRaises(SR.SROSError):
            mpath_dmp.refresh('360a98000534b4f4e46704f5270674d70', 0)
