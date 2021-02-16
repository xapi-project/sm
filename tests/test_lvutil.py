import unittest
import testlib
import lvmlib
import mock

import os
import lvutil
import util

ONE_MEGABYTE = 1 * 1024 * 1024


def with_lvm_subsystem(func):
    @testlib.with_context
    def decorated(self, context, *args, **kwargs):
        lvsystem = lvmlib.LVSubsystem(context.log, context.add_executable)
        return func(self, lvsystem, * args, ** kwargs)

    decorated.__name__ = func.__name__
    return decorated


class TestCreate(unittest.TestCase):
    def setUp(self):
        lock_patcher = mock.patch('lvutil.lock', autospec=True)
        self.addCleanup(lock_patcher.stop)
        self.mock_lock = lock_patcher.start()

    @with_lvm_subsystem
    def test_create_volume_size(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', 100 * ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')

        self.assertEquals(100, created_lv.size_mb)

    @with_lvm_subsystem
    def test_create_volume_is_in_the_right_volume_group(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', 100 * ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')

        self.assertEquals(100, created_lv.size_mb)

        self.assertEquals('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7', created_lv.volume_group.name)
        self.assertTrue(created_lv.active)
        self.assertTrue(created_lv.zeroed)

    @with_lvm_subsystem
    def test_create_volume_is_active(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', 100 * ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')

        self.assertEquals(100, created_lv.size_mb)

        self.assertTrue(created_lv.active)
        self.assertTrue(created_lv.zeroed)

    @with_lvm_subsystem
    def test_create_volume_is_zeroed(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', 100 * ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')

        self.assertEquals(100, created_lv.size_mb)

        self.assertTrue(created_lv.zeroed)

    @with_lvm_subsystem
    def test_create_creates_logical_volume_with_tags(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7', tag='hello')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')
        self.assertEquals('hello', created_lv.tag)

    @mock.patch('util.pread', autospec=True)
    def test_create_percentage_has_precedence_over_size(self, mock_pread):
        lvutil.create('volume', ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7',
                      size_in_percentage="10%F")

        self.assertEqual(1, mock_pread.call_count)
        self.assertIn("10%F", mock_pread.call_args[0][0])


class TestRemove(unittest.TestCase):
    def setUp(self):
        lock_patcher = mock.patch('lvutil.lock', autospec=True)
        self.addCleanup(lock_patcher.stop)
        self.mock_lock = lock_patcher.start()

    @with_lvm_subsystem
    def test_remove_removes_volume(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')
        lvsystem.get_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7').add_volume('volume', 100)

        lvutil.remove('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume')

        self.assertEquals([], lvsystem.get_logical_volumes_with_name('volume'))

    @mock.patch('lvutil._lvmBugCleanup', autospec=True)
    @mock.patch('util.pread', autospec=True)
    def test_remove_additional_config_param(self, mock_pread, _bugCleanup):
        lvutil.remove('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume', config_param="blah")
        mock_pread.assert_called_once_with(
            [os.path.join(lvutil.LVM_BIN, lvutil.CMD_LVREMOVE)]
            + "-f VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume --config devices{blah}".split(),
           quiet=False)


class TestDeactivate(unittest.TestCase):

    def setUp(self):
        lock_patcher = mock.patch('lvutil.lock', autospec=True)
        pathexists_patcher = mock.patch('lvutil.util.pathexists', autospec=True)
        lexists_patcher = mock.patch('lvutil.os.path.lexists', autospec=True)
        unlink_patcher = mock.patch('lvutil.os.unlink', autospec=True)
        self.addCleanup(mock.patch.stopall)
        self.mock_lock = lock_patcher.start()
        self.mock_exists = pathexists_patcher.start()
        self.mock_lexists = lexists_patcher.start()
        self.mock_unlink = unlink_patcher.start()


    def __create_test_volume(self, lvsystem):
        lvsystem.add_volume_group(
            'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')
        lvsystem.get_volume_group(
            'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7'
        ).add_volume('volume', 100)

    @with_lvm_subsystem
    def test_deactivate_noref_withbugcleanup(self, lvsystem):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = True
        self.mock_lexists.return_value = True

        # Act
        lvutil.deactivateNoRefcount(
            'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume')

    @mock.patch('lvutil.util.pread')
    @with_lvm_subsystem
    def test_deactivate_noref_withnobugcleanup(
            self, lvsystem, mock_pread):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = False
        mock_pread.side_effect = [0, 0]

        # Act
        lvutil.deactivateNoRefcount(
            'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume')

    @mock.patch('lvutil.util.pread')
    @with_lvm_subsystem
    def test_deactivate_noref_withbugcleanup_retry(
            self, lvsystem, mock_pread):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = True
        self.mock_lexists.return_value = True
        mock_pread.side_effect = [0, util.CommandException(0),
                                  util.CommandException(1), 0]

        # Act
        lvutil.deactivateNoRefcount(
            'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume')

    @mock.patch('lvutil.os.symlink', autotspec=True)
    @mock.patch('lvutil.time.sleep', autospec=True)
    @mock.patch('lvutil.util.pread')
    @with_lvm_subsystem
    def test_deactivate_noref_withbugcleanup_retry_fail(
            self, lvsystem, mock_pread, mock_sleep, mock_symlink):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = True
        self.mock_lexists.return_value = False
        side_effect = [0, util.CommandException(0)]
        side_effect += 11 * [util.CommandException(1),
                             util.CommandException(0)]
        mock_pread.side_effect = side_effect

        # Act
        with self.assertRaises(util.CommandException):
            lvutil.deactivateNoRefcount(
                'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume')

        # Assert
        mock_symlink.assert_called_once_with(
            mock.ANY, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume')

