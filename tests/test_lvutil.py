import unittest.mock as mock
import os
import syslog
import unittest

import testlib
import lvmlib
import util

import lvutil

ONE_MEGABYTE = 1 * 1024 * 1024

TEST_VG = "VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7"
TEST_VOL = "%s/volume" % TEST_VG

def with_lvm_subsystem(func):
    @testlib.with_context
    def decorated(self, context, *args, **kwargs):
        lvsystem = lvmlib.LVSubsystem(context.log, context.add_executable)
        return func(self, lvsystem, * args, ** kwargs)

    decorated.__name__ = func.__name__
    return decorated


class TestCreate(unittest.TestCase):
    def setUp(self):
        lock_patcher = mock.patch('lvutil.Fairlock', autospec=True)
        self.addCleanup(lock_patcher.stop)
        self.mock_lock = lock_patcher.start()

    @with_lvm_subsystem
    def test_create_volume_size(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', 100 * ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')

        self.assertEqual(100, created_lv.size_mb)

    @with_lvm_subsystem
    def test_create_volume_is_in_the_right_volume_group(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', 100 * ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')

        self.assertEqual(100, created_lv.size_mb)

        self.assertEqual('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7', created_lv.volume_group.name)
        self.assertTrue(created_lv.active)
        self.assertTrue(created_lv.zeroed)

    @with_lvm_subsystem
    def test_create_volume_is_active(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', 100 * ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')

        self.assertEqual(100, created_lv.size_mb)

        self.assertTrue(created_lv.active)
        self.assertTrue(created_lv.zeroed)

    @with_lvm_subsystem
    def test_create_volume_is_zeroed(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', 100 * ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')

        self.assertEqual(100, created_lv.size_mb)

        self.assertTrue(created_lv.zeroed)

    @with_lvm_subsystem
    def test_create_creates_logical_volume_with_tags(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')

        lvutil.create('volume', ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7', tag='hello')

        created_lv, = lvsystem.get_logical_volumes_with_name('volume')
        self.assertEqual('hello', created_lv.tag)

    @mock.patch('util.pread', autospec=True)
    def test_create_percentage_has_precedence_over_size(self, mock_pread):
        lvutil.create('volume', ONE_MEGABYTE, 'VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7',
                      size_in_percentage="10%F")

        self.assertEqual(1, mock_pread.call_count)
        self.assertIn("10%F", mock_pread.call_args[0][0])


class TestRemove(unittest.TestCase):
    def setUp(self):
        lock_patcher = mock.patch('lvutil.Fairlock', autospec=True)
        self.addCleanup(lock_patcher.stop)
        self.mock_lock = lock_patcher.start()

    @with_lvm_subsystem
    def test_remove_removes_volume(self, lvsystem):
        lvsystem.add_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7')
        lvsystem.get_volume_group('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7').add_volume('volume', 100)

        lvutil.remove('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume')

        self.assertEqual([], lvsystem.get_logical_volumes_with_name('volume'))

    @mock.patch('lvutil._lvmBugCleanup', autospec=True)
    @mock.patch('util.pread', autospec=True)
    def test_remove_additional_config_param(self, mock_pread, _bugCleanup):
        lvutil.remove('VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume', config_param="blah")
        mock_pread.assert_called_once_with(
            [os.path.join(lvutil.LVM_BIN, lvutil.CMD_LVREMOVE)]
            + "-f VG_XenStorage-b3b18d06-b2ba-5b67-f098-3cdd5087a2a7/volume --config devices{blah}".split(),
           quiet=False, text=True)


class TestDeactivate(unittest.TestCase):

    def setUp(self):
        lock_patcher = mock.patch('lvutil.Fairlock', autospec=True)
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


class TestActivate(unittest.TestCase):
    def setUp(self):
        self.addCleanup(mock.patch.stopall)

        lock_patcher = mock.patch('lvutil.Fairlock', autospec=True)
        self.mock_lock = lock_patcher.start()
        pathexists_patcher = mock.patch('lvutil.util.pathexists', autospec=True)
        self.mock_exists = pathexists_patcher.start()

        log_patcher = mock.patch('lvutil.util.SMlog', autospec=True)
        mock_log = log_patcher.start()
        mock_log.side_effect = self.__log

    def __log(self, message, ident="SM", priority=syslog.LOG_INFO):
        print("%s: %s: %s" %(ident, priority, message))

    def __create_test_volume(self, lvsystem):
        lvsystem.add_volume_group(TEST_VG)
        lvsystem.get_volume_group(TEST_VG).add_volume('volume', 100)

    @with_lvm_subsystem
    def test_activate_noref_norefresh(self, lvsystem):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = True

        # Act
        lvutil.activateNoRefcount(TEST_VOL, False)

    @mock.patch('lvutil.time.sleep', autospec=True)
    @mock.patch('lvutil.cmd_lvm')
    @with_lvm_subsystem
    def test_activate_noref_metadata_error_retry(self, lvsystem, mock_cmd_lvm, mock_sleep):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = True

        metadata_error = """  Incorrect checksum in metadata area header on /dev/sdb at 4096
   Failed to read mda header from /dev/sdb
   Failed to scan VG from /dev/sdb
   Volume group "VG_XenStorage-94d5c7de-3bee-e7c2-8aeb-d609e7dcd358" not found
   Cannot process volume group VG_XenStorage-94d5c7de-3bee-e7c2-8aeb-d609e7dcd358"""

        mock_cmd_lvm.side_effect = [
            util.CommandException(5, 'lvchange', metadata_error),
            ''
        ]

        # Act
        lvutil.activateNoRefcount(TEST_VOL, False)

    @mock.patch('lvutil.time.sleep', autospec=True)
    @mock.patch('lvutil.cmd_lvm')
    @with_lvm_subsystem
    def test_activate_noref_metadata_max_retries(self, lvsystem, mock_cmd_lvm, mock_sleep):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = True

        metadata_error = """  Incorrect checksum in metadata area header on /dev/sdb at 4096
   Failed to read mda header from /dev/sdb
   Failed to scan VG from /dev/sdb
   Volume group "VG_XenStorage-94d5c7de-3bee-e7c2-8aeb-d609e7dcd358" not found
   Cannot process volume group VG_XenStorage-94d5c7de-3bee-e7c2-8aeb-d609e7dcd358"""

        mock_cmd_lvm.side_effect = util.CommandException(5, 'lvchange', metadata_error)

        # Act
        with self.assertRaises(util.CommandException):
            lvutil.activateNoRefcount(TEST_VOL, False)

        self.assertEqual(9, mock_sleep.call_count)

    @mock.patch('lvutil.cmd_lvm')
    @with_lvm_subsystem
    def test_activate_noref_IO_error_reported(self, lvsystem, mock_cmd_lvm):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = True


        mock_cmd_lvm.side_effect = [
            util.CommandException(5, 'lvchange', "Device not found")
        ]

        # Act
        with self.assertRaises(util.CommandException) as ce:
            lvutil.activateNoRefcount(TEST_VOL, False)

        self.assertEqual(5, ce.exception.code)

    @with_lvm_subsystem
    def test_activate_noref_not_activated(self, lvsystem):
        # Arrange
        self.__create_test_volume(lvsystem)
        self.mock_exists.return_value = False

        # Act
        with self.assertRaises(util.CommandException) as ce:
            lvutil.activateNoRefcount(TEST_VOL, False)

        self.assertIn('LV not activated', ce.exception.reason)


@mock.patch('util.pread', autospec=True) # m_pread
@mock.patch('lvutil.Fairlock', autospec=True) # _1
class Test_cmd_lvm(unittest.TestCase):

    def test_refuse_to_run_empty_list(self, _1, m_pread):
        r = lvutil.cmd_lvm([])
        self.assertIsNone(r)
        self.assertEqual(m_pread.call_count, 0)

    def test_refuse_to_run_none_list(self, _1, m_pread):
        r = lvutil.cmd_lvm("i am not a list")
        self.assertIsNone(r)
        self.assertEqual(m_pread.call_count, 0)

    def test_refuse_to_run_not_whitelisted_command(self, _1, m_pread):
        r = lvutil.cmd_lvm(["/usr/bin/i_am_not_an_approved_command"])
        self.assertIsNone(r)
        self.assertEqual(m_pread.call_count, 0)

    def test_refuse_to_run_with_non_string_args(self, _1, m_pread):
        r = lvutil.cmd_lvm([lvutil.CMD_LVDISPLAY, 458])
        self.assertIsNone(r)
        self.assertEqual(m_pread.call_count, 0)

    def test_args_are_passed_to_pread(self, _1, m_pread):
        r = lvutil.cmd_lvm([lvutil.CMD_LVDISPLAY, "pancakes"])
        self.assertEqual(m_pread.call_count, 1)
        self.assertIn(lvutil.CMD_LVDISPLAY, m_pread.call_args[0][0][0])
        self.assertIn("pancakes", m_pread.call_args[0][0][1])

    def test_output_is_returned(self, _1, m_pread):
        m_pread.return_value = "muffins"
        r = lvutil.cmd_lvm([lvutil.CMD_LVDISPLAY])
        self.assertEqual("muffins", r)

    @mock.patch('time.time', autospec=True) # m_time
    @mock.patch('util.SMlog', autospec=True) # m_smlog
    def test_warning_if_cmd_takes_too_long(self, m_smlog, m_time, _1, m_pread):
        m_time.side_effect = [0, lvutil.MAX_OPERATION_DURATION*2]
        lvutil.cmd_lvm([lvutil.CMD_LVDISPLAY])
        self.assertEqual(m_pread.call_count, 1)
        self.assertIn("Long LVM call", m_smlog.call_args[0][0])
        self.assertIn(f"took {lvutil.MAX_OPERATION_DURATION*2}", m_smlog.call_args[0][0])

@mock.patch('lvutil.cmd_lvm')
@mock.patch('util.SMlog', autospec=True)
class TestGetPVsInVG(unittest.TestCase):

    def test_pvs_in_vg(self, mock_smlog, mock_cmd_lvm):
        # Normal case
        mock_cmd_lvm.return_value = "uuid1 vg1\nuuid2 vg1\nuuid3 vg2"
        result = lvutil.getPVsInVG("vg1")
        self.assertEqual(result, ["uuid1", "uuid2"])
        mock_smlog.assert_called_once_with("PVs in VG vg1: ['uuid1', 'uuid2']")
    
    def test_no_pvs(self, mock_smlog, mock_cmd_lvm):
        # Test when no PVs are returned
        mock_cmd_lvm.return_value = ""
        result = lvutil.getPVsInVG("vg1")
        self.assertEqual(result, [])
        mock_smlog.assert_has_calls([
            mock.call("Warning: Invalid or empty line in pvs output: "),
            mock.call("PVs in VG vg1: []")
        ])

    def test_no_pvs_in_vg(self, mock_smlog, mock_cmd_lvm):
        # Test when no PVs belong to the specified VG
        mock_cmd_lvm.return_value = "uuid1 vg2\nuuid2 vg2"
        result = lvutil.getPVsInVG("vg1")
        self.assertEqual(result, [])
        mock_smlog.assert_called_once_with("PVs in VG vg1: []")

    def test_command_error(self, mock_smlog, mock_cmd_lvm):
        # Test invalid return value from cmd_lvm
        mock_cmd_lvm.return_value = "Invalid retrun value."
        result = lvutil.getPVsInVG("vg1")
        self.assertEqual(result, [])
        mock_smlog.assert_has_calls([
            mock.call("Warning: Invalid or empty line in pvs output: Invalid retrun value."),
            mock.call("PVs in VG vg1: []")
        ])
        mock_smlog.assert_called_with("PVs in VG vg1: []")

@mock.patch('lvutil.cmd_lvm')
@mock.patch('util.SMlog', autospec=True)
class TestGetPVsWithUUID(unittest.TestCase):

    def test_pv_match_uuid(self, mock_smlog, mock_cmd_lvm):
        # Normal case
        mock_cmd_lvm.return_value = "pv1 uuid1\npv2 uuid2"
        result = lvutil.getPVsWithUUID("uuid1")
        self.assertEqual(result, ["pv1"])
        mock_smlog.assert_called_once_with("PVs with uuid uuid1: ['pv1']")
    
    def test_no_pvs(self, mock_smlog, mock_cmd_lvm):
        # Test when no PVs are returned
        mock_cmd_lvm.return_value = ""
        result = lvutil.getPVsWithUUID("uuid1")
        self.assertEqual(result, [])
        mock_smlog.assert_has_calls([
            mock.call("Warning: Invalid or empty line in pvs output: "),
            mock.call("PVs with uuid uuid1: []")
        ])

    def test_no_pvs_match_uuid(self, mock_smlog, mock_cmd_lvm):
        # Test when no PVs is with the specified uuid
        mock_cmd_lvm.return_value = "pv1 uuid1\npv2 uuid2"
        result = lvutil.getPVsWithUUID("uuid3")
        self.assertEqual(result, [])
        mock_smlog.assert_called_once_with("PVs with uuid uuid3: []")

    def test_command_error(self, mock_smlog, mock_cmd_lvm):
        # Test invalid return value from cmd_lvm
        mock_cmd_lvm.return_value = "Invalid retrun value."
        result = lvutil.getPVsWithUUID("uuid1")
        self.assertEqual(result, [])
        mock_smlog.assert_has_calls([
            mock.call("Warning: Invalid or empty line in pvs output: Invalid retrun value."),
            mock.call("PVs with uuid uuid1: []")
        ])