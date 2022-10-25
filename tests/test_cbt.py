import unittest.mock as mock
import SR
import testlib
import unittest
import uuid
import VDI
import vhdutil
import xs_errors
import util
import errno
import cbtutil
from bitarray import bitarray
import base64
import xmlrpc.client


class TestVDI(VDI.VDI):
    def load(self, vdi_uuid):
        self.vdi_type = vhdutil.VDI_TYPE_VHD
        self._state_mock = mock.Mock()
        self.path = "/mock/sr_path/" + str(vdi_uuid)
        self.block_tracking_state = False

    @property
    def state_mock(self):
        return self._state_mock

    def _get_blocktracking_status(self, uuid=None):
        return self.block_tracking_state

    def _ensure_cbt_space(self):
        super(TestVDI, self)._ensure_cbt_space()
        self.state_mock._ensure_cbt_space()

    def _get_cbt_logpath(self, uuid):
        super(TestVDI, self)._get_cbt_logpath(uuid)
        self.state_mock._get_cbt_logpath(uuid)
        return "/mock/sr_path/{0}.log".format(uuid)

    def _create_cbt_log(self):
        logpath = super(TestVDI, self)._create_cbt_log()
        self.state_mock._create_cbt_log()
        self.block_tracking_state = True
        return logpath

    def _delete_cbt_log(self):
        self.state_mock._delete_cbt_log()
        self.block_tracking_state = False

    def _rename(self, from_path, to_path):
        self.state_mock._rename(from_path, to_path)

    def _do_snapshot(self, sr_uuid, vdi_uuid, snapType,
                     cloneOp=False, secondary=None, cbtlog=None):
        self.state_mock._do_snapshot(sr_uuid, vdi_uuid, snapType, cloneOp,
                                     secondary, cbtlog)

    def _activate_cbt_log(self, logname):
        self.state_mock._activate_cbt_log(logname)

    def _deactivate_cbt_log(self, logname):
        self.state_mock._deactivate_cbt_log(logname)


class TestCBT(unittest.TestCase):

    def setUp(self):
        self.sr = mock.MagicMock()
        self.vdi_uuid = uuid.uuid4()
        self.sr_uuid = uuid.uuid4()
        self.xenapi = mock.MagicMock()
        self.xenapi.VDI = mock.MagicMock()

        self.sr.session = mock.MagicMock()
        self.sr.path = '/run/sr-mount/test-sr'

        self.sr.session.xenapi = self.xenapi

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.VDI._cbt_op', autospec=True)
    def test_configure_blocktracking_enable_success(self, context, mock_cbt, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

        self._check_tapdisk_paused_and_resumed(mock_bt_vdi, self.vdi_uuid)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_configure_blocktracking_enable_already_enabled(self, context, mock_cbt, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

        self._check_setting_not_changed()
        self._check_tapdisk_not_modified(mock_bt_vdi)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('VDI.VDI._cbt_op', autospec=True)
    @mock.patch('lock.LockImplementation')
    def test_configure_blocktracking_disable_when_enabled_without_parent(
            self, context, mock_lock, mock_cbt, mock_logchecker, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, True)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

        self._check_tapdisk_paused_and_resumed(mock_bt_vdi, self.vdi_uuid)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation')
    def test_configure_blocktracking_disable_when_enabled_with_parent(
            self, context, mock_lock, mock_logcheck, mock_cbt, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, True)
        (parent_uuid, child_uuid) = self._set_CBT_chain_state(mock_logcheck,
                                                              mock_cbt, True)
        logpath = self.vdi._get_cbt_logpath(self.vdi_uuid)
        expected_parent_path = '/mock/sr_path/%s.log' % parent_uuid

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

        logfile = self._check_setting_state(self.vdi, False)
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi, self.vdi_uuid)
        parent_call = [mock.call(logpath)]
        child_call = [mock.call(expected_parent_path, uuid.UUID(int=0))]
        mock_cbt.get_cbt_parent.assert_has_calls(parent_call)
        mock_cbt.set_cbt_child.assert_has_calls(child_call)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    def test_configure_blocktracking_disable_already_disabled(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, False)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

        self._check_setting_not_changed()
        self._check_tapdisk_not_modified(mock_bt_vdi)

    @testlib.with_context
    def test_configure_blocktracking_enable_raw_vdi(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = VDI.VDI(self.sr, self.vdi_uuid)
        self.vdi.path = "/mock/sr_path/" + str(self.vdi_uuid)

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

    @testlib.with_context
    def test_configure_blocktracking_enable_snapshot(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self.xenapi.VDI.get_is_a_snapshot.return_value = True

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.util', autospec=True)
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_configure_blocktracking_enable_pause_fail(self, context, mock_cbt, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, False)
        mock_bt_vdi.tap_pause.return_value = False

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.util', autospec=True)
    def test_configure_blocktracking_disable_pause_fail(self, context, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, True)
        mock_bt_vdi.tap_pause.return_value = False

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

    @testlib.with_context
    @mock.patch('VDI.util', autospec=True)
    def test_configure_blocktracking_enable_metadata_no_space(self, context, mock_util):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self.vdi.state_mock._ensure_cbt_space.side_effect = [xs_errors.XenError('SRNoSpace')]

        self._set_initial_state(self.vdi, False)

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_configure_blocktracking_enable_metadata_creation_fail(self, context, mock_cbt, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)
        self.vdi.state_mock._get_cbt_logpath.side_effect = [xs_errors.XenError('CBTActivateFailed')]

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

        self._check_tapdisk_not_modified(mock_bt_vdi)
        self._check_setting_state(self.vdi, False)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_configure_blocktracking_enable_metadata_initialisation_fail(self, context, mock_cbt, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)
        mock_cbt.create_cbt_log.side_effect = Exception(errno.EIO)

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

        self._check_tapdisk_not_modified(mock_bt_vdi)
        self._check_setting_state(self.vdi, False)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    @mock.patch('lock.LockImplementation')
    def test_configure_blocktracking_disable_metadata_deletion_fail(self, context, mock_lock, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        self.vdi.state_mock._delete_cbt_log.side_effect = [xs_errors.XenError('CBTDeactivateFailed')]

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

        #self._check_tapdisk_not_modified(mock_bt_vdi)
        self._check_setting_state(self.vdi, True)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_activate_no_tracking_success(self, context, mock_cbt):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)
        vdi_options = self.vdi.activate(self.sr_uuid, self.vdi_uuid)

        self.assertIsNone(vdi_options)
        # python2-mock-1.0.1-9.el doesn't support these asserts
        #mock_cbt.get_cbt_consistency.assert_not_called()
        self.assertEqual(0, mock_cbt.get_cbt_consistency.call_count)
        self.assertEqual(0, self.vdi.state_mock._activate_cbt_log.call_count)

    @testlib.with_context
    @mock.patch('VDI.VDI._cbt_op', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_activate_consistent_success(self, context, mock_lock, mock_cbt):
        context.setup_error_codes()

        expected_log_path = '/mock/sr_path/{0}.log'.format(self.vdi_uuid)
        logname = '%s.cbtlog' % self.vdi_uuid

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        mock_cbt.return_value = [True, None]

        log_path = self.vdi.activate(self.sr_uuid, self.vdi_uuid)

        args1 = (expected_log_path)
        args2 = (expected_log_path, False)

        calls = [mock.call(self.vdi, self.vdi_uuid,
                           cbtutil.get_cbt_consistency, args1),
                 mock.call(self.vdi, self.vdi_uuid,
                           cbtutil.set_cbt_consistency, * args2)]

        mock_cbt.assert_has_calls(calls)
        self.assertEqual({'cbtlog': expected_log_path}, log_path)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_activate_consistency_check_fail(self, context, mock_lock, mock_cbt):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        mock_cbt.get_cbt_consistency.side_effect = [False]

        result = self.vdi.activate(self.sr_uuid, self.vdi_uuid)

        # Check that one message is raised and return is None
        self._check_setting_state(self.vdi, False)
        self.xenapi.message.create.assert_called_once()
        self.assertEqual(result, None)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_deactivate_no_tracking_success(self, context, mock_cbt):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)

        self.vdi.deactivate(self.sr_uuid, self.vdi_uuid)

        # python2-mock-1.0.1-9.el doesn't support these asserts
        #mock_cbt.set_cbt_consistency.assert_not_called()
        self.assertEqual(0, mock_cbt.set_cbt_consistency.call_count)
        self.assertEqual(0, self.vdi.state_mock._deactivate_cbt_log.call_count)

    @testlib.with_context
    @mock.patch('VDI.VDI._cbt_op', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_deactivate_success(self, context, mock_lock, mock_cbt):
        context.setup_error_codes()

        expected_log_path = '/mock/sr_path/{0}.log'.format(self.vdi_uuid)
        logname = '%s.cbtlog' % self.vdi_uuid

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)

        self.vdi.deactivate(self.sr_uuid, self.vdi_uuid)

        args = (expected_log_path, True)
        mock_cbt.assert_called_with(self.vdi, self.vdi_uuid,
                                    cbtutil.set_cbt_consistency, * args)

    @testlib.with_context
    def test_snapshot_success_with_CBT_disable(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)

        self.vdi.snapshot(self.sr_uuid, self.vdi_uuid)

        self.vdi.state_mock._do_snapshot.assert_called_with(self.sr_uuid,
                                                            self.vdi_uuid,
                                                            mock.ANY,
                                                            mock.ANY,
                                                            mock.ANY,
                                                            None)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation')
    def test_snapshot_success_no_parent(self, context, mock_lock,
                                        mock_logchecker, mock_cbt):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        (parent_uuid, child_uuid) = self._set_CBT_chain_state(mock_logchecker,
                                                              mock_cbt, False)
        snap_uuid = uuid.uuid4()

        self.vdi._cbt_snapshot(snap_uuid, True)

        self._check_CBT_chain_created(self.vdi, mock_cbt, self.vdi_uuid,
                                      snap_uuid, parent_uuid, child_uuid)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation')
    def test_snapshot_success_with_parent(self, context, mock_lock,
                                          mock_logcheck, mock_cbt):
        #context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        # Set initial state
        self._set_initial_state(self.vdi, True)
        (parent_uuid, child_uuid) = self._set_CBT_chain_state(mock_logcheck,
                                                              mock_cbt, True)
        snap_uuid = uuid.uuid4()

        self.vdi._cbt_snapshot(snap_uuid, True)

        self._check_CBT_chain_created(self.vdi, mock_cbt, self.vdi_uuid,
                                      snap_uuid, parent_uuid, child_uuid)

    @testlib.with_context
    @mock.patch('VDI.VDI._cbt_op', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('VDI.VDI._ensure_cbt_space', autospec=True)
    @mock.patch('util.SMlog', autospec=True)
    def test_snapshot_out_of_space_failure(self, context, mock_smlog,
            mock_ensure_space, mock_logcheck, mock_cbt):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        # Set initial state
        self._set_initial_state(self.vdi, True)
        snap_uuid = uuid.uuid4()

        # Write util.SMlog calls to smlog_out
        self.fakesmlog = ""

        def fakeSMlog(inArg):
            self.fakesmlog = self.fakesmlog + inArg.strip()

        mock_smlog.side_effect = fakeSMlog
        mock_ensure_space.side_effect = xs_errors.XenError('SRNoSpace')

        self.vdi._cbt_snapshot(snap_uuid, True)

        self.assertEqual(1, self.vdi.state_mock._delete_cbt_log.call_count)
        self.assertTrue("insufficient space" in self.fakesmlog)
        self._check_setting_state(self.vdi, False)
        self.xenapi.message.create.assert_called_once()

    @testlib.with_context
    @mock.patch('VDI.VDI._cbt_op', autospec=True)
    def test_resize_cbt_enabled(self, context, mock_cbt):
        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        # Set initial state
        self._set_initial_state(self.vdi, True)
        size = 2093050
        logpath = self.vdi._get_cbt_logpath(self.vdi_uuid)

        self.vdi.resize_cbt(self.sr_uuid, self.vdi_uuid, size)
        args = (logpath, size)
        mock_cbt.assert_called_with(self.vdi, self.vdi_uuid,
                                    cbtutil.set_cbt_size, * args)
        self._check_setting_state(self.vdi, True)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_resize_cbt_disable(self, context, mock_cbt):
        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        # Set initial state
        self._set_initial_state(self.vdi, False)
        size = 2093050

        self.vdi.resize_cbt(self.sr_uuid, self.vdi_uuid, size)
        self.assertEqual(0, mock_cbt.set_cbt_size.call_count)
        self._check_setting_state(self.vdi, False)

    @testlib.with_context
    @mock.patch('VDI.VDI._cbt_op', autospec=True)
    def test_resize_exception(self, context, mock_cbt):
        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        # Set initial state
        self._set_initial_state(self.vdi, True)
        size = 2093050
        mock_cbt.side_effect = util.CommandException(errno.EINVAL)

        self.vdi.resize_cbt(self.sr_uuid, self.vdi_uuid, size)
        self._check_setting_state(self.vdi, False)
        self.xenapi.message.create.assert_called_once()

    @testlib.with_context
    @mock.patch('lock.LockImplementation')
    def test_vdi_data_destroy_cbt_enabled(self, context, mock_lock):
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)

        self.vdi.delete(self.sr_uuid, self.vdi_uuid, data_only=True)
        self.assertEqual(0, self.vdi.state_mock._delete_cbt_log.call_count)

    @testlib.with_context
    @mock.patch('lock.LockImplementation')
    def test_vdi_data_destroy_cbt_disabled(self, context, mock_lock):
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)

        self.vdi.delete(self.sr_uuid, self.vdi_uuid, data_only=True)
        self.assertEqual(0, self.vdi.state_mock._delete_cbt_log.call_count)

    @testlib.with_context
    @mock.patch('lock.LockImplementation')
    def test_vdi_delete_cbt_disabled(self, context, mock_lock):
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)

        self.vdi.delete(self.sr_uuid, self.vdi_uuid, data_only=False)
        self.assertEqual(0, self.vdi.state_mock._delete_cbt_log.call_count)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists')
    @mock.patch('lock.LockImplementation')
    def test_vdi_delete_cbt_enabled_no_child(self, context, mock_lock,
                                             mock_logcheck, mock_cbt):
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        parent_uuid = "parentUUID"
        child_uuid = uuid.UUID(int=0)
        mock_cbt.get_cbt_parent.return_value = parent_uuid
        mock_cbt.get_cbt_child.return_value = child_uuid
        parent_logpath = self.vdi._get_cbt_logpath(parent_uuid)
        mock_logcheck.side_effect = [True, False]

        self.vdi.delete(self.sr_uuid, self.vdi_uuid, data_only=False)

        mock_cbt.set_cbt_child.assert_called_with(parent_logpath, child_uuid)
        self.assertEqual(0, mock_cbt.set_cbt_parent.call_count)
        self.assertEqual(0, mock_cbt.coalesce_bitmap.call_count)
        self.vdi.state_mock._delete_cbt_log.assert_called_with()

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists')
    @mock.patch('lock.LockImplementation')
    def test_vdi_delete_cbt_enabled_with_child(self, context, mock_lock,
                                               mock_logcheck, mock_cbt,
                                               mock_bt):
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        parent_uuid = "parentUUID"
        child_uuid = "childUUID"
        mock_cbt.get_cbt_parent.return_value = parent_uuid
        mock_cbt.get_cbt_child.return_value = child_uuid
        logpath = self.vdi._get_cbt_logpath(self.vdi_uuid)
        parent_log = self.vdi._get_cbt_logpath(parent_uuid)
        child_log = self.vdi._get_cbt_logpath(child_uuid)
        mock_logcheck.side_effect = [True, True]
        #Mock child log in use
        mock_cbt.get_cbt_consistency.return_value = False

        self.vdi.delete(self.sr_uuid, self.vdi_uuid, data_only=False)

        mock_cbt.set_cbt_child.assert_called_with(parent_log, child_uuid)
        mock_cbt.set_cbt_parent.assert_called_with(child_log, parent_uuid)
        self._check_tapdisk_paused_and_resumed(mock_bt, child_uuid)
        mock_cbt.coalesce_bitmap.assert_called_with(logpath, child_log)
        self.vdi.state_mock._delete_cbt_log.assert_called_with()

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists')
    @mock.patch('lock.LockImplementation')
    def test_vdi_delete_bitmap_coalesce_exc(self, context, mock_lock,
                                            mock_logcheck, mock_cbt):
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        parent_uuid = "parentUUID"
        child_uuid = "childUUID"
        mock_cbt.get_cbt_parent.return_value = parent_uuid
        mock_cbt.get_cbt_child.return_value = child_uuid
        parent_log = self.vdi._get_cbt_logpath(parent_uuid)
        child_log = self.vdi._get_cbt_logpath(child_uuid)
        mock_logcheck.side_effect = [True, True, True, True]
        mock_cbt.coalesce_bitmap.side_effect = util.CommandException(errno.EIO)

        self.vdi.delete(self.sr_uuid, self.vdi_uuid, data_only=False)

        mock_cbt.set_cbt_child.assert_called_with(parent_log, self.vdi_uuid)
        mock_cbt.set_cbt_parent.assert_called_with(child_log, self.vdi_uuid)
        self.assertEqual(0, self.vdi.state_mock._delete_cbt_log.call_count)

    @testlib.with_context
    def test_list_changed_blocks_same_vdi(self, context):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self.xenapi.VDI.get_uuid.return_value = self.vdi_uuid

        with self.assertRaises(SR.SROSError) as exc:
            self.vdi.list_changed_blocks()
        # Test CBTChangedBlocksError is raised
        self.assertEqual(exc.exception.errno, 460)

    @testlib.with_context
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    def test_list_changed_blocks_not_related(self, context, mock_log):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self.xenapi.VDI.get_uuid.return_value = "target_uuid"
        # Terminate the chain before target_uuid is reached
        mock_log.side_effect = [True, False]

        with self.assertRaises(SR.SROSError) as exc:
            self.vdi.list_changed_blocks()
        # Test CBTChangedBlocksError is raised
        self.assertEqual(exc.exception.errno, 460)

    @testlib.with_context
    def test_list_changed_blocks_cbt_disabled(self, context):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)
        self.xenapi.VDI.get_uuid.return_value = "target_uuid"

        with self.assertRaises(SR.SROSError) as exc:
            self.vdi.list_changed_blocks()
        # Test CBTChangedBlocksError is raised
        self.assertEqual(exc.exception.errno, 460)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_list_changed_blocks_success(self, context, mock_lock,
                                         mock_log, mock_cbt):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        # Set up scenario such that metadata chain looks like
        # source_vdi->snap_vdi->target_vdi
        snap_uuid = "snapUUID"
        target_uuid = "targetUUID"
        self.xenapi.VDI.get_uuid.return_value = target_uuid
        mock_cbt.get_cbt_child.side_effect = [snap_uuid, target_uuid]
        bitmap1 = bitarray(1024)
        bitmap2 = bitarray(1024)
        mock_cbt.get_cbt_bitmap.side_effect = [bitmap1.tobytes(),
                                               bitmap2.tobytes()]
        mock_cbt.get_cbt_size.return_value = 67108864
        bitmap1.bytereverse()
        bitmap2.bytereverse()
        expected_string = base64.b64encode((bitmap1 | bitmap2).tobytes())
        expected_result = xmlrpc.client.dumps((expected_string, ), "", True)

        result = self.vdi.list_changed_blocks()
        # Assert that bitmap is only read for VDIs from source + 1 to target
        self.assertEqual(2, mock_cbt.get_cbt_bitmap.call_count)
        self.assertEqual(result, expected_result)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_list_changed_blocks_vdi_resized_success(self, context,
                                                     mock_lock, mock_log,
                                                     mock_cbt):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        # Set up scenario such that metadata chain looks like
        # source_vdi->snap_vdi->target_vdi
        snap_uuid = "snapUUID"
        target_uuid = "targetUUID"
        self.xenapi.VDI.get_uuid.return_value = target_uuid
        mock_cbt.get_cbt_child.side_effect = [snap_uuid, target_uuid]
        vdi_size1 = 5242880  # 5MB
        vdi_size2 = 10485760  # 10MB
        bitmap1 = bitarray(80)
        bitmap2 = bitarray(160)
        mock_cbt.get_cbt_size.side_effect = [vdi_size1, vdi_size2]
        mock_cbt.get_cbt_bitmap.side_effect = [bitmap1.tobytes(),
                                               bitmap2.tobytes()]
        bitmap1.bytereverse()
        # Pad bitmap1 with extra 0s
        bitmap1 += 80 * bitarray('0')
        bitmap2.bytereverse()
        expected_string = base64.b64encode((bitmap1 | bitmap2).tobytes())
        expected_result = xmlrpc.client.dumps((expected_string, ), "", True)

        result = self.vdi.list_changed_blocks()
        self.assertEqual(result, expected_result)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_list_changed_blocks_vdi_shrunk(self, context, mock_lock,
                                            mock_log, mock_cbt):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        target_uuid = "targetUUID"
        self.xenapi.VDI.get_uuid.return_value = target_uuid
        vdi_size1 = 10485760  # 10MB
        vdi_size2 = 5242880  # 5MB
        mock_cbt.get_cbt_size.side_effect = [vdi_size1, vdi_size2]
        bitmap1 = bitarray(160)
        bitmap2 = bitarray(80)
        mock_cbt.get_cbt_bitmap.side_effect = [bitmap1.tobytes(),
                                               bitmap2.tobytes()]

        with self.assertRaises(SR.SROSError) as exc:
            self.vdi.list_changed_blocks()
        # Test CBTChangedBlocksError is raised
        self.assertEqual(exc.exception.errno, 459)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_list_changed_blocks_smaller_bitmap(self, context,
                                                mock_lock,
                                                mock_log, mock_cbt):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        #target_uuid = "targetUUID"
        #self.xenapi.VDI.get_uuid.return_value = target_uuid
        bitmap1 = bitarray(160)
        bitmap2 = bitarray(80)
        mock_cbt.get_cbt_bitmap.side_effect = [bitmap1.tobytes(),
                                               bitmap2.tobytes()]
        mock_cbt.get_cbt_size.return_value = 16777216

        with self.assertRaises(SR.SROSError) as exc:
            self.vdi.list_changed_blocks()
        # Test CBTChangedBlocksError is raised
        self.assertEqual(exc.exception.errno, 459)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_list_changed_blocks_larger_bitmap(self, context,
                                               mock_lock,
                                               mock_log, mock_cbt):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        # Set up scenario such that metadata chain looks like
        # source_vdi->snap_vdi->target_vdi
        snap_uuid = "snapUUID"
        target_uuid = "targetUUID"
        self.xenapi.VDI.get_uuid.return_value = target_uuid
        mock_cbt.get_cbt_child.side_effect = [snap_uuid, target_uuid]
        vdi_size1 = 5242880  # 5MB
        vdi_size2 = 5242880  # 5MB
        bitmap1 = bitarray(80)
        bitmap2 = bitarray(160)
        mock_cbt.get_cbt_size.side_effect = [vdi_size1, vdi_size2]
        mock_cbt.get_cbt_bitmap.side_effect = [bitmap1.tobytes(),
                                               bitmap2.tobytes()]
        bitmap1.bytereverse()
        bitmap2.bytereverse()
        # Trim bitmap to the expected size
        bitmap2 = bitmap2[:80]
        expected_string = base64.b64encode((bitmap1 | bitmap2).tobytes())
        expected_result = xmlrpc.client.dumps((expected_string, ), "", True)

        result = self.vdi.list_changed_blocks()
        self.assertEqual(result, expected_result)

    @testlib.with_context
    @mock.patch('VDI.cbtutil.get_cbt_size')
    @mock.patch('VDI.cbtutil.get_cbt_child')
    @mock.patch('VDI.cbtutil._call_cbt_util')
    @mock.patch('VDI.VDI._cbt_log_exists', autospec=True)
    @mock.patch('lock.LockImplementation', autospec=True)
    def test_list_changed_blocks_strip_sensitive_bitmap(self, context, mock_lock,
                                                        mock_log, mock_call,
                                                        mock_child, mock_size):
        context.setup_error_codes()
        # Create the test object and initialise
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        # Set up scenario such that metadata chain looks like
        # source_vdi->snap_vdi->target_vdi
        snap_uuid = "snapUUID"
        target_uuid = "targetUUID"
        self.xenapi.VDI.get_uuid.return_value = target_uuid
        mock_child.side_effect = [snap_uuid, target_uuid]
        vdi_size1 = 8388608  # 8MB
        vdi_size2 = 8388608  # 8MB
        bitmap1 = bitarray(128)
        bitmap2 = bitarray()
        # strip sensitive byte string
        data = b'\x0c\xbb3a\xf75Jy\x17\x04\x92;\x0b\x93\xf3E'
        bitmap2.frombytes(data)
        mock_size.side_effect = [vdi_size1, vdi_size2]
        mock_call.side_effect = [bitmap1.tobytes(), data]
        bitmap1.bytereverse()
        bitmap2.bytereverse()
        expected_string = base64.b64encode((bitmap1 | bitmap2).tobytes())
        expected_result = xmlrpc.client.dumps((expected_string, ), "", True)

        result = self.vdi.list_changed_blocks()
        self.assertEqual(result, expected_result)

    def _set_initial_state(self, vdi, cbt_enabled):
        self.xenapi.VDI.get_is_a_snapshot.return_value = False
        vdi.block_tracking_state = cbt_enabled

    def _set_CBT_chain_state(self, mock_logcheck, mock_cbt, parent_exists):
        parent_uuid = None
        if parent_exists:
            parent_uuid = "parentUUID"
            mock_cbt.get_cbt_parent.return_value = parent_uuid
        mock_logcheck.return_value = parent_exists
        child_uuid = "childUUID"
        mock_cbt.get_cbt_child.return_value = child_uuid
        return (parent_uuid, child_uuid)

    def _check_setting_state(self, vdi, cbt_enabled):
        self.assertEqual(vdi._get_blocktracking_status(), cbt_enabled)
        if cbt_enabled:
            return vdi._get_cbt_logpath(self.vdi_uuid)
        else:
            return None

    def _check_setting_not_changed(self):
        pass

    def _check_tapdisk_paused_and_resumed(self, check_mock, vdi_uuid):
        check_mock.tap_pause.assert_called_with(self.sr.session,
                                                self.sr_uuid, vdi_uuid)
        check_mock.tap_unpause.assert_called_with(self.sr.session,
                                                  self.sr_uuid, vdi_uuid)

    def _check_tapdisk_not_modified(self, mock):
        # python2-mock-1.0.1-9.el doesn't support these asserts
        #mock.tap_refresh.assert_not_called()
        #mock.tap_pause.assert_not_called()
        #mock.tap_unpause.assert_not_called()
        pass

    def _check_CBT_chain_created(self, vdi, mock_cbt, vdi_uuid,
                                 snap_uuid, parent_uuid, child_uuid):
        vdi_logpath = vdi._get_cbt_logpath(vdi_uuid)
        snap_logpath = vdi._get_cbt_logpath(snap_uuid)

        vdi.state_mock._rename.assert_called_with(vdi_logpath, snap_logpath)
        vdi.state_mock._create_cbt_log.assert_called_with()
        calls = [mock.call(snap_logpath, vdi_uuid),
                 mock.call(vdi_logpath, child_uuid)]

        if parent_uuid:
            parent_logpath = vdi._get_cbt_logpath(parent_uuid)
            calls.append(mock.call(parent_logpath, snap_uuid))

        mock_cbt.set_cbt_parent.assert_called_with(vdi_logpath, snap_uuid)
        mock_cbt.set_cbt_child.assert_has_calls(calls, any_order=True)
