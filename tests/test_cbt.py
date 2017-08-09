import mock
import SR
import testlib
import unittest
import uuid
import VDI
import vhdutil
import xs_errors
import util
import errno


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

class TestCBT(unittest.TestCase):

    def setUp(self):
        self.sr = mock.MagicMock()
        self.vdi_uuid = uuid.uuid4()
        self.sr_uuid = uuid.uuid4()
        self.xenapi = mock.MagicMock()
        self.xenapi.VDI = mock.MagicMock()

        self.sr.session = mock.MagicMock()

        self.sr.session.xenapi = self.xenapi

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_configure_blocktracking_enable_success(self, context, mock_cbt, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

        logfile = self._check_setting_state(self.vdi, True)
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi, logfile)

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
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_configure_blocktracking_disable_when_enabled_without_parent(self, context, mock_cbt, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, True)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

        logfile = self._check_setting_state(self.vdi, False)
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi, logfile)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.util', autospec=True)
    def test_configure_blocktracking_disable_when_enabled_with_parent(self, context, mock_util, mock_cbt, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, True)
        self._set_CBT_chain_state(mock_util, mock_cbt, True) 
        expected_parent_path = '/mock/sr_path/parentUUID.log'

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

        logfile = self._check_setting_state(self.vdi, False)
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi, logfile)
        mock_cbt.setCBTChild.assert_called_with(expected_parent_path, uuid.UUID(int=0))

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
    def test_configure_blocktracking_enable_refresh_fail(self, context, mock_cbt, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, False)
        mock_bt_vdi.tap_refresh.return_value = False

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    @mock.patch('VDI.util', autospec=True)
    def test_configure_blocktracking_disable_refresh_fail(self, context, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(self.vdi, True)
        mock_bt_vdi.tap_refresh.return_value = False

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

    @testlib.with_context
    @mock.patch('VDI.util', autospec=True)
    def test_configure_blocktracking_enable_metadata_no_space(self, context, mock_util):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self.vdi.state_mock._ensure_cbt_space.side_effect = [ xs_errors.XenError('SRNoSpace') ]

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
        self.vdi.state_mock._get_cbt_logpath.side_effect = [ xs_errors.XenError('CBTActivateFailed') ]

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
        mock_cbt.createCBTLog.side_effect = Exception(errno.EIO)

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

        self._check_tapdisk_not_modified(mock_bt_vdi)
        self._check_setting_state(self.vdi, False)

    @testlib.with_context
    @mock.patch('blktap2.VDI', autospec=True)
    def test_configure_blocktracking_disable_metadata_deletion_fail(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        self.vdi.state_mock._delete_cbt_log.side_effect = [ xs_errors.XenError('CBTDeactivateFailed') ]

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
        #mock_cbt.getCBTConsistency.assert_not_called()

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_activate_consistent_success(self, context, mock_cbt):
        context.setup_error_codes()

        expected_log_path = '/mock/sr_path/{0}.log'.format(self.vdi_uuid)

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        mock_cbt.getCBTConsistency.side_effect = [ True ]

        log_path = self.vdi.activate(self.sr_uuid, self.vdi_uuid)

        mock_cbt.getCBTConsistency.assert_called_with(expected_log_path)
        self.assertEquals({'cbtlog': expected_log_path}, log_path)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_activate_consistency_check_fail(self, context, mock_cbt):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        mock_cbt.getCBTConsistency.side_effect = [ False ]

        with self.assertRaises(SR.SROSError) as cm:
            self.vdi.activate(self.sr_uuid, self.vdi_uuid)
        # Check we get the CBTMetadataInconsistent error
        self.assertEquals(cm.exception.errno, 459)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_deactivate_no_tracking_success(self, context, mock_cbt):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, False)

        self.vdi.deactivate(self.sr_uuid, self.vdi_uuid)

        # python2-mock-1.0.1-9.el doesn't support these asserts
        #mock_cbt.setCBTConsistency.assert_not_called()

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_deactivate_success(self, context, mock_cbt):
        context.setup_error_codes()

        expected_log_path = '/mock/sr_path/{0}.log'.format(self.vdi_uuid)

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)

        self.vdi.deactivate(self.sr_uuid, self.vdi_uuid)

        mock_cbt.setCBTConsistency.assert_called_with(expected_log_path, True)

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
    @mock.patch('VDI.util', autospec=True)
    def test_snapshot_success_no_parent(self, context, mock_util, mock_cbt):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self._set_initial_state(self.vdi, True)
        parent_uuid = self._set_CBT_chain_state(mock_util, mock_cbt, False) 
        snap_uuid = uuid.uuid4()

        self.vdi._cbt_snapshot(snap_uuid)

        self._check_CBT_chain_created(self.vdi, mock_cbt,
                                      self.vdi_uuid, snap_uuid, parent_uuid)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    @mock.patch('VDI.util', autospec=True)
    def test_snapshot_success_with_parent(self, context, mock_util, mock_cbt):
        #context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        # Set initial state
        self._set_initial_state(self.vdi, True)
        parent_uuid = self._set_CBT_chain_state(mock_util, mock_cbt, True) 
        snap_uuid = uuid.uuid4()

        self.vdi._cbt_snapshot(snap_uuid)
        self._check_CBT_chain_created(self.vdi, mock_cbt,
                                      self.vdi_uuid, snap_uuid, None)

    @testlib.with_context
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_resize_cbt_enabled(self, context, mock_cbt):
        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        # Set initial state
        self._set_initial_state(self.vdi, True)
        size = 2093050
        logpath = self.vdi._get_cbt_logpath(self.vdi_uuid)

        self.vdi.resize_cbt(self.sr_uuid, self.vdi_uuid, size)
        mock_cbt.set_cbt_size.assert_called_with(logpath, size)
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
    @mock.patch('VDI.cbtutil', autospec=True)
    def test_resize_exception(self, context, mock_cbt):
        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        # Set initial state
        self._set_initial_state(self.vdi, True)
        size = 2093050
        mock_cbt.set_cbt_size.side_effect = util.CommandException(errno.EINVAL)
       
        self.vdi.resize_cbt(self.sr_uuid, self.vdi_uuid, size)
        self._check_setting_state(self.vdi, False)
        self.xenapi.message.create.assert_called_once()

    def _set_initial_state(self, vdi, cbt_enabled):
        self.xenapi.VDI.get_is_a_snapshot.return_value = False
        vdi.block_tracking_state = cbt_enabled

    def _set_CBT_chain_state(self, mock_util, mock_cbt, parent_exists):
        parent_uuid = None
        if parent_exists:
            mock_cbt.getCBTParent.return_value = "parentUUID"
        mock_util.pathexists.return_value = parent_exists
        return parent_uuid

    def _check_setting_state(self, vdi, cbt_enabled):
        self.assertEquals(vdi._get_blocktracking_status(), cbt_enabled)
        if cbt_enabled:
            return vdi._get_cbt_logpath(self.vdi_uuid)
        else:
            return None

    def _check_setting_not_changed(self):
        pass

    def _check_tapdisk_paused_and_resumed(self, check_mock, logfile):
        check_mock.tap_refresh.assert_called_with(self.sr.session,
                                                  self.sr_uuid, self.vdi_uuid,
                                                  cbtlog=logfile)
        # python2-mock-1.0.1-9.el doesn't support these asserts
        #check_mock.tap_pause.assert_not_called()
        #check_mock.tap_unpause.assert_not_called()

    def _check_tapdisk_not_modified(self, mock):
        # python2-mock-1.0.1-9.el doesn't support these asserts
        #mock.tap_refresh.assert_not_called()
        #mock.tap_pause.assert_not_called()
        #mock.tap_unpause.assert_not_called()
        pass

    def _check_CBT_chain_created(self, vdi, mock_cbt, vdi_uuid,
                                 snap_uuid, parent_uuid):
        vdi_logpath = vdi._get_cbt_logpath(vdi_uuid)
        snap_logpath = vdi._get_cbt_logpath(snap_uuid)

        vdi.state_mock._rename.assert_called_with(vdi_logpath, snap_logpath)
        if parent_uuid:
            parent_logpath = self._get_cbt_logpath(parent_uuid)
            mock_cbt.setCBTChild.assert_called_with(parent_logpath, snap_uuid)
        vdi.state_mock._create_cbt_log.assert_called_with()
        mock_cbt.setCBTChild.assert_called_with(snap_logpath, vdi_uuid)
        mock_cbt.setCBTParent.assert_called_with(vdi_logpath, snap_uuid)
