import mock
import SR
import testlib
import unittest
import uuid
import VDI
import vhdutil
import xs_errors


class TestVDI(VDI.VDI):
    def load(self, vdi_uuid):
        self.vdi_type = vhdutil.VDI_TYPE_VHD
        self._state_mock = mock.Mock()
        ## TODO: Set self.path to something useful
        self.path = "/mock/sr_path/" + str(vdi_uuid)

    @property
    def state_mock(self):
        return self._state_mock

    def _ensure_cbt_space(self):
        super(TestVDI, self)._ensure_cbt_space()
        self.state_mock._ensure_cbt_space()

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
    @mock.patch('blktap2.VDI')
    @mock.patch('VDI.util')
    def test_configure_blocktracking_enable_success(self, context, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(mock_util, False)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

        self._check_setting_state(True)
        ## Check that metadata update calls made
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    @mock.patch('VDI.util')
    def test_configure_blocktracking_enable_already_enabled(self, context, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(mock_util, True)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

        self._check_setting_not_changed()
        self._check_tapdisk_not_modified(mock_bt_vdi)

    # Obsoleted by change to design, remove later
    # @testlib.with_context
    # @mock.patch('blktap2.VDI')
    # def test_configure_blocktracking_enable_already_defined(self, context, mock_bt_vdi):
    #     context.setup_error_codes()

    #     # Create the test object
    #     self.vdi = TestVDI(self.sr, self.vdi_uuid)

    #     self._set_initial_state('False')

    #     self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

    #     self._check_setting_state('True')
    #     self._check_tapdisk_paused_and_resumed(mock_bt_vdi)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    @mock.patch('VDI.util')
    def test_configure_blocktracking_disable_when_enabled(self, context, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(mock_util, True)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

        self._check_setting_state(False)
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    @mock.patch('VDI.util')
    def test_configure_blocktracking_disable_already_disabled(self, context, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(mock_util, False)

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
    @mock.patch('blktap2.VDI')
    @mock.patch('VDI.util')
    def test_configure_blocktracking_enable_refresh_fail(self, context, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(mock_util, False)
        mock_bt_vdi.tap_refresh.return_value = False

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    @mock.patch('VDI.util')
    def test_configure_blocktracking_disable_refresh_fail(self, context, mock_util, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(mock_util, True)
        mock_bt_vdi.tap_refresh.return_value = False

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, False)

    @testlib.with_context
    @mock.patch('VDI.util')
    def test_configure_blocktracking_enable_metadata_no_space(self, context, mock_util):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self.vdi.state_mock._ensure_cbt_space.side_effect = [ xs_errors.XenError('SRNoSpace') ]

        self._set_initial_state(mock_util, False)

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, True)

    @testlib.with_context
    @unittest.skip('metadata not implemented')
    def test_configure_blocktracking_enable_metadata_initialisation_fail(self, context):
        self.fail("Not implemented")

    def _set_initial_state(self, mock_util, cbt_enabled):
        self.xenapi.VDI.get_is_a_snapshot.return_value = False

        # This needs to change as state is determined by the presence or not of
        # the metadata file
        if (cbt_enabled) :
            mock_util.pathexists.return_value = True
        else:
            mock_util.pathexists.return_value = False

    def _check_setting_state(self, cbt_enabled):
        #self.xenapi.VDI.add_to_other_config.assert_called_with(mock.ANY, "cbt_enabled", cbt_enabled)
        pass

    def _check_setting_not_changed(self):
        #self.xenapi.VDI.add_to_other_config.assert_not_called()
        pass

    def _check_tapdisk_paused_and_resumed(self, check_mock):
        check_mock.tap_refresh.assert_called_with(mock.ANY, self.sr_uuid, self.vdi_uuid)
        check_mock.tap_pause.assert_not_called()
        check_mock.tap_unpause.assert_not_called()

    def _check_tapdisk_not_modified(self, mock):
        mock.tap_refresh.assert_not_called()
        mock.tap_pause.assert_not_called()
        mock.tap_unpause.assert_not_called()
