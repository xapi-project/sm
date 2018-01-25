import mock
import SR
import testlib
import unittest
import uuid
import VDI
import vhdutil


class TestVDI(VDI.VDI):
    def load(self, vdi_uuid):
        self.vdi_type = vhdutil.VDI_TYPE_VHD


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
    def test_configure_blocktracking_enable_success(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(None)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

        self._check_setting_state('True')
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    def test_configure_blocktracking_enable_already_enabled(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state('True')

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

        self._check_setting_not_changed()
        self._check_tapdisk_not_modified(mock_bt_vdi)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    def test_configure_blocktracking_enable_already_defined(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state('False')

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

        self._check_setting_state('True')
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    def test_configure_blocktracking_disable_when_enabled(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state('True')

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'False')

        self._check_setting_state('False')
        self._check_tapdisk_paused_and_resumed(mock_bt_vdi)

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    def test_configure_blocktracking_disable_already_disabled(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state('False')

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'False')

        self._check_setting_not_changed()
        self._check_tapdisk_not_modified(mock_bt_vdi)

    @testlib.with_context
    def test_configure_blocktracking_enable_raw_vdi(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = VDI.VDI(self.sr, self.vdi_uuid)

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

    @testlib.with_context
    def test_configure_blocktracking_enable_snapshot(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)
        self.xenapi.VDI.get_is_a_snapshot.return_value = True

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    def test_configure_blocktracking_enable_refresh_fail(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(None)
        mock_bt_vdi.tap_refresh.return_value = False

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

    @testlib.with_context
    @mock.patch('blktap2.VDI')
    def test_configure_blocktracking_disable_refresh_fail(self, context, mock_bt_vdi):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state('True')
        mock_bt_vdi.tap_refresh.return_value = False

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'False')

    def _set_initial_state(self, cbt_enabled):
        self.xenapi.VDI.get_is_a_snapshot.return_value = False
        vdi_config = {}

        if (cbt_enabled) :
            vdi_config = {"cbt_enabled" : cbt_enabled}

        self.xenapi.VDI.get_other_config.return_value = vdi_config

    def _check_setting_state(self, cbt_enabled):
        self.xenapi.VDI.add_to_other_config.assert_called_with(mock.ANY, "cbt_enabled", cbt_enabled)

    def _check_setting_not_changed(self):
        self.xenapi.VDI.add_to_other_config.assert_not_called()

    def _check_tapdisk_paused_and_resumed(self, check_mock):
        check_mock.tap_refresh.assert_called_with(mock.ANY, self.sr_uuid, self.vdi_uuid)
        check_mock.tap_pause.assert_not_called()
        check_mock.tap_unpause.assert_not_called()

    def _check_tapdisk_not_modified(self, mock):
        mock.tap_refresh.assert_not_called()
        mock.tap_pause.assert_not_called()
        mock.tap_unpause.assert_not_called()
