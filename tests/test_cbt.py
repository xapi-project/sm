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
    def test_configure_blocktracking_enable_success(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state(None)

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

        self._check_setting_state('True')

    @testlib.with_context
    def test_configure_blocktracking_enable_already_enabled(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state('True')

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

        self._check_setting_not_changed()

    @testlib.with_context
    def test_configure_blocktracking_enable_already_defined(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state('False')

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

        self._check_setting_state('True')

    @testlib.with_context
    def test_configure_blocktracking_disable_when_enabled(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = TestVDI(self.sr, self.vdi_uuid)

        self._set_initial_state('True')

        self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'False')

        self._check_setting_state('False')

    @testlib.with_context
    def test_configure_blocktracking_enable_raw_vdi(self, context):
        context.setup_error_codes()

        # Create the test object
        self.vdi = VDI.VDI(self.sr, self.vdi_uuid)

        with self.assertRaises(SR.SROSError):
            self.vdi.configure_blocktracking(self.sr_uuid, self.vdi_uuid, 'True')

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

