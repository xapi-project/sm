import unittest
import unittest.mock as mock

import XenAPI

from sm import resetvdis

HOST_UUID = "3fa1e58f-2545-4b3b-8e07-c53a6bab0a41"
HOST_REF = "OpaqueRef:host1"
OTHER_HOST_REF = "OpaqueRef:host2"
SR_UUID = "3dd7d05e-8b7f-4d64-8801-ee2ee0b3ff81"
SR_REF = "OpaqueRef:sr1"
VDI_UUID = "b0dc7f19-4ee6-4d05-acd9-4a1b0f0e6be7"
VDI_REF = "OpaqueRef:vdi1"


@mock.patch('sm.resetvdis.util.SMlog', autospec=True)
class TestResetSr(unittest.TestCase):
    def setUp(self):
        cleanup_patcher = mock.patch('sm.resetvdis.cleanup', autospec=True)
        cleanup_patcher.start()
        lock_patcher = mock.patch('sm.resetvdis.lock.Lock', autospec=True)
        lock_patcher.start()

        self.addCleanup(mock.patch.stopall)

        self.session = mock.MagicMock()
        self.session.xenapi.host.get_by_uuid.return_value = HOST_REF
        self.session.xenapi.SR.get_by_uuid.return_value = SR_REF

    def set_vdis(self, sm_config):
        self.session.xenapi.VDI.get_all_records_where.return_value = {
            VDI_REF: {"uuid": VDI_UUID, "sm_config": sm_config}}

    def test_reset_sr_clears_host_key(self, mock_log):
        self.set_vdis({"host_%s" % HOST_REF: "RW"})

        resetvdis.reset_sr(self.session, HOST_UUID, SR_UUID, False)

        self.session.xenapi.VDI.remove_from_sm_config.assert_called_once_with(
            VDI_REF, "host_%s" % HOST_REF)

    def test_reset_sr_clears_activating_owned_by_host(self, mock_log):
        self.set_vdis({"activating": HOST_REF})

        resetvdis.reset_sr(self.session, HOST_UUID, SR_UUID, False)

        self.session.xenapi.VDI.remove_from_sm_config.assert_called_once_with(
            VDI_REF, "activating")

    def test_reset_sr_keeps_activating_owned_by_other_host(self, mock_log):
        self.set_vdis({"activating": OTHER_HOST_REF})

        resetvdis.reset_sr(self.session, HOST_UUID, SR_UUID, False)

        self.session.xenapi.VDI.remove_from_sm_config.assert_not_called()

    def test_reset_sr_keeps_legacy_activating(self, mock_log):
        # written by a host running a version predating owner encoding
        self.set_vdis({"activating": "True"})

        resetvdis.reset_sr(self.session, HOST_UUID, SR_UUID, False)

        self.session.xenapi.VDI.remove_from_sm_config.assert_not_called()

    def test_reset_sr_clears_paused_on_master(self, mock_log):
        self.set_vdis({"paused": "true"})

        resetvdis.reset_sr(self.session, HOST_UUID, SR_UUID, True)

        self.session.xenapi.VDI.remove_from_sm_config.assert_called_once_with(
            VDI_REF, "paused")


@mock.patch('sm.resetvdis.util.SMlog', autospec=True)
class TestResetVdi(unittest.TestCase):
    def setUp(self):
        self.session = mock.MagicMock()
        self.session.xenapi.VDI.get_by_uuid.return_value = VDI_REF

    def set_vdi(self, sm_config):
        self.session.xenapi.VDI.get_record.return_value = {
            "uuid": VDI_UUID, "SR": SR_REF, "sm_config": sm_config}

    def test_reset_vdi_force_clears_activating(self, mock_log):
        self.set_vdi({"activating": HOST_REF})

        clean = resetvdis.reset_vdi(self.session, VDI_UUID, force=True,
                                    term_output=False)

        self.assertTrue(clean)
        self.session.xenapi.VDI.remove_from_sm_config.assert_called_once_with(
            VDI_REF, "activating")

    def test_reset_vdi_force_clears_legacy_activating(self, mock_log):
        self.set_vdi({"activating": "True"})

        clean = resetvdis.reset_vdi(self.session, VDI_UUID, force=True,
                                    term_output=False)

        self.assertTrue(clean)
        self.session.xenapi.VDI.remove_from_sm_config.assert_called_once_with(
            VDI_REF, "activating")

    def test_reset_vdi_keeps_activating_of_valid_host(self, mock_log):
        self.set_vdi({"activating": HOST_REF})
        self.session.xenapi.host.get_record.return_value = {
            "uuid": HOST_UUID, "name_label": "host1"}

        resetvdis.reset_vdi(self.session, VDI_UUID, force=False,
                            term_output=False)

        self.session.xenapi.VDI.remove_from_sm_config.assert_not_called()

    def test_reset_vdi_clears_activating_of_invalid_host(self, mock_log):
        self.set_vdi({"activating": HOST_REF})
        self.session.xenapi.host.get_record.side_effect = XenAPI.Failure(
            ["HANDLE_INVALID", "host", HOST_REF])

        resetvdis.reset_vdi(self.session, VDI_UUID, force=False,
                            term_output=False)

        self.session.xenapi.VDI.remove_from_sm_config.assert_called_once_with(
            VDI_REF, "activating")

    def test_reset_vdi_keeps_legacy_activating_without_force(self, mock_log):
        # a legacy 'True' value cannot be attributed to a host, so only
        # --force may clear it
        self.set_vdi({"activating": "True"})

        resetvdis.reset_vdi(self.session, VDI_UUID, force=False,
                            term_output=False)

        self.session.xenapi.VDI.remove_from_sm_config.assert_not_called()

    def test_reset_vdi_term_output_prints_activating(self, mock_log):
        self.set_vdi({"activating": HOST_REF})

        with mock.patch('builtins.print') as mock_print:
            resetvdis.reset_vdi(self.session, VDI_UUID, force=True)

        self.session.xenapi.VDI.remove_from_sm_config.assert_called_once_with(
            VDI_REF, "activating")
        printed = "".join(str(c) for c in mock_print.call_args_list)
        self.assertIn("activating", printed)
