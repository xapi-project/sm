import unittest
import blktap2
import mock


class TestVDI(unittest.TestCase):
    @mock.patch('blktap2.VDI.TargetDriver')
    def setUp(self, mock_target):
        mock_target.get_vdi_type.return_value = 'phy'
        mock_target.vdi.sr.sm_config = {'type': 'cd'}

        self.vdi = blktap2.VDI('uuid', mock_target, None)
        self.vdi.target = mock_target

    def test_tap_wanted_returns_true_for_physical_dvd_drive(self):
        result = self.vdi.tap_wanted()

        self.assertEquals(True, result)

    def test_get_tap_type_returns_aio_for_physical_dvd_drive(self):
        result = self.vdi.get_tap_type()

        self.assertEquals('aio', result)
