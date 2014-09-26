import unittest
import udevSR
import SRCommand


VDI_LOCATION = '/path/to/vdi'


class MockUdevSR(udevSR.udevSR):
    def load(self, sr_uuid):
        pass

    def get_vdi_location(self, uuid):
        return VDI_LOCATION


class TestVdi(unittest.TestCase):
    def test_vdi_succeeds_if_vdi_location_not_in_params_dictionary(self):
        srcmd = SRCommand.SRCommand('driver_info')
        srcmd.params = {'command': 'cmd'}
        sr_uuid = 'sr_uuid'
        udev_sr = MockUdevSR(srcmd, sr_uuid)

        self.assertEquals(None, udev_sr.srcmd.params.get('vdi_location'))

        udev_vdi = udev_sr.vdi('vdi_uuid')

        self.assertEquals(VDI_LOCATION, udev_vdi.location)
