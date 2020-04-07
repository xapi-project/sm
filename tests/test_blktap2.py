import unittest
import blktap2
import mock
import testlib
import os


class TestVDI(unittest.TestCase):
    # This can't use autospec as vdi is created in __init__
    # See https://docs.python.org/3/library/unittest.mock.html#autospeccing
    @mock.patch('blktap2.VDI.TargetDriver')
    @mock.patch('blktap2.Lock', autospec=True)
    def setUp(self, mock_lock, mock_target):
        mock_target.get_vdi_type.return_value = 'phy'

        def mock_handles(type_str):
            return type_str == 'udev'

        mock_target.vdi.sr.handles.side_effect = mock_handles

        self.vdi = blktap2.VDI('uuid', mock_target, None)
        self.vdi.target = mock_target

    def test_tap_wanted_returns_true_for_udev_device(self):
        result = self.vdi.tap_wanted()

        self.assertEquals(True, result)

    def test_get_tap_type_returns_aio_for_udev_device(self):
        result = self.vdi.get_tap_type()

        self.assertEquals('aio', result)

    class NBDLinkForTest(blktap2.VDI.NBDLink):
        __name__ = "bob"

    @mock.patch('blktap2.VDI.NBDLink', autospec=NBDLinkForTest)
    @mock.patch('blktap2.VDI.NBDLink', autospec=NBDLinkForTest)
    def test_linknbd_not_called_for_no_tap(self, nbd_link2, nbd_link):
        self.vdi.linkNBD("blahblah", "yadayada")
        self.assertEquals(nbd_link.from_uuid.call_count, 0)

    @mock.patch('blktap2.VDI.NBDLink', autospec=NBDLinkForTest)
    @mock.patch('blktap2.VDI.NBDLink', autospec=NBDLinkForTest)
    def test_linknbd(self, nbd_link2, nbd_link):
        self.vdi.tap = blktap2.Tapdisk(123, 456, "blah", "blah", "blah")
        nbd_link.from_uuid.return_value = nbd_link2
        self.vdi.linkNBD("blahblah", "yadayada")
        expected_path = '/run/blktap-control/nbd%d.%d' % (123, 456)
        nbd_link.from_uuid.assert_called_with("blahblah", "yadayada")
        nbd_link2.mklink.assert_called_with(expected_path)


