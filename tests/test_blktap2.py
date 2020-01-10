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
    def test_linknbd(self, nbd_link2, nbd_link):
        self.vdi.tap = blktap2.Tapdisk(123, 456, "blah", "blah", "blah")
        nbd_link.from_uuid.return_value = nbd_link2
        self.vdi.linkNBD("blahblah", "yadayada")
        expected_path = '/run/blktap-control/nbd%d.%d' % (123, 456)
        nbd_link.from_uuid.assert_called_with("blahblah", "yadayada")
        nbd_link2.mklink.assert_called_with(expected_path)

    def reset_removedirmocks(self, mock_rmdir, mock_unlink, mock_listdir,
                             mock_split, mock_lexists):
        mock_rmdir.reset_mock()
        mock_unlink.reset_mock()
        mock_listdir.reset_mock()
        mock_split.reset_mock()
        mock_lexists.reset_mock()

    @mock.patch("blktap2.os.path.lexists", autospec=True)
    @mock.patch("blktap2.os.path.split", autospec=True)
    @mock.patch("blktap2.os.listdir", autospec=True)
    @mock.patch("blktap2.os.unlink", autospec=True)
    @mock.patch("blktap2.os.rmdir", autospec=True)
    def test_removedirs(self, mock_rmdir, mock_unlink, mock_listdir,
                        mock_split, mock_lexists):
        # Path does not exist
        mock_lexists.return_value = False
        blktap2.rmdirs("blah/blah1/blah2.txt")
        mock_lexists.assert_called_with("blah/blah1/blah2.txt")
        self.assertEquals(mock_split.call_count, 0)

        self.reset_removedirmocks(mock_rmdir, mock_unlink,
                                  mock_listdir, mock_split, mock_lexists)
        # Split returns nothing.
        mock_lexists.return_value = True
        mock_split.return_value = [None, None]
        blktap2.rmdirs("blah/blah1/blah2.txt")
        mock_lexists.assert_called_with("blah/blah1/blah2.txt")
        mock_split.assert_called_with("blah/blah1/blah2.txt")
        self.assertEquals(mock_rmdir.call_count, 0)
        self.assertEquals(mock_unlink.call_count, 0)
        self.assertEquals(mock_listdir.call_count, 0)

        self.reset_removedirmocks(mock_rmdir, mock_unlink,
                                  mock_listdir, mock_split, mock_lexists)
        # Split returns single file.
        mock_lexists.return_value = True
        mock_split.return_value = [None, "blah.txt"]
        blktap2.rmdirs("blah/blah1/blah2.txt")
        mock_lexists.assert_called_with("blah/blah1/blah2.txt")
        mock_split.assert_called_with("blah/blah1/blah2.txt")
        self.assertEquals(mock_rmdir.call_count, 0)
        self.assertEquals(mock_listdir.call_count, 0)
        mock_unlink.assert_called_with("blah/blah1/blah2.txt")

        self.reset_removedirmocks(mock_rmdir, mock_unlink,
                                  mock_listdir, mock_split, mock_lexists)
        # Split returns valid stuff. but directory is not empty
        mock_lexists.return_value = True
        mock_split.return_value = ["blah/blah1/", "blah2.txt"]
        mock_listdir.return_value = ["stuff.txt", "more_stuff.txt"]
        blktap2.rmdirs("blah/blah1/blah2.txt")
        mock_lexists.assert_called_with("blah/blah1/blah2.txt")
        mock_split.assert_called_with("blah/blah1/blah2.txt")
        self.assertEquals(mock_rmdir.call_count, 0)
        mock_listdir.assert_called_with("blah/blah1/")
        mock_unlink.assert_called_with("blah/blah1/blah2.txt")

        self.reset_removedirmocks(mock_rmdir, mock_unlink,
                                  mock_listdir, mock_split, mock_lexists)
        # Everything is good to remove
        mock_lexists.return_value = True
        mock_split.return_value = ["blah/blah1/", "blah2.txt"]
        mock_listdir.return_value = []
        blktap2.rmdirs("blah/blah1/blah2.txt")
        mock_lexists.assert_called_with("blah/blah1/blah2.txt")
        mock_split.assert_called_with("blah/blah1/blah2.txt")
        mock_rmdir.assert_called_with("blah/blah1/")
        mock_listdir.assert_called_with("blah/blah1/")
        mock_unlink.assert_called_with("blah/blah1/blah2.txt")
