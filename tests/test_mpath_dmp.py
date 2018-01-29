import mock
import unittest
import testlib
import mpath_dmp
import SR


class Test_mpath_dmp(unittest.TestCase):

    @testlib.with_context
    @mock.patch('mpath_dmp.util', autospec=True)
    def test_is_valid_multipath_device(self, context, util_mod):
        """
        Tests for checking validity of multipath device
        """

        # Setup errors codes
        context.setup_error_codes()

        # Test 'multipath -a' failure
        util_mod.doexec.side_effect = [(1, "out", "err")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        # Test 'multipath -c' with error and empty output
        util_mod.doexec.side_effect = [(0, "out", "err"), (1, "", ""), OSError()]
        with self.assertRaises(SR.SROSError) as exc:
            mpath_dmp._is_valid_multipath_device("xx")
        self.assertEqual(exc.exception.errno, 431)

        # Test 'multipath -c' with error but some output
        util_mod.doexec.side_effect = [(0, "out", "err"), (1, "xx", "")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))
        util_mod.doexec.side_effect = [(0, "out", "err"), (1, "xx", "yy")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))
        util_mod.doexec.side_effect = [(0, "out", "err"), (1, "", "yy")]
        self.assertFalse(mpath_dmp._is_valid_multipath_device("fake_dev"))

        # Test when everything is fine
        util_mod.doexec.side_effect = [(0, "out", "err"), (0, "out", "err")]
        self.assertTrue(mpath_dmp._is_valid_multipath_device("fake_dev"))
