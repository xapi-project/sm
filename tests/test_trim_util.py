import unittest
import unittest.mock as mock
import testlib

from sm.core import util
from sm import trim_util

IOCTL_ERROR = "blkdiscard: /dev/VG_XenStorage-33f99d3e-1c69-1a64-e05d-8a84ef7b8efc/33f99d3e-1c69-1a64-e05d-8a84ef7b8efc_trim_lv: BLKDISCARD ioctl failed: Operation not supported"

EMPTY_VG_SPACE = 4 * 1024 * 1024


class AlwaysBusyLock(object):
    def acquireNoblock(self):
        return False


class AlwaysFreeLock(object):
    def __init__(self):
        self.acquired = False

    def acquireNoblock(self):
        self.acquired = True
        return True

    def release(self):
        self.acquired = False


class TestTrimUtil(unittest.TestCase, testlib.XmlMixIn):
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_error_code_trim_not_supported(self,
                                                   context,
                                                   sr_get_capability):
        sr_get_capability.return_value = []

        result = trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertXML("""
        <?xml version="1.0" ?>
        <trim_response>
            <key_value_pair>
                <key>errcode</key>
                <value>UnsupportedSRForTrim</value>
            </key_value_pair>
            <key_value_pair>
                <key>errmsg</key>
                <value>Trim on [some-uuid] not supported</value>
            </key_value_pair>
        </trim_response>
        """, result)

    @mock.patch('time.sleep', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_unable_to_obtain_lock_on_sr(self,
                                                 context,
                                                 sr_get_capability,
                                                 MockLock,
                                                 sleep):
        MockLock.return_value = AlwaysBusyLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        result = trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertXML("""
        <?xml version="1.0" ?>
        <trim_response>
            <key_value_pair>
                <key>errcode</key>
                <value>SRUnavailable</value>
            </key_value_pair>
            <key_value_pair>
                <key>errmsg</key>
                <value>Unable to get SR lock [some-uuid]</value>
            </key_value_pair>
        </trim_response>
        """, result)

    @mock.patch('time.sleep', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_sleeps_a_sec_and_retries_three_times(self,
                                                          context,
                                                          sr_get_capability,
                                                          MockLock,
                                                          sleep):
        MockLock.return_value = AlwaysBusyLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertEqual([
                mock.call(1),
                mock.call(1),
                mock.call(1)
            ],
            sleep.mock_calls
        )

    @mock.patch("sm.trim_util.lvutil.LVM_SIZE_INCREMENT", EMPTY_VG_SPACE)
    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_creates_an_lv(self,
                                   context,
                                   sr_get_capability,
                                   MockLock,
                                   lvutil):
        lvutil._getVGstats.return_value = {'physical_size': 0,
                                           'physical_utilisation': 0,
                                           'freespace': EMPTY_VG_SPACE}
        MockLock.return_value = AlwaysFreeLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        lvutil.create.assert_called_once_with(
            'some-uuid_trim_lv', 0, 'VG_XenStorage-some-uuid',
            size_in_percentage='100%F'
        )

    @mock.patch("sm.trim_util.lvutil.LVM_SIZE_INCREMENT", EMPTY_VG_SPACE)
    @mock.patch('sm.trim_util.util.pread2', autospec=True)
    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_removes_lv_no_leftover_trim_vol(self,
                                                     context,
                                                     sr_get_capability,
                                                     MockLock,
                                                     lvutil,
                                                     pread2):
        lvutil._getVGstats.return_value = {'physical_size': 0,
                                           'physical_utilisation': 0,
                                           'freespace': EMPTY_VG_SPACE}
        lvutil.exists.return_value = False
        MockLock.return_value = AlwaysFreeLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        pread2.assert_called_once_with(
            ["/usr/sbin/blkdiscard", "-v",
            "/dev/VG_XenStorage-some-uuid/some-uuid_trim_lv"])

    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_releases_lock(self,
                                   context,
                                   sr_get_capability,
                                   MockLock,
                                   lvutil):
        lvutil.exists.return_value = False
        sr_lock = MockLock.return_value = AlwaysFreeLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertFalse(sr_lock.acquired)

    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_removes_lv_with_leftover_trim_vol(self,
                                                      context,
                                                      sr_get_capability,
                                                      MockLock,
                                                      lvutil):
        lvutil.exists.return_value = True
        MockLock.return_value = AlwaysFreeLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertEqual([
                mock.call('/dev/VG_XenStorage-some-uuid/some-uuid_trim_lv'),
                mock.call(
                    '/dev/VG_XenStorage-some-uuid/some-uuid_trim_lv')
            ], lvutil.remove.mock_calls)

    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_lock_released_even_if_exception_raised(self,
                                                            context,
                                                            sr_get_capability,
                                                            MockLock,
                                                            lvutil):
        lvutil.create.side_effect = Exception('blah')
        srlock = AlwaysFreeLock()
        MockLock.return_value = srlock
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertFalse(srlock.acquired)

    @mock.patch("sm.trim_util.lvutil.LVM_SIZE_INCREMENT", EMPTY_VG_SPACE)
    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_when_exception_then_returns_generic_err(self,
                                                             context,
                                                             sr_get_capability,
                                                             MockLock,
                                                             lvutil):
        lvutil._getVGstats.return_value = {'physical_size': 0,
                                           'physical_utilisation': 0,
                                           'freespace': EMPTY_VG_SPACE}
        lvutil.create.side_effect = Exception('blah')
        srlock = AlwaysFreeLock()
        MockLock.return_value = srlock
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        result = trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertXML("""
        <?xml version="1.0" ?>
        <trim_response>
            <key_value_pair>
                <key>errcode</key>
                <value>UnknownTrimException</value>
            </key_value_pair>
            <key_value_pair>
                <key>errmsg</key>
                <value>Unknown Exception: trim failed on SR [some-uuid]</value>
            </key_value_pair>
        </trim_response>
        """, result)

    @mock.patch("sm.trim_util.lvutil.LVM_SIZE_INCREMENT", EMPTY_VG_SPACE)
    @mock.patch('sm.trim_util.util.pread2', autospec=True)
    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_when_trim_succeeded_returns_true(self,
                                                      context,
                                                      sr_get_capability,
                                                      MockLock,
                                                      lvutil,
                                                      pread2):
        lvutil._getVGstats.return_value = {'physical_size': 0,
                                           'physical_utilisation': 0,
                                           'freespace': EMPTY_VG_SPACE}
        MockLock.return_value = AlwaysFreeLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        result = trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertEqual('True', result)

    @mock.patch('sm.trim_util.time.time', autospec=True)
    def test_log_last_triggered_no_key(self, mock_time):
        session = mock.Mock()
        mock_time.return_value = 0
        session.xenapi.SR.get_by_uuid.return_value = 'sr_ref'
        session.xenapi.SR.get_other_config.return_value = {}

        trim_util._log_last_triggered(session, 'sr_uuid')

        session.xenapi.SR.add_to_other_config.assert_called_with(
            'sr_ref', trim_util.TRIM_LAST_TRIGGERED_KEY, '0')
        self.assertEqual(0, session.xenapi.SR.remove_from_other_config.call_count)

    @mock.patch('sm.trim_util.time.time', autospec=True)
    def test_log_last_triggered_has_key(self, mock_time):
        session = mock.Mock()
        mock_time.return_value = 0
        session.xenapi.SR.get_by_uuid.return_value = 'sr_ref'
        other_config = {trim_util.TRIM_LAST_TRIGGERED_KEY: '0'}
        session.xenapi.SR.get_other_config.return_value = other_config

        trim_util._log_last_triggered(session, 'sr_uuid')

        session.xenapi.SR.remove_from_other_config.assert_called_with(
            'sr_ref', trim_util.TRIM_LAST_TRIGGERED_KEY)
        session.xenapi.SR.add_to_other_config.assert_called_with(
            'sr_ref', trim_util.TRIM_LAST_TRIGGERED_KEY, '0')

    @mock.patch('sm.trim_util.time.time', autospec=True)
    @mock.patch('sm.trim_util.util.logException', autospec=True)
    def test_log_last_triggered_exc_logged(self, mock_log_exc, mock_time):
        session = mock.Mock()
        mock_time.return_value = 0
        session.xenapi.SR.get_by_uuid.side_effect = Exception()

        # This combination ensures that an exception does not cause the log
        # function to throw, but the exception is still logged
        trim_util._log_last_triggered(session, 'sr_uuid')

        self.assertEqual(1, mock_log_exc.call_count)

    @mock.patch("sm.trim_util.lvutil.LVM_SIZE_INCREMENT", EMPTY_VG_SPACE)
    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_returns_exception_when_sr_full(self,
                                   context,
                                   sr_get_capability,
                                   MockLock,
                                   lvutil):
        lvutil._getVGstats.return_value = {'physical_size': 0,
                                           'physical_utilisation': 0,
                                           'freespace': 0}
        MockLock.return_value = AlwaysFreeLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        result = trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        self.assertXML("""
        <?xml version="1.0" ?>
        <trim_response>
            <key_value_pair>
                <key>errcode</key>
                <value>Trim failed on full SR</value>
            </key_value_pair>
            <key_value_pair>
                <key>errmsg</key>
                <value>No space to claim on a full SR</value>
            </key_value_pair>
        </trim_response>
        """, result)

    @mock.patch("sm.trim_util.lvutil.LVM_SIZE_INCREMENT", EMPTY_VG_SPACE)
    @mock.patch('sm.trim_util.util.pread2', autospec=True)
    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_ioctl_not_supported(
            self, context, sr_get_capability, mock_lock, lvutil, mock_pread):
        """
        Check that ioctl not supported error is not propagated
        """
        # Arrange
        def pread2(cmd):
            raise util.CommandException(1, cmd, IOCTL_ERROR)
        mock_pread.side_effect = pread2
        lvutil._getVGstats.return_value = {'physical_size': 0,
                                           'physical_utilisation': 0,
                                           'freespace': EMPTY_VG_SPACE}
        mock_lock.return_value = AlwaysFreeLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        # Act
        result = trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        # Assert
        self.assertEqual("True", result)

    @mock.patch("sm.trim_util.lvutil.LVM_SIZE_INCREMENT", EMPTY_VG_SPACE)
    @mock.patch('sm.trim_util.util.pread2', autospec=True)
    @mock.patch('sm.trim_util.lvutil', autospec=True)
    @mock.patch('sm.trim_util.lock.Lock', autospec=True)
    @mock.patch('sm.trim_util.util.sr_get_capability', autospec=True)
    @testlib.with_context
    def test_do_trim_blkdiscard_error_not_ioctl(
            self, context, sr_get_capability, mock_lock, lvutil, mock_pread):
        """
        Check that blkdiscard errors are reported
        """
        # Arrange
        def pread2(cmd):
            raise util.CommandException(5, cmd, "IO Error")
        mock_pread.side_effect = pread2
        lvutil._getVGstats.return_value = {'physical_size': 0,
                                           'physical_utilisation': 0,
                                           'freespace': EMPTY_VG_SPACE}
        mock_lock.return_value = AlwaysFreeLock()
        sr_get_capability.return_value = [trim_util.TRIM_CAP]

        # Act
        result = trim_util.do_trim(None, {'sr_uuid': 'some-uuid'})

        # Assert
        self.assertXML("""
                <?xml version="1.0" ?>
                <trim_response>
                    <key_value_pair>
                        <key>errcode</key>
                        <value>TrimException</value>
                    </key_value_pair>
                    <key_value_pair>
                        <key>errmsg</key>
                        <value>IO Error</value>
                    </key_value_pair>
                </trim_response>
                """, result)
