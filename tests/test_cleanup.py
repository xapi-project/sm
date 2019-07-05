import errno
import unittest
import mock
import __builtin__

from uuid import uuid4

import cleanup
import lock

import util


class FakeXapi(object):
    def __init__(self):
        self.srRecord = {
            'name_label': 'dummy'
        }

    def isPluggedHere(self):
        return True

    def isMaster(self):
        return True


class AlwaysLockedLock(object):
    def acquireNoblock(self):
        return False


class AlwaysFreeLock(object):
    def acquireNoblock(self):
        return True


class TestRelease(object):
    pass

class IrrelevantLock(object):
    pass


def create_cleanup_sr(uuid=None):
    xapi = FakeXapi()
    return cleanup.SR(uuid=uuid, xapi=xapi, createLock=False, force=False)


class TestSR(unittest.TestCase):
    def setUp(self):
        self.sleep_patcher = mock.patch('cleanup.time.sleep')
        self.sleep_patcher.start()

    def tearDown(self):
        self.sleep_patcher.stop()

    def setup_abort_flag(self, ipc_mock, should_abort=False):
        flag = mock.Mock()
        flag.test = mock.Mock(return_value=should_abort)

        ipc_mock.return_value = flag

    def setup_mock_sr(selfs, mock_sr):
        xapi = FakeXapi()
        mock_sr.configure_mock(uuid=1234, xapi=xapi,
                               createLock=False, force=False)

    def mock_cleanup_locks(self):
        cleanup.lockActive = TestRelease()
        cleanup.lockActive.release = mock.Mock(return_value=None)

        cleanup.lockRunning = TestRelease()
        cleanup.lockRunning.release = mock.Mock(return_value=None)

    def test_lock_if_already_locked(self):
        """
        Given an already locked SR, a lock call
        increments the lock counter
        """

        sr = create_cleanup_sr()
        sr._srLock = IrrelevantLock()
        sr._locked = 1

        sr.lock()

        self.assertEquals(2, sr._locked)

    def test_lock_if_no_locking_is_used(self):
        """
        Given no srLock present, the lock operations don't touch
        the counter
        """

        sr = create_cleanup_sr()
        sr._srLock = None

        sr.lock()

        self.assertEquals(0, sr._locked)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    def test_lock_succeeds_if_lock_is_acquired(
            self,
            mock_ipc_flag):
        """
        After performing a lock, the counter equals to 1
        """

        self.setup_abort_flag(mock_ipc_flag)
        sr = create_cleanup_sr()
        sr._srLock = AlwaysFreeLock()

        sr.lock()

        self.assertEquals(1, sr._locked)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    def test_lock_raises_exception_if_abort_requested(
            self,
            mock_ipc_flag):
        """
        If IPC abort was requested, lock raises AbortException
        """

        self.setup_abort_flag(mock_ipc_flag, should_abort=True)
        sr = create_cleanup_sr()
        sr._srLock = AlwaysLockedLock()

        self.assertRaises(cleanup.AbortException, sr.lock)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    def test_lock_raises_exception_if_unable_to_acquire_lock(
            self,
            mock_ipc_flag):
        """
        If the lock is busy, SMException is raised
        """

        self.setup_abort_flag(mock_ipc_flag)
        sr = create_cleanup_sr()
        sr._srLock = AlwaysLockedLock()

        self.assertRaises(util.SMException, sr.lock)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    def test_lock_leaves_sr_consistent_if_unable_to_acquire_lock(
            self,
            mock_ipc_flag):
        """
        If the lock is busy, the lock counter is not incremented
        """

        self.setup_abort_flag(mock_ipc_flag)
        sr = create_cleanup_sr()
        sr._srLock = AlwaysLockedLock()

        try:
            sr.lock()
        except (util.SMException, cleanup.AbortException) as e:
            pass

        self.assertEquals(0, sr._locked)

    def test_gcPause_fist_point_legal(self):
        """
        Make sure the fist point has been added to the array of legal
        fist points.
        """
        self.assertTrue(util.fistpoint.is_legal(util.GCPAUSE_FISTPOINT))

    @mock.patch('util.fistpoint', autospec=True)
    @mock.patch('cleanup.SR', autospec=True)
    @mock.patch('cleanup.Util.runAbortable')
    def test_gcPause_calls_fist_point(
            self,
            mock_abortable,
            mock_sr,
            mock_fist):
        """
        Call fist point if active and not abortable sleep.
        """
        self.setup_mock_sr(mock_sr)

        # Fake that we have an active fist point.
        mock_fist.is_active.return_value = True

        cleanup._gcLoopPause(mock_sr, False)

        # Make sure we check for correct fist point.
        mock_fist.is_active.assert_called_with(util.GCPAUSE_FISTPOINT)

        # Make sure we are calling the fist point.
        mock_fist.activate_custom_fn.assert_called_with(util.GCPAUSE_FISTPOINT,
                                                        mock.ANY)

        # And don't call abortable sleep
        mock_abortable.assert_not_called()

    @mock.patch('util.fistpoint', autospec=True)
    @mock.patch('cleanup.SR', autospec=True)
    @mock.patch('cleanup.Util.runAbortable')
    def test_gcPause_calls_abortable_sleep(
            self,
            mock_abortable,
            mock_sr,
            mock_fist_point):
        """
        Call abortable sleep if fist point is not active.
        """
        self.setup_mock_sr(mock_sr)

        # Fake that the fist point is not active.
        mock_fist_point.is_active.return_value = False

        cleanup._gcLoopPause(mock_sr, False)

        # Make sure we check for the active fist point.
        mock_fist_point.is_active.assert_called_with(util.GCPAUSE_FISTPOINT)

        # Fist point is not active so call abortable sleep.
        mock_abortable.assert_called_with(mock.ANY, None, mock_sr.uuid,
                                          mock.ANY, cleanup.VDI.POLL_INTERVAL,
                                          cleanup.GCPAUSE_DEFAULT_SLEEP * 1.1)

    @mock.patch('cleanup.SR', autospec=True)
    @mock.patch('cleanup._abort')
    def test_lock_released_by_abort_when_held(
            self,
            mock_abort,
            mock_sr):
        """
        If _abort returns True make sure we release the lockActive which will
        have been held by _abort, also check that we return True.
        """
        self.setup_mock_sr(mock_sr)

        # Fake that abort returns True, so we hold lockActive.
        mock_abort.return_value = True

        # Setup mock of release function.
        cleanup.lockActive = TestRelease()
        cleanup.lockActive.release = mock.Mock(return_value=None)

        ret = cleanup.abort(mock_sr, False)

        # Pass on the return from _abort.
        self.assertEquals(True, ret)

        # We hold lockActive so make sure we release it.
        self.assertEquals(cleanup.lockActive.release.call_count, 1)

    @mock.patch('cleanup.SR', autospec=True)
    @mock.patch('cleanup._abort')
    def test_lock_not_released_by_abort_when_not_held(
            self,
            mock_abort,
            mock_sr):
        """
        If _abort returns False don't release lockActive and ensure that
        False returned by _abort is passed on.
        """
        self.setup_mock_sr(mock_sr)

        # Fake _abort returning False.
        mock_abort.return_value = False

        # Mock lock release function.
        cleanup.lockActive = TestRelease()
        cleanup.lockActive.release = mock.Mock(return_value=None)

        ret = cleanup.abort(mock_sr, False)

        # Make sure pass on False returned by _abort
        self.assertEquals(False, ret)

        # Make sure we did not release the lock as we don't have it.
        self.assertEquals(cleanup.lockActive.release.call_count, 0)

    @mock.patch('cleanup._abort')
    @mock.patch.object(__builtin__, 'raw_input')
    def test_abort_optional_renable_active_held(
            self,
            mock_raw_input,
            mock_abort):
        """
        Cli has option to re enable gc make sure we release the locks
        correctly if _abort returns True.
        """
        mock_abort.return_value = True
        mock_raw_input.return_value = None

        self.mock_cleanup_locks()

        cleanup.abort_optional_reenable(None)

        # Make sure released lockActive
        self.assertEquals(cleanup.lockActive.release.call_count, 1)

        # Make sure released lockRunning
        self.assertEquals(cleanup.lockRunning.release.call_count, 1)

    @mock.patch('cleanup._abort')
    @mock.patch.object(__builtin__, 'raw_input')
    def test_abort_optional_renable_active_not_held(
            self,
            mock_raw_input,
            mock_abort):
        """
        Cli has option to reenable gc make sure we release the locks
        correctly if _abort return False.
        """
        mock_abort.return_value = False
        mock_raw_input.return_value = None

        self.mock_cleanup_locks()

        cleanup.abort_optional_reenable(None)

        # Don't release lockActive, we don't hold it.
        self.assertEquals(cleanup.lockActive.release.call_count, 0)

        # Make sure released lockRunning
        self.assertEquals(cleanup.lockRunning.release.call_count, 1)

    @mock.patch('cleanup.init')
    def test__abort_returns_true_when_get_lock(
            self,
            mock_init):
        """
        _abort should return True when it can get
        the lockActive straight off the bat.
        """
        cleanup.lockActive = AlwaysFreeLock()
        ret = cleanup._abort(None)
        self.assertEquals(ret, True)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    @mock.patch('cleanup.init')
    def test__abort_return_false_if_flag_not_set(
            self,
            mock_init,
            mock_ipcflag):
        """
        If flag not set return False.
        """
        mock_init.return_value = None

        # Fake the flag returning False.
        mock_ipcflag.return_value.set.return_value = False

        # Not important for this test but we call it so mock it.
        cleanup.lockActive = AlwaysLockedLock()

        ret = cleanup._abort(None)

        self.assertEqual(mock_ipcflag.return_value.set.call_count, 1)
        self.assertEqual(ret, False)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    @mock.patch('cleanup.init')
    def test__abort_should_raise_if_cant_get_lock(
            self,
            mock_init,
            mock_ipcflag):
        """
        _abort should raise an exception if it completely
        fails to get lockActive.
        """
        mock_init.return_value = None

        # Fake return true so we don't bomb out straight away.
        mock_ipcflag.return_value.set.return_value = True

        # Fake never getting the lock.
        cleanup.lockActive = AlwaysLockedLock()

        with self.assertRaises(util.CommandException):
            cleanup._abort(None)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    @mock.patch('cleanup.init')
    def test__abort_should_succeed_if_aquires_on_second_attempt(
            self,
            mock_init,
            mock_ipcflag):
        """
        _abort should succeed if gets lock on second attempt
        """
        mock_init.return_value = None

        # Fake return true so we don't bomb out straight away.
        mock_ipcflag.return_value.set.return_value = True

        # Use side effect to fake failing to get the lock
        # on the first call, succeeding on the second.
        mocked_lock = AlwaysLockedLock()
        mocked_lock.acquireNoblock = mock.Mock()
        mocked_lock.acquireNoblock.side_effect = [False, True]
        cleanup.lockActive = mocked_lock

        ret = cleanup._abort(None)

        self.assertEqual(mocked_lock.acquireNoblock.call_count, 2)
        self.assertEqual(ret, True)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    @mock.patch('cleanup.init')
    def test__abort_should_fail_if_reaches_maximum_retries_for_lock(
            self,
            mock_init,
            mock_ipcflag):
        """
        _abort should fail if we max out the number of attempts for
        obtaining the lock.
        """
        mock_init.return_value = None

        # Fake return true so we don't bomb out straight away.
        mock_ipcflag.return_value.set.return_value = True

        # Fake a series of failed attempts to get the lock.
        mocked_lock = AlwaysLockedLock()
        mocked_lock.acquireNoblock = mock.Mock()

        # +1 to SR.LOCK_RETRY_ATTEMPTS as we attempt to get lock
        # once outside the loop.
        side_effect = [False]*(cleanup.SR.LOCK_RETRY_ATTEMPTS + 1)

        # Make sure we are not trying once again
        side_effect.append(True)

        mocked_lock.acquireNoblock.side_effect = side_effect
        cleanup.lockActive = mocked_lock

        # We've failed repeatedly to gain the lock so raise exception.
        with self.assertRaises(util.CommandException) as te:
            cleanup._abort(None)

        the_exception = te.exception
        self.assertIsNotNone(the_exception)
        self.assertEqual(errno.ETIMEDOUT, the_exception.code)
        self.assertEqual(mocked_lock.acquireNoblock.call_count,
                         cleanup.SR.LOCK_RETRY_ATTEMPTS+1)

    @mock.patch('cleanup.IPCFlag', autospec=True)
    @mock.patch('cleanup.init')
    def test__abort_succeeds_if_gets_lock_on_final_attempt(
            self,
            mock_init,
            mock_ipcflag):
        """
        _abort succeeds if we get the lockActive on the final retry
        """
        mock_init.return_value = None
        mock_ipcflag.return_value.set.return_value = True
        mocked_lock = AlwaysLockedLock()
        mocked_lock.acquireNoblock = mock.Mock()

        # +1 to SR.LOCK_RETRY_ATTEMPTS as we attempt to get lock
        # once outside the loop.
        side_effect = [False]*(cleanup.SR.LOCK_RETRY_ATTEMPTS)

        # On the final attempt we succeed.
        side_effect.append(True)

        mocked_lock.acquireNoblock.side_effect = side_effect
        cleanup.lockActive = mocked_lock

        ret = cleanup._abort(None)
        self.assertEqual(mocked_lock.acquireNoblock.call_count,
                         cleanup.SR.LOCK_RETRY_ATTEMPTS+1)
        self.assertEqual(ret, True)

    @mock.patch('cleanup.lock', autospec=True)
    def test_file_vdi_delete(self, mock_lock):
        """
        Test to confirm fix for HFX-651
        """
        mock_lock.Lock = mock.MagicMock(spec=lock.Lock)

        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        vdi_uuid = uuid4()

        vdi = cleanup.VDI(sr, str(vdi_uuid), False)

        vdi.delete()
        mock_lock.Lock.cleanupAll.assert_called_with(str(vdi_uuid))
