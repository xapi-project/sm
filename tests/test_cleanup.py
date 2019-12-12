import errno
import unittest
import mock
import __builtin__

from uuid import uuid4

import cleanup
import lock

import util
import xs_errors
import os
import stat


class FakeFile(object):
    pass


class FakeException(Exception):
    pass


class FakeUtil:
    record = []

    def log(input):
        FakeUtil.record.append(input)
    log = staticmethod(log)


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

    @mock.patch('cleanup.VDI', autospec=True)
    @mock.patch('cleanup.SR._liveLeafCoalesce', autospec=True)
    @mock.patch('cleanup.SR._snapshotCoalesce', autospec=True)
    def test_coalesceLeaf(self, mock_srSnapshotCoalesce,
                          mock_srLeafCoalesce, mock_vdi):

        mock_vdi.canLiveCoalesce.return_value = True
        mock_srLeafCoalesce.return_value = "This is a test"
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        vdi_uuid = uuid4()
        vdi = cleanup.VDI(sr, str(vdi_uuid), False)

        res = sr._coalesceLeaf(vdi)
        self.assertEquals(res, "This is a test")
        self.assertEqual(sr._liveLeafCoalesce.call_count, 1)
        self.assertEqual(sr._snapshotCoalesce.call_count, 0)

    @mock.patch('cleanup.VDI', autospec=True)
    @mock.patch('cleanup.SR._liveLeafCoalesce', autospec=True)
    @mock.patch('cleanup.SR._snapshotCoalesce', autospec=True)
    def test_coalesceLeaf_coalesce_failed(self,
                                          mock_srSnapshotCoalesce,
                                          mock_srLeafCoalesce,
                                          mock_vdi):

        mock_vdi.canLiveCoalesce.return_value = False
        mock_srSnapshotCoalesce.return_value = False
        mock_srLeafCoalesce.return_value = False
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        vdi_uuid = uuid4()
        vdi = cleanup.VDI(sr, str(vdi_uuid), False)

        res = sr._coalesceLeaf(vdi)
        self.assertFalse(res)

    @mock.patch('cleanup.VDI.canLiveCoalesce',
                autospec=True, return_value=False)
    @mock.patch('cleanup.VDI.getSizeVHD',
                autospec=True, return_value=1024)
    @mock.patch('cleanup.Util.log')
    @mock.patch('cleanup.SR._snapshotCoalesce',
                autospec=True, return_value=True)
    def test_coalesceLeaf_size_the_same(self,
                                        mock_srSnapshotCoalesce,
                                        mock_log,
                                        mock_vdisize,
                                        mockliveCoalesce):

        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        vdi_uuid = uuid4()
        vdi = cleanup.VDI(sr, str(vdi_uuid), False)

        with self.assertRaises(util.SMException) as exc:
            res = sr._coalesceLeaf(vdi)

        self.assertEqual("VDI {uuid} could not be"
                         " coalesced".format(uuid=vdi_uuid),
                         exc.exception.message)

    @mock.patch('cleanup.VDI.canLiveCoalesce', autospec=True,
                return_value=False)
    @mock.patch('cleanup.VDI.getSizeVHD', autospec=True)
    @mock.patch('cleanup.SR._snapshotCoalesce', autospec=True,
                return_value=True)
    @mock.patch('cleanup.Util.log')
    def test_coalesceLeaf_size_bigger(self, mock_log,
                                      mock_snapshotCoalesce, mock_vhdSize,
                                      mock_vdiLiveCoalesce):

        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        vdi_uuid = uuid4()
        vdi = cleanup.VDI(sr, str(vdi_uuid), False)

        mock_vhdSize.side_effect = iter([1024, 4096, 4096, 8000, 8000, 16000])

        sr._snapshotCoalesce = mock.MagicMock(autospec=True)
        sr._snapshotCoalesce.return_value = True

        with self.assertRaises(util.SMException) as exc:
            res = sr._coalesceLeaf(vdi)

        self.assertEqual("VDI {uuid} could not be"
                         " coalesced".format(uuid=vdi_uuid),
                         exc.exception.message)

    @mock.patch('cleanup.VDI.canLiveCoalesce', autospec=True)
    @mock.patch('cleanup.VDI.getSizeVHD', autospec=True)
    @mock.patch('cleanup.SR._snapshotCoalesce', autospec=True,
                return_value=True)
    @mock.patch('cleanup.SR._liveLeafCoalesce', autospec=True,
                return_value="This is a Test")
    @mock.patch('cleanup.Util.log')
    def test_coalesceLeaf_success_after_4_iterations(self,
                                                     mock_log,
                                                     mock_liveLeafCoalesce,
                                                     mock_snapshotCoalesce,
                                                     mock_vhdSize,
                                                     mock_vdiLiveCoalesce):
        mock_vdiLiveCoalesce.side_effect = iter([False, False, False, True])
        mock_snapshotCoalesce.side_effect = iter([True, True, True])
        mock_vhdSize.side_effect = iter([1024, 1023, 1023, 1022, 1022, 1021])

        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        vdi_uuid = uuid4()
        vdi = cleanup.VDI(sr, str(vdi_uuid), False)

        res = sr._coalesceLeaf(vdi)

        self.assertEqual(res, "This is a Test")
        self.assertEqual(4, mock_vdiLiveCoalesce.call_count)
        self.assertEqual(3, mock_snapshotCoalesce.call_count)
        self.assertEqual(6, mock_vhdSize.call_count)

    @mock.patch('cleanup.Util.log')
    def test_findLeafCoalesceable_forbidden1(self, mock_log):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        sr.xapi.srRecord = {"other_config": {cleanup.VDI.DB_COALESCE: "false"}}

        res = sr.findLeafCoalesceable()
        self.assertEqual(res, [])
        mock_log.assert_called_with("Coalesce disabled for this SR")

    @mock.patch('cleanup.Util.log')
    def test_findLeafCoalesceable_forbidden2(self, mock_log):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        sr.xapi.srRecord =\
            {"other_config":
             {cleanup.VDI.DB_LEAFCLSC: cleanup.VDI.LEAFCLSC_DISABLED}}

        res = sr.findLeafCoalesceable()
        self.assertEqual(res, [])
        mock_log.assert_called_with("Leaf-coalesce disabled for this SR")

    @mock.patch('cleanup.Util.log')
    def test_findLeafCoalesceable_forbidden3(self, mock_log):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        sr.xapi.srRecord = {"other_config":
                            {cleanup.VDI.DB_LEAFCLSC:
                             cleanup.VDI.LEAFCLSC_DISABLED,
                             cleanup.VDI.DB_COALESCE:
                             "false"}}

        res = sr.findLeafCoalesceable()
        self.assertEqual(res, [])
        mock_log.assert_called_with("Coalesce disabled for this SR")

    @mock.patch('cleanup.Util.log')
    def test_findLeafCoalesceable_forbidden4(self, mock_log):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        sr.xapi.srRecord = {"other_config": {cleanup.VDI.DB_LEAFCLSC:
                                             cleanup.VDI.LEAFCLSC_DISABLED,
                                             cleanup.VDI.DB_COALESCE:
                                             "true"}}

        res = sr.findLeafCoalesceable()
        self.assertEqual(res, [])
        mock_log.assert_called_with("Leaf-coalesce disabled for this SR")

    @mock.patch('cleanup.Util.log')
    def test_findLeafCoalesceable_forbidden5(self, mock_log):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        sr.xapi.srRecord = {"other_config": {cleanup.VDI.DB_LEAFCLSC:
                                             cleanup.VDI.LEAFCLSC_FORCE,
                                             cleanup.VDI.DB_COALESCE:
                                             "false"}}

        res = sr.findLeafCoalesceable()
        self.assertEqual(res, [])
        mock_log.assert_called_with("Coalesce disabled for this SR")

# Utils for testing gatherLeafCoalesceable.

    def srWithOneGoodVDI(self, mock_getConfig, goodConfig):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))

        vdi_uuid = uuid4()
        if goodConfig:
            mock_getConfig.side_effect = goodConfig
        else:
            mock_getConfig.side_effect = iter(["good", False, "blah", "blah"])
        good = cleanup.VDI(sr, str(vdi_uuid), False)
        sr.vdis = {"good": good}
        return sr, good

    def addBadVDITOSR(self, sr, config, coalesceable=True):
        vdi_uuid = uuid4()
        bad = cleanup.VDI(sr, str(vdi_uuid), False)
        bad.getConfig = mock.MagicMock(side_effect=iter(config))
        bad.isLeafCoalesceable = mock.MagicMock(return_value=coalesceable)
        sr.vdis.update({"bad": bad})
        return bad

    def gather_candidates(self, mock_getConfig, config, coalesceable=True,
                          failed=False, expected=None, goodConfig=None):
        sr, good = self.srWithOneGoodVDI(mock_getConfig, goodConfig)
        bad = self.addBadVDITOSR(sr, config, coalesceable=coalesceable)
        if failed:
            sr._failedCoalesceTargets = [bad]

        res = []
        sr.gatherLeafCoalesceable(res)
        self.assertEqual(res, [good])

    @mock.patch("cleanup.AUTO_ONLINE_LEAF_COALESCE_ENABLED", True)
    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.VDI.isLeafCoalesceable', autospec=True,
                return_value=True)
    @mock.patch('cleanup.VDI.getConfig', autospec=True)
    def test_gather_candidates_leaf_not_coalescable(self, mock_getConfig,
                                                    mock_isLeafCoalesceable,
                                                    mock_leafCoalesceForbidden
                                                    ):

        """ The bad vdi returns false for isLeafCoalesceable and is not
            added to the list.
        """
        self.gather_candidates(mock_getConfig,
                               iter(["blah", False, "blah", "blah"]),
                               coalesceable=False)

    @mock.patch("cleanup.AUTO_ONLINE_LEAF_COALESCE_ENABLED", True)
    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.VDI.isLeafCoalesceable', autospec=True,
                return_value=True)
    @mock.patch('cleanup.VDI.getConfig', autospec=True)
    def test_gather_candidates_failed_candidates(self,
                                                 mock_getConfig,
                                                 mock_isLeafCoalesceable,
                                                 mock_leafCoalesceForbidden):

        """ The bad vdi is in the failed list so is not added to the list."""
        self.gather_candidates(mock_getConfig, iter(["blah", False, "blah",
                                                     "blah"]), failed=True)

    @mock.patch("cleanup.AUTO_ONLINE_LEAF_COALESCE_ENABLED", True)
    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.VDI.isLeafCoalesceable', autospec=True,
                return_value=True)
    @mock.patch('cleanup.VDI.getConfig', autospec=True)
    def test_gather_candidates_reset(self, mock_getConfig,
                                     mock_isLeafCoalesceable,
                                     mock_leafCoalesceForbidden):

        """bad has cleanup.VDI.ONBOOT_RESET so not added to list"""
        self.gather_candidates(mock_getConfig,
                               iter([cleanup.VDI.ONBOOT_RESET, False, "blah",
                                     "blah"]))

    @mock.patch("cleanup.AUTO_ONLINE_LEAF_COALESCE_ENABLED", True)
    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.VDI.isLeafCoalesceable', autospec=True,
                return_value=True)
    @mock.patch('cleanup.VDI.getConfig', autospec=True)
    def test_gather_candidates_caching_allowed(self, mock_getConfig,
                                               mock_isLeafCoalesceable,
                                               mock_leafCoalesceForbidden):

        """Bad candidate has caching allowed so not added"""
        self.gather_candidates(mock_getConfig, iter(["blah", True, "blah",
                                                     "blah"]))

    @mock.patch("cleanup.AUTO_ONLINE_LEAF_COALESCE_ENABLED", True)
    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.VDI.isLeafCoalesceable', autospec=True,
                return_value=True)
    @mock.patch('cleanup.VDI.getConfig', autospec=True)
    def test_gather_candidates_clsc_disabled(self, mock_getConfig,
                                             mock_isLeafCoalesceable,
                                             mock_leafCoalesceForbidden):
        """clsc disabled so not added"""
        self.gather_candidates(mock_getConfig,
                               iter(["blah", False,
                                     cleanup.VDI.LEAFCLSC_DISABLED,
                                     "blah"]))

    @mock.patch("cleanup.AUTO_ONLINE_LEAF_COALESCE_ENABLED", False)
    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.VDI.isLeafCoalesceable', autospec=True,
                return_value=True)
    @mock.patch('cleanup.VDI.getConfig', autospec=True)
    def test_gather_candidates_auto_coalesce_off(self, mock_getConfig,
                                                 mock_isLeafCoalesceable,
                                                 mock_leafCoalesceForbidden):
        """Globally turned off but good vdi has force"""
        self.gather_candidates(mock_getConfig,
                               iter(["blah", False, "blah", "blah"]),
                               goodConfig=iter(["blah", False, "blah",
                                                cleanup.VDI.LEAFCLSC_FORCE]))

    def makeVDIReturningSize(self, sr, size, canLiveCoalesce, liveSize):
        vdi_uuid = uuid4()
        vdi = cleanup.VDI(sr, str(vdi_uuid), False)
        vdi._calcExtraSpaceForSnapshotCoalescing =\
            mock.MagicMock(return_value=size)
        vdi.canLiveCoalesce = mock.MagicMock(return_value=canLiveCoalesce)
        vdi._calcExtraSpaceForLeafCoalescing =\
            mock.MagicMock(return_value=liveSize)
        vdi.setConfig = mock.MagicMock()
        return vdi

    def findLeafCoalesceable(self, mock_gatherLeafCoalesceable, goodSize,
                             canLiveCoalesce=False, liveSize=None,
                             expectedNothing=False):

        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))

        good = self.makeVDIReturningSize(sr, goodSize, canLiveCoalesce,
                                         liveSize)
        bad = self.makeVDIReturningSize(sr, 4096, False, 4096)

        def fakeCandidates(blah, stuff):
            stuff.append(good)
            stuff.append(bad)

        mock_gatherLeafCoalesceable.side_effect = fakeCandidates
        res = sr.findLeafCoalesceable()
        if expectedNothing:
            self.assertEqual(res, None)
        else:
            self.assertEqual(res, good)
        return good, bad

    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.SR.getFreeSpace', autospec=True, return_value=1024)
    @mock.patch('cleanup.SR.gatherLeafCoalesceable', autospec=True)
    def test_insufficient_space(self, mock_gatherLeafCoalesceable,
                                mock_getFreeSpace,
                                mock_leafCoalesceForbidden):
        """Good vdi calculates space less than remaining on sr"""
        self.findLeafCoalesceable(mock_gatherLeafCoalesceable, 4)

    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.SR.getFreeSpace', autospec=True, return_value=1024)
    @mock.patch('cleanup.SR.gatherLeafCoalesceable', autospec=True)
    def test_space_equal(self, mock_gatherLeafCoalesceable,
                         mock_getFreeSpace,
                         mock_leafCoalesceForbidden):
        """Good has calculates space equal to remaining space"""
        self.findLeafCoalesceable(mock_gatherLeafCoalesceable, 1024)

    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.SR.getFreeSpace', autospec=True, return_value=1024)
    @mock.patch('cleanup.SR.gatherLeafCoalesceable', autospec=True)
    def test_fall_back_to_leaf_coalescing(self, mock_gatherLeafCoalesceable,
                                          mock_getFreeSpace,
                                          mock_leafCoalesceForbidden):
        """Good VDI can can live coalesce and has right size"""
        self.findLeafCoalesceable(mock_gatherLeafCoalesceable, 4096,
                                  canLiveCoalesce=True,
                                  liveSize=4)

    @mock.patch('cleanup.SR.leafCoalesceForbidden', autospec=True,
                return_value=False)
    @mock.patch('cleanup.SR.getFreeSpace', autospec=True, return_value=1024)
    @mock.patch('cleanup.SR.gatherLeafCoalesceable', autospec=True)
    def test_leaf_coalescing_cannt_live_coalesce(self,
                                                 mock_gatherLeafCoalesceable,
                                                 mock_getFreeSpace,
                                                 mock_leafCoalesceForbidden):
        """1st VDI is too big for snap but right size for live
        2nd VDI is too big for snap and too big for live"""
        vdi1, vdi2 = self.findLeafCoalesceable(mock_gatherLeafCoalesceable,
                                               4097,
                                               canLiveCoalesce=False,
                                               liveSize=4,
                                               expectedNothing=True)
        vdi1.setConfig.assert_called_with(cleanup.VDI.DB_LEAFCLSC,
                                          cleanup.VDI.LEAFCLSC_OFFLINE)
        self.assertEqual(vdi2.setConfig.call_count, 0)

    def test_calcStorageSpeed(self):
        sr_uuid = uuid4()
        xapi = mock.MagicMock(autospec=True)
        sr = cleanup.SR(uuid=sr_uuid, xapi=xapi, createLock=False, force=False)
        self.assertEqual(sr.calcStorageSpeed(0, 2, 5), 2.5)
        self.assertEqual(sr.calcStorageSpeed(0.0, 2.0, 5.0), 2.5)
        self.assertEqual(sr.calcStorageSpeed(0.0, 0.0, 5.0), None)

    def test_recordStorageSpeed_bad_speed(self):
        sr_uuid = uuid4()
        xapi = mock.MagicMock(autospec=True)
        sr = cleanup.SR(uuid=sr_uuid, xapi=xapi, createLock=False, force=False)
        sr.writeSpeedToFile = mock.MagicMock(autospec=True)
        sr.recordStorageSpeed(0, 0, 0)
        self.assertEqual(sr.writeSpeedToFile.call_count, 0)

    def test_recordStorageSpeed_good_speed(self):
        sr_uuid = uuid4()
        xapi = mock.MagicMock(autospec=True)
        sr = cleanup.SR(uuid=sr_uuid, xapi=xapi, createLock=False, force=False)
        sr.writeSpeedToFile = mock.MagicMock(autospec=True)
        sr.recordStorageSpeed(1, 6, 9)
        self.assertEqual(sr.writeSpeedToFile.call_count, 1)
        sr.writeSpeedToFile.assert_called_with(1.8)

    def makeFakeFile(self):
        FakeFile.writelines = mock.MagicMock()
        FakeFile.write = mock.MagicMock()
        FakeFile.readlines = mock.MagicMock()
        FakeFile.close = mock.MagicMock()
        FakeFile.seek = mock.MagicMock()
        return FakeFile

    def getStorageSpeed(self, mock_isFile, sr, fakeFile, isFile, expectedRes,
                        closeCount, lines=None):
        fakeFile.close.call_count = 0
        mock_isFile.return_value = isFile
        if lines:
            FakeFile.readlines.return_value = lines
        res = sr.getStorageSpeed()
        self.assertEqual(res, expectedRes)

        self.assertEqual(fakeFile.close.call_count, closeCount)

    @mock.patch("__builtin__.open", autospec=True)
    @mock.patch("os.path.isfile", autospec=True)
    @mock.patch("os.chmod", autospec=True)
    def test_getStorageSpeed(self, mock_chmod, mock_isFile, mock_open):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        fakeFile = self.makeFakeFile()
        mock_open.return_value = FakeFile

        # File does not exist
        self.getStorageSpeed(mock_isFile, sr, fakeFile, False, None, 0)

        # File exists but empty (should be impossible)
        self.getStorageSpeed(mock_isFile, sr, fakeFile, True, None, 1,
                             lines=[])

        # File exists one value
        self.getStorageSpeed(mock_isFile, sr, fakeFile, True, 2.0, 1,
                             lines=[2.0])

        # File exists 3 values
        self.getStorageSpeed(mock_isFile, sr, fakeFile, True, 3.0, 1,
                             lines=[1.0, 2.0, 6.0])

        # File exists contains, a string
        self.getStorageSpeed(mock_isFile, sr, fakeFile, True, None, 1,
                             lines=[1.0, 2.0, "Hello"])

    def speedFileSetup(self, sr, FakeFile, mock_isFile, isFile):
        expectedPath = cleanup.SPEED_LOG_ROOT.format(uuid=sr.uuid)
        mock_isFile.return_value = isFile
        FakeFile.writelines.reset_mock()
        FakeFile.write.reset_mock()
        FakeFile.readlines.reset_mock()
        FakeFile.close.reset_mock()
        FakeFile.seek.reset_mock()
        return expectedPath

    def writeSpeedFile(self, sr, speed, mock_isFile, isFile, mock_open,
                       mock_chmod, write=None, writeLines=None, readLines=None,
                       openOp="r+"):
        mock_open.reset_mock()
        mock_chmod.reset_mock()
        expectedPath = self.speedFileSetup(sr, FakeFile, mock_isFile, isFile)
        FakeFile.readlines.return_value = readLines
        sr.writeSpeedToFile(speed)
        mock_open.assert_called_with(expectedPath, openOp)
        if openOp == "w":
            mock_chmod.assert_called_with(expectedPath, stat.S_IRWXU)
        if write:
            FakeFile.write.assert_called_with(write)
        if writeLines:
            FakeFile.seek.assert_called_with(0)
            FakeFile.writelines.assert_called_with(writeLines)
        self.assertEqual(FakeFile.close.call_count, 1)

    @mock.patch("__builtin__.open",
                autospec=True)
    @mock.patch("os.path.isfile", autospec=True)
    @mock.patch("os.chmod", autospec=True)
    def test_writeSpeedToFile(self, mock_chmod, mock_isFile, mock_open):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        FakeFile = self.makeFakeFile()
        mock_open.return_value = FakeFile

        # File does not exist
        self.writeSpeedFile(sr, 1.8, mock_isFile, False, mock_open, mock_chmod,
                            write="1.8\n", openOp="w")

        # File does exist but empty (Should not happen)
        readLines = []
        writeLines = ["1.8\n"]
        self.writeSpeedFile(sr, 1.8, mock_isFile, True, mock_open, mock_chmod,
                            readLines=readLines, writeLines=writeLines)

        # File does exist, exception fired, make sure close fd.
        expectedPath = self.speedFileSetup(sr, FakeFile, mock_isFile, True)
        FakeFile.readlines.side_effect = Exception
        with self.assertRaises(Exception):
            sr.writeSpeedToFile(1.8)
        mock_open.assert_called_with(expectedPath, 'r+')
        self.assertEqual(FakeFile.close.call_count, 1)
        FakeFile.readlines.side_effect = None

        # File does exist
        readLines = ["1.9\n", "2.1\n", "3\n"]
        writeLines = ["1.9\n", "2.1\n", "3\n", "1.8\n"]
        self.writeSpeedFile(sr, 1.8, mock_isFile, True, mock_open, mock_chmod,
                            readLines=readLines, writeLines=writeLines)

        # File does exist and almost full
        readLines = ["2.0\n",
                     "2.1\n",
                     "2.2\n",
                     "2.3\n",
                     "2.4\n",
                     "2.5\n",
                     "2.6\n",
                     "2.7\n",
                     "2.8\n"]

        writeLines = ["2.0\n",
                      "2.1\n",
                      "2.2\n",
                      "2.3\n",
                      "2.4\n",
                      "2.5\n",
                      "2.6\n",
                      "2.7\n",
                      "2.8\n",
                      "1.8\n"]

        self.writeSpeedFile(sr, 1.8, mock_isFile, True, mock_open, mock_chmod,
                            readLines=readLines, writeLines=writeLines)

        # File does exist and full
        readLines = ["2.0\n",
                     "1.9\n",
                     "1.9\n",
                     "1.9\n",
                     "1.9\n",
                     "1.9\n",
                     "1.9\n",
                     "1.9\n",
                     "1.9\n",
                     "1.9\n",
                     "1.9\n"]

        writeLines = ["1.9\n",
                      "1.9\n",
                      "1.9\n",
                      "1.9\n",
                      "1.9\n",
                      "1.9\n",
                      "1.9\n",
                      "1.9\n",
                      "1.9\n",
                      "1.9\n",
                      "1.8\n"]

        self.writeSpeedFile(sr, 1.8, mock_isFile, True, mock_open, mock_chmod,
                            readLines=readLines, writeLines=writeLines)

    def canLiveCoalesce(self, vdi, size, config, speed, expectedRes):
        vdi.getSizeVHD = mock.MagicMock(return_value=size)
        vdi.getConfig = mock.MagicMock(return_value=config)
        res = vdi.canLiveCoalesce(speed)
        self.assertEqual(res, expectedRes)

    def test_canLiveCoalesce(self):
        sr_uuid = uuid4()
        sr = create_cleanup_sr(uuid=str(sr_uuid))
        vdi_uuid = uuid4()
        vdi = cleanup.VDI(sr, str(vdi_uuid), False)
        # Fast enough to for size 10/10 = 1 second and not forcing
        self.canLiveCoalesce(vdi, 10, "blah", 10, True)

        # To slow 10/0.1 = 100 seconds and not forcing
        self.canLiveCoalesce(vdi, 10, "blah", 0.1, False)

        # Fast enough to for size 10/10 = 1 second and forcing
        self.canLiveCoalesce(vdi, 10, cleanup.VDI.LEAFCLSC_FORCE, 10, True)

        # To slow 10/0.1 = 100 seconds and forcing
        self.canLiveCoalesce(vdi, 10, cleanup.VDI.LEAFCLSC_FORCE, 0.1, True)

        # Fallback to hardcoded data size, too big
        self.canLiveCoalesce(vdi, cleanup.VDI.LIVE_LEAF_COALESCE_MAX_SIZE+1,
                             "blah", None, False)

        # Fallback to hardcoded data size, too big but force
        self.canLiveCoalesce(vdi, cleanup.VDI.LIVE_LEAF_COALESCE_MAX_SIZE+1,
                             cleanup.VDI.LEAFCLSC_FORCE, None, True)

        # Fallback to hardcoded data size, acceptable size.
        self.canLiveCoalesce(vdi, 10, "blah", None, True)

        # Fallback to hardcoded data size, acceptable size, force also.
        self.canLiveCoalesce(vdi, 10, cleanup.VDI.LEAFCLSC_FORCE, None, True)

    def test_getSwitch(self):
        sr_uuid = uuid4()
        xapi = mock.MagicMock(autospec=True)
        sr = cleanup.SR(uuid=sr_uuid, xapi=xapi, createLock=False, force=False)
        xapi.srRecord = {"other_config": {"test1": "test1", "test2": "test2"}}
        self.assertEqual(sr.getSwitch("test1"), "test1")
        self.assertEqual(sr.getSwitch("test2"), "test2")
        self.assertEqual(sr.getSwitch("test3"), None)

    def forbiddenBySwitch(self, sr, mock_log, switch, switchValue, failMessage,
                          expectedRes):
        mock_log.reset_mock()
        res = sr.forbiddenBySwitch(switch,  switchValue, failMessage)
        self.assertEqual(res, expectedRes)
        sr.getSwitch.assert_called_with(switch)
        if failMessage:
            mock_log.assert_called_with(failMessage)
        else:
            self.assertEqual(mock_log.call_count, 0)

    @mock.patch('cleanup.Util.log')
    @mock.patch('cleanup.SR.getSwitch')
    def test_forbiddenBySwitch(self, mock_getSwitch, mock_log):
        sr_uuid = uuid4()
        switch = "blah"
        switchValue = "test"
        failMessage = "This is a test"

        mock_getSwitch.return_value = switchValue
        xapi = mock.MagicMock(autospec=True)
        sr = cleanup.SR(uuid=sr_uuid, xapi=xapi, createLock=False, force=False)

        self.forbiddenBySwitch(sr, mock_log, switch, switchValue, failMessage,
                               True)

        self.forbiddenBySwitch(sr, mock_log, switch, "notForbidden", None,
                               False)

        mock_getSwitch.return_value = None
        self.forbiddenBySwitch(sr, mock_log, switch, "notForbidden", None,
                               False)

    def leafCoalesceForbidden(self, sr, mock_srforbiddenBySwitch, side_effect,
                              expectedRes,  expected_callCount, *argv):
        mock_srforbiddenBySwitch.call_count = 0
        mock_srforbiddenBySwitch.side_effect = side_effect
        res = sr.leafCoalesceForbidden()
        self.assertEqual(res, expectedRes)
        sr.forbiddenBySwitch.assert_called_with(*argv)
        self.assertEqual(expected_callCount, sr.forbiddenBySwitch.call_count)

    @mock.patch('cleanup.SR.forbiddenBySwitch', autospec=True)
    def test_leafCoalesceForbidden(self, mock_srforbiddenBySwitch):
        sr_uuid = uuid4()
        switch = "blah"
        switchValue = "test"
        failMessage = "This is a test"

        sr = create_cleanup_sr()

        side_effect = iter([True, True])
        self.leafCoalesceForbidden(sr, mock_srforbiddenBySwitch, side_effect,
                                   True, 1, sr, cleanup.VDI.DB_COALESCE,
                                   "false",
                                   "Coalesce disabled "
                                   "for this SR")
        side_effect = iter([True, False])
        self.leafCoalesceForbidden(sr, mock_srforbiddenBySwitch, side_effect,
                                   True, 1, sr, cleanup.VDI.DB_COALESCE,
                                   "false",
                                   "Coalesce disabled "
                                   "for this SR")

        side_effect = iter([False, False])
        self.leafCoalesceForbidden(sr, mock_srforbiddenBySwitch,
                                   side_effect, False, 2, sr,
                                   cleanup.VDI.DB_LEAFCLSC,
                                   cleanup.VDI.LEAFCLSC_DISABLED,
                                   "Leaf-coalesce disabled"
                                   " for this SR")

        side_effect = iter([False, True])
        self.leafCoalesceForbidden(sr, mock_srforbiddenBySwitch, side_effect,
                                   True, 2, sr,
                                   cleanup.VDI.DB_LEAFCLSC,
                                   cleanup.VDI.LEAFCLSC_DISABLED,
                                   "Leaf-coalesce disabled"
                                   " for this SR")

    def trackerReportOk(self, tracker, expectedHistory, expectedReason,
                        start, finish, minimum):
        _before = cleanup.Util
        cleanup.Util = FakeUtil
        tracker.printReasoning()
        pos = 0
        self.assertEqual(FakeUtil.record[0], "Aborted coalesce")
        pos += 1

        for hist in expectedHistory:
            self.assertEqual(FakeUtil.record[pos], hist)
            pos += 1

        self.assertEqual(FakeUtil.record[pos], expectedReason)
        pos += 1
        self.assertEqual(FakeUtil.record[pos],
                         "Starting size was"
                         "         {size}".format(size=start))
        pos += 1
        self.assertEqual(FakeUtil.record[pos],
                         "Final size was"
                         "            {size}".format(size=finish))
        pos += 1
        self.assertEqual(FakeUtil.record[pos],
                         "Minimum size acheived"
                         " was {size}".format(size=minimum))
        FakeUtil.record = []
        cleanup.Util = _before

    def autopsyTracker(self, tracker, finalRes, expectedHistory,
                       expectedReason, start, finish, minimum):

        self.assertTrue(finalRes)
        self.assertEqual(expectedHistory, tracker.history)
        self.assertEqual(expectedReason, tracker.reason)
        self.trackerReportOk(tracker, expectedHistory,
                             expectedReason, start, finish, minimum)

    def exerciseTracker(self, size1, size2, its,  expectedHistory,
                        expectedReason, start, finish, minimum):
        tracker = cleanup.SR.CoalesceTracker()
        for x in range(its):
            res = tracker.abortCoalesce(size1, size2)
            self.assertFalse(res)
        res = tracker.abortCoalesce(size1, size2)
        self.autopsyTracker(tracker, res, expectedHistory, expectedReason,
                            start, finish, minimum)

    def test_leafCoalesceTracker(self):
        # Test initialization
        tracker = cleanup.SR.CoalesceTracker()
        self.assertEqual(tracker.itsNoProgress, 0)
        self.assertEqual(tracker.its, 0)
        self.assertEqual(tracker.minSize, float("inf"))
        self.assertEqual(tracker.history, [])
        self.assertEqual(tracker.reason, "")
        self.assertEqual(tracker.startSize, None)
        self.assertEqual(tracker.finishSize, None)

        # 10 iterations no progress 11th fails.
        expectedHistory = [
            "Iteration: 1 -- Initial size 10 --> Final size 10",
            "Iteration: 2 -- Initial size 10 --> Final size 10",
            "Iteration: 3 -- Initial size 10 --> Final size 10",
            "Iteration: 4 -- Initial size 10 --> Final size 10",
            "Iteration: 5 -- Initial size 10 --> Final size 10",
            "Iteration: 6 -- Initial size 10 --> Final size 10",
            "Iteration: 7 -- Initial size 10 --> Final size 10",
            "Iteration: 8 -- Initial size 10 --> Final size 10",
            "Iteration: 9 -- Initial size 10 --> Final size 10",
            "Iteration: 10 -- Initial size 10 --> Final size 10",
            "Iteration: 11 -- Initial size 10 --> Final size 10"
        ]
        expectedReason = "Max iterations (10) exceeded"
        self.exerciseTracker(10, 10, 10, expectedHistory,
                             expectedReason, 10, 10, 10)

        # 3 iterations getting bigger, then fail
        expectedHistory = [
            "Iteration: 1 -- Initial size 100 --> Final size 101",
            "Iteration: 2 -- Initial size 100 --> Final size 101",
            "Iteration: 3 -- Initial size 100 --> Final size 101",
            "Iteration: 4 -- Initial size 100 --> Final size 101"
        ]
        expectedReason = "No progress made for 3 iterations"
        self.exerciseTracker(100, 101, 3, expectedHistory,
                             expectedReason, 100, 101, 100)

        # Increase beyond maximum allowed growth
        expectedHistory = [
            "Iteration: 1 -- Initial size 100 --> Final size 100",
            "Iteration: 2 -- Initial size 100 --> Final size 121",
        ]
        expectedReason = "Unexpected bump in size,"\
                         " compared to minimum acheived"
        res = tracker.abortCoalesce(100, 100)
        self.assertFalse(res)
        res = tracker.abortCoalesce(100, 121)
        self.autopsyTracker(tracker, res, expectedHistory,
                            expectedReason, 100, 121, 100)
