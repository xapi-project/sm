import fcntl
import unittest
import unittest.mock as mock
import os
import errno
import struct

import testlib

from sm.core import flock
from sm.core import lock


class TestLock(unittest.TestCase):

    def tearDown(self):
        lock.Lock.INSTANCES = {}
        lock.Lock.BASE_INSTANCES = {}

    @testlib.with_context
    def test_lock_without_namespace_creates_nil_namespace(self, context):
        lck = lock.Lock('somename')

        self.assertTrue(
            os.path.exists(
                os.path.join(lock.Lock.BASE_DIR, '.nil')))

    @testlib.with_context
    def test_lock_with_namespace_creates_namespace(self, context):
        lck = lock.Lock('somename', ns='namespace')

        self.assertTrue(
            os.path.exists(
                os.path.join(lock.Lock.BASE_DIR, 'namespace')))

    @testlib.with_context
    def test_lock_without_namespace_creates_file(self, context):
        lck = lock.Lock('somename')

        self.assertTrue(
            os.path.exists(
                os.path.join(lock.Lock.BASE_DIR, '.nil', 'somename')))

    @testlib.with_context
    def test_lock_with_namespace_creates_file(self, context):
        lck = lock.Lock('somename', ns='namespace')

        self.assertTrue(
            os.path.exists(
                os.path.join(lock.Lock.BASE_DIR, 'namespace', 'somename')))

    @testlib.with_context
    def test_lock_file_create_fails_retried(self, context):
        Lock = create_lock_class_that_fails_to_create_file(1)
        lck = Lock('somename', ns='namespace')

        self.assertTrue(
            os.path.exists(
                os.path.join(lock.Lock.BASE_DIR, 'namespace', 'somename')))

    def setup_fcntl_return(self, context):
        context.mock_fcntl = mock.MagicMock(spec=fcntl.fcntl)
        context.mock_fcntl.return_value = struct.pack(
            flock.Flock.FORMAT, fcntl.F_WRLCK, 0, 0, 0, 0)

    @testlib.with_context
    def test_lock_acquire_release(self, context):
        self.setup_fcntl_return(context)

        lck = lock.Lock("somename")

        lck.acquire()

        self.assertTrue(lck.held())

        lck.release()

        self.assertFalse(lck.held())

    @testlib.with_context
    def test_lock_acquire_noblock_release(self, context):
        self.setup_fcntl_return(context)

        lck = lock.Lock("somename")

        lck.acquireNoblock()

        self.assertTrue(lck.held())

        lck.release()

        self.assertFalse(lck.held())

    @testlib.with_context
    def test_lock_acquire_twice_release(self, context):
        self.setup_fcntl_return(context)

        lck1 = lock.Lock("somename")

        lck1.acquire()

        self.assertTrue(lck1.held())

        # This should be the same lock as lck1
        lck2 = lock.Lock("somename")
        lck2.acquire()

        self.assertTrue(lck2.held())
        lck2.release()

        # As lck1 and lck2 refer to the same lock they should still be held
        self.assertTrue(lck2.held())
        self.assertTrue(lck1.held())

        lck1.release()

        self.assertFalse(lck1.held())

    @testlib.with_context
    def test_lock_acquire_noblock_twice_release(self, context):
        self.setup_fcntl_return(context)

        lck1 = lock.Lock("somename")

        lck1.acquireNoblock()

        self.assertTrue(lck1.held())

        # This should be the same lock as lck1
        lck2 = lock.Lock("somename")
        lck2.acquireNoblock()

        self.assertTrue(lck2.held())
        lck2.release()

        # As lck1 and lck2 refer to the same lock they should still be held
        self.assertTrue(lck2.held())
        self.assertTrue(lck1.held())

        lck1.release()

        self.assertFalse(lck1.held())

    @testlib.with_context
    def test_lock_acquire_then_noblock_release(self, context):
        self.setup_fcntl_return(context)

        lck1 = lock.Lock("somename")

        lck1.acquire()

        self.assertTrue(lck1.held())

        # This should be the same lock as lck1
        lck2 = lock.Lock("somename")
        lck2.acquireNoblock()

        self.assertTrue(lck2.held())
        lck2.release()

        # As lck1 and lck2 refer to the same lock they should still be held
        self.assertTrue(lck2.held())
        self.assertTrue(lck1.held())

        lck1.release()

        self.assertFalse(lck1.held())

    @testlib.with_context
    def test_lock_noblock_then_acquire_release(self, context):
        self.setup_fcntl_return(context)

        lck1 = lock.Lock("somename")

        lck1.acquireNoblock()

        self.assertTrue(lck1.held())

        # This should be the same lock as lck1
        lck2 = lock.Lock("somename")
        lck2.acquire()

        self.assertTrue(lck2.held())
        lck2.release()

        # As lck1 and lck2 refer to the same lock they should still be held
        self.assertTrue(lck2.held())
        self.assertTrue(lck1.held())

        lck1.release()

        self.assertFalse(lck1.held())


def create_lock_class_that_fails_to_create_file(number_of_failures):

    class LockThatFailsToCreateFile(lock.LockImplementation):
        _failures = number_of_failures

        def _open_lockfile(self):
            if self._failures > 0:
                error = IOError('No such file')
                error.errno = errno.ENOENT
                self._failures -= 1
                raise error
            return super(LockThatFailsToCreateFile, self)._open_lockfile()

    return LockThatFailsToCreateFile
