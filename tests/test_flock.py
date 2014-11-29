import unittest
import tempfile
import shutil
import os
import subprocess
import sys

import flock


class TestWriteLock(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.lock_path = os.path.join(self.temp_dir, 'lockfile')
        self.cleanup_hooks = []

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        for cleanup_hook in self.cleanup_hooks:
            cleanup_hook()

    def get_write_lock(self):
        temp_file = open(self.lock_path, 'w+')
        self.workaround_to_hold_file_object = temp_file
        file_descriptor = temp_file.fileno()

        return flock.WriteLock(file_descriptor)

    def test_test_new_lock_is_not_held_by_anyone(self):
        write_lock = self.get_write_lock()

        self.assertEquals(-1, write_lock.test())

    def test_test_when_locked_returns_pid(self):
        write_lock = self.get_write_lock()
        write_lock.lock()

        self.assertEquals(os.getpid(), write_lock.test())

    def test_trylock_when_called_locks_lock(self):
        write_lock = self.get_write_lock()

        write_lock.trylock()

        self.assertEquals(os.getpid(), write_lock.test())

    def test_trylock_when_lockable_returns_true(self):
        write_lock = self.get_write_lock()

        result = write_lock.trylock()

        self.assertEquals(True, result)

    def test_unlock_with_new_lock_is_safe_to_call(self):
        write_lock = self.get_write_lock()

        write_lock.unlock()

        self.assertEquals(-1, write_lock.test())

    def lock_with_subprocess(self):
        subproc = hold_lock_with_subprocess(self.lock_path)
        self.cleanup_hooks.append(lambda: cleanup_subprocess(subproc))
        return subproc

    def test_trylock_when_locked_by_other_process_then_fails(self):
        write_lock = self.get_write_lock()

        self.lock_with_subprocess()

        result = write_lock.trylock()

        self.assertEquals(False, result)

    def test_test_when_locked_by_other_process_then_reports_pid(self):
        write_lock = self.get_write_lock()

        subproc = self.lock_with_subprocess()

        result = write_lock.test()

        self.assertEquals(subproc.pid, result)

    def test_unlock_when_locked_by_other_process_does_not_unlock(self):
        write_lock = self.get_write_lock()

        subproc = self.lock_with_subprocess()

        write_lock.unlock()

        result = write_lock.test()
        self.assertEquals(subproc.pid, result)

    def test_trylock_when_already_locked_returns_false(self):
        write_lock = self.get_write_lock()
        write_lock.lock()

        result = write_lock.trylock()

        self.assertEquals(False, result)

    def test_lock_when_already_locked_raises_exception(self):
        write_lock = self.get_write_lock()
        write_lock.lock()

        self.assertRaises(flock.AlreadyLocked, write_lock.lock)


def cleanup_subprocess(subproc):
    subproc.stdin.write('OK')
    subproc.wait()
    assert subproc.returncode == 5


def hold_lock_with_subprocess(lock_path):
    fname = __file__

    subproc = subprocess.Popen(
        [sys.executable, fname, lock_path],
        stdout=subprocess.PIPE, stdin=subprocess.PIPE)

    assert 'SYN' == subproc.stdout.read(3)

    return subproc


def _lock_subprocess_impl(lock_path):
    temp_file = open(lock_path, 'w+')
    file_descriptor = temp_file.fileno()

    lock = flock.WriteLock(file_descriptor)

    lock.lock()

    sys.stdout.write('SYN')
    sys.stdout.flush()

    result = sys.stdin.read(2)
    if 'OK' == result:
        sys.exit(5)


if __name__ == "__main__":
    lock_path = sys.argv[1]

    _lock_subprocess_impl(lock_path)
