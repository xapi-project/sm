#
# Copyright (C) Citrix Systems Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation; version 2.1 only.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

"""Serialization for concurrent operations"""

import os
import errno
from sm.core import flock
from sm.core import util

VERBOSE = True

# Still just called "running" for backwards compatibility
LOCK_TYPE_GC_RUNNING = "running"
LOCK_TYPE_ISCSIADM_RUNNING = "isciadm_running"

class LockException(util.SMException):
    pass


class Lock(object):
    """Simple file-based lock on a local FS. With shared reader/writer
    attributes."""

    BASE_DIR = "/var/lock/sm"

    INSTANCES = {}
    BASE_INSTANCES = {}

    def __new__(cls, name, ns=None, *args, **kwargs):
        if ns:
            if ns not in Lock.INSTANCES:
                Lock.INSTANCES[ns] = {}
            instances = Lock.INSTANCES[ns]
        else:
            instances = Lock.BASE_INSTANCES

        if name not in instances:
            instances[name] = LockImplementation(name, ns)
        return instances[name]

    def acquire(self):
        raise NotImplementedError("Lock methods implemented in LockImplementation")

    def acquireNoblock(self):
        raise NotImplementedError("Lock methods implemented in LockImplementation")

    def release(self):
        raise NotImplementedError("Lock methods implemented in LockImplementation")

    def held(self):
        raise NotImplementedError("Lock methods implemented in LockImplementation")

    def _mknamespace(ns):

        if ns is None:
            return ".nil"

        assert not ns.startswith(".")
        assert ns.find(os.path.sep) < 0
        return ns
    _mknamespace = staticmethod(_mknamespace)

    @staticmethod
    def clearAll():
        """
        Drop all lock instances, to be used when forking, but not execing
        """
        Lock.INSTANCES = {}
        Lock.BASE_INSTANCES = {}

    def cleanup(name, ns=None):
        if ns:
            if ns in Lock.INSTANCES:
                if name in Lock.INSTANCES[ns]:
                    del Lock.INSTANCES[ns][name]
                if len(Lock.INSTANCES[ns]) == 0:
                    del Lock.INSTANCES[ns]
        elif name in Lock.BASE_INSTANCES:
            del Lock.BASE_INSTANCES[name]

        ns = Lock._mknamespace(ns)
        path = os.path.join(Lock.BASE_DIR, ns, name)
        if os.path.exists(path):
            Lock._unlink(path)

    cleanup = staticmethod(cleanup)

    def cleanupAll(ns=None):
        ns = Lock._mknamespace(ns)
        nspath = os.path.join(Lock.BASE_DIR, ns)

        if not os.path.exists(nspath):
            return

        for file in os.listdir(nspath):
            path = os.path.join(nspath, file)
            Lock._unlink(path)

        Lock._rmdir(nspath)

    cleanupAll = staticmethod(cleanupAll)
    #
    # Lock and attribute file management
    #

    def _mkdirs(path):
        """Concurrent makedirs() catching EEXIST."""
        if os.path.exists(path):
            return
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise LockException("Failed to makedirs(%s)" % path)
    _mkdirs = staticmethod(_mkdirs)

    def _unlink(path):
        """Non-raising unlink()."""
        util.SMlog("lock: unlinking lock file %s" % path)
        try:
            os.unlink(path)
        except Exception as e:
            util.SMlog("Failed to unlink(%s): %s" % (path, e))
    _unlink = staticmethod(_unlink)

    def _rmdir(path):
        """Non-raising rmdir()."""
        util.SMlog("lock: removing lock dir %s" % path)
        try:
            os.rmdir(path)
        except Exception as e:
            util.SMlog("Failed to rmdir(%s): %s" % (path, e))
    _rmdir = staticmethod(_rmdir)


class LockImplementation(object):

    def __init__(self, name, ns=None):
        self.lockfile = None

        self.ns = Lock._mknamespace(ns)

        assert not name.startswith(".")
        assert name.find(os.path.sep) < 0
        self.name = name

        self.count = 0

        self._open()

    def _open(self):
        """Create and open the lockable attribute base, if it doesn't exist.
        (But don't lock it yet.)"""

        # one directory per namespace
        self.nspath = os.path.join(Lock.BASE_DIR, self.ns)

        # the lockfile inside that namespace directory per namespace
        self.lockpath = os.path.join(self.nspath, self.name)

        number_of_enoent_retries = 10

        while True:
            Lock._mkdirs(self.nspath)

            try:
                self._open_lockfile()
            except IOError as e:
                # If another lock within the namespace has already
                # cleaned up the namespace by removing the directory,
                # _open_lockfile raises an ENOENT, in this case we retry.
                if e.errno == errno.ENOENT:
                    if number_of_enoent_retries > 0:
                        number_of_enoent_retries -= 1
                        continue
                raise
            break

        fd = self.lockfile.fileno()
        self.lock = flock.WriteLock(fd)

    def _open_lockfile(self):
        """Provide a seam, so extreme situations could be tested"""
        util.SMlog("lock: opening lock file %s" % self.lockpath)
        self.lockfile = open(self.lockpath, "w+")

    def _close(self):
        """Close the lock, which implies releasing the lock."""
        if self.lockfile is not None:
            if self.held():
                # drop all reference counts
                self.count = 0
                self.release()
            self.lockfile.close()
            util.SMlog("lock: closed %s" % self.lockpath)
            self.lockfile = None

    __del__ = _close

    def cleanup(self, name, ns=None):
        Lock.cleanup(name, ns)

    def cleanupAll(self, ns=None):
        Lock.cleanupAll(ns)
    #
    # Actual Locking
    #

    def acquire(self):
        """Blocking lock aquisition, with warnings. We don't expect to lock a
        lot. If so, not to collide. Coarse log statements should be ok
        and aid debugging."""
        if not self.held():
            if not self.lock.trylock():
                util.SMlog("Failed to lock %s on first attempt, " % self.lockpath
                       + "blocked by PID %d" % self.lock.test())
                self.lock.lock()
            if VERBOSE:
                util.SMlog("lock: acquired %s" % self.lockpath)
        self.count += 1

    def acquireNoblock(self):
        """Acquire lock if possible, or return false if lock already held"""
        if not self.held():
            exists = os.path.exists(self.lockpath)
            ret = self.lock.trylock()
            if VERBOSE:
                util.SMlog("lock: tried lock %s, acquired: %s (exists: %s)" % \
                        (self.lockpath, ret, exists))
        else:
            ret = True

        if ret:
            self.count += 1

        return ret

    def held(self):
        """True if @self acquired the lock, False otherwise."""
        return self.lock.held()

    def release(self):
        """Release a previously acquired lock."""
        if self.count >= 1:
            self.count -= 1

        if self.count > 0:
            return

        self.lock.unlock()
        if VERBOSE:
            util.SMlog("lock: released %s" % self.lockpath)
