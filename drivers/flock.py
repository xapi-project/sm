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
#

"""
Fcntl-based Advisory Locking with a proper .trylock()

Python's fcntl module is not good at locking. In particular, proper
testing and trying of locks isn't well supported. Looks as if we've
got to grow our own.
"""

import os, fcntl, struct
import errno

class Flock:
    """A C flock struct."""

    def __init__(self, l_type, l_whence=0, l_start=0, l_len=0, l_pid=0):
        """See fcntl(2) for field details."""
        self.fields = [l_type, l_whence, l_start, l_len, l_pid]

    FORMAT = "hhqql"
    # struct flock(2) format, tested with python2.4/i686 and
    # python2.5/x86_64. http://docs.python.org/lib/posix-large-files.html
        
    def fcntl(self, fd, cmd):
        """Issues a system fcntl(fd, cmd, self). Updates self with what was
        returned by the kernel. Otherwise raises IOError(errno)."""

        st = struct.pack(self.FORMAT, *self.fields)
        st = fcntl.fcntl(fd, cmd, st)

        fields = struct.unpack(self.FORMAT, st)
        self.__init__(*fields)

    FIELDS = { 'l_type':       0,
               'l_whence':     1,
               'l_start':      2,
               'l_len':        3,
               'l_pid':        4 }

    def __getattr__(self, name):
        idx = self.FIELDS[name]
        return self.fields[idx]

    def __setattr__(self, name, value):
        idx = self.FIELDS.get(name)
        if idx is None:
            self.__dict__[name] = value
        else:
            self.fields[idx] = value

class FcntlLockBase:
    """Abstract base class for either reader or writer locks. A respective
    definition of LOCK_TYPE (fcntl.{F_RDLCK|F_WRLCK}) determines the
    type."""

    LOCK_TYPE = None
    
    if __debug__:
        ERROR_ISLOCKED = "Attempt to acquire lock held."
        ERROR_NOTLOCKED = "Attempt to unlock lock not held."

    def __init__(self, fd):
        """Creates a new, unheld lock."""
        self.fd = fd
        #
        # Subtle: fcntl(2) permits re-locking it as often as you want
        # once you hold it. This is slightly counterintuitive and we
        # want clean code, so we add one bit of our own bookkeeping.
        #
        self._held = False

    def lock(self):
        """Blocking lock aquisition."""
        assert not self._held, self.ERROR_ISLOCKED
        Flock(self.LOCK_TYPE).fcntl(self.fd, fcntl.F_SETLKW)
        self._held = True

    def trylock(self):
        """Non-blocking lock aquisition. Returns True on success, False
        otherwise."""
        if self._held: return False
        try:
            Flock(self.LOCK_TYPE).fcntl(self.fd, fcntl.F_SETLK)
        except IOError, e:
            if e.errno in [errno.EACCES, errno.EAGAIN]:
                return False
            raise
        self._held = True
        return True

    def held(self):
        """Returns True if @self holds the lock, False otherwise."""
        return self._held

    def unlock(self):
        """Release a previously acquired lock."""
        Flock(fcntl.F_UNLCK).fcntl(self.fd, fcntl.F_SETLK)
        self._held = False

    def test(self):
        """Returns the PID of the process holding the lock or -1 if the lock
        is not held."""
        if self._held: return os.getpid()
        flock = Flock(self.LOCK_TYPE)
        flock.fcntl(self.fd, fcntl.F_GETLK)
        if flock.l_type == fcntl.F_UNLCK:
            return -1
        return flock.l_pid


class WriteLock(FcntlLockBase):
    """A simple global writer (i.e. exclusive) lock."""
    LOCK_TYPE = fcntl.F_WRLCK

class ReadLock(FcntlLockBase):
    """A simple global reader (i.e. shared) lock."""
    LOCK_TYPE = fcntl.F_RDLCK


