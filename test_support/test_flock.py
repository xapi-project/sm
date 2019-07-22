#!/usr/bin/python
from flock import WriteLock, ReadLock

#
# Test/Example
#
import os
import sys

def test_interface():

    lockfile = file("/tmp/lockfile", "w+")

    # Create a WriteLock
    fd = lockfile.fileno()
    lock = WriteLock(fd)

    # It's not yet held.
    assert lock.test() == -1
    assert lock.held() == False

    #
    # Let a child aquire it
    #

    (pin, cout) = os.pipe()
    (cin, pout) = os.pipe()

    pid = os.fork()
    if pid == 0:
        os.close(pin)
        os.close(pout)

        lock.lock()

        # Synchronize
        os.write(cout, "SYN")

        # Wait for parent
        assert os.read(cin, 3) == "ACK", "Faulty parent"

        sys.exit(0)

    os.close(cout)
    os.close(cin)

    # Wait for child
    assert os.read(pin, 3) == "SYN", "Faulty child"

    # Lock should be held by child
    assert lock.test() == pid
    assert lock.trylock() == False
    assert lock.held() == False

    # Synchronize child
    os.write(pout, "ACK")

    # Lock requires our uncooperative child to terminate.
    lock.lock()

    # We got the lock, so child should have exited, right?
    #assert os.waitpid(pid, os.WNOHANG) == (pid, 0)
    #
    # Won't work but race, because the runtime will explicitly
    # lockfile.close() before the real exit(2). See
    # FcntlLockBase.__del__() above.

    # Attempt to re-lock should throw
    try:
        lock.lock()
    except AssertionError, e:
        if str(e) != WriteLock.ERROR_ISLOCKED:
            raise
    else:
        raise AssertionError("Held locks should not be lockable.")

    # We got the lock..
    assert lock.held() == True
    # .. so trylock should also know.
    assert lock.trylock() == False

    # Fcntl won't do this, but we do. Users should be able to avoid
    # relying on it.
    assert lock.test() == os.getpid()

    # Release the lock.
    lock.unlock()

    # Attempt to re-unlock should throw.
    try:
        lock.unlock()
    except AssertionError, e:
        if str(e) != WriteLock.ERROR_NOTLOCKED:
            raise
    else:
        raise AssertionError("Unlocked locks should not unlock.")

def test_rwlocking():

    lockfile = file("/tmp/lockfile", "w+")

    fd = lockfile.fileno()

    rdlock = ReadLock(fd)
    assert rdlock.test() == None

    wrlock = WriteLock(fd)
    assert wrlock.test() == None

    rdlock.lock()
    # Same story: need to fork to get this going
    assert wrlock.test() == None
    rdlock.unlock()

    #
    # Let a child aquire it
    #

    (pin, cout) = os.pipe()
    (cin, pout) = os.pipe()

    pid = os.fork()
    if pid == 0:
        os.close(pin)
        os.close(pout)

        # Synchronize parent
        os.write(cout, "SYN")

        wrlock.lock()
        assert os.read(cin, 3) == "SYN", "Faulty parent"

        # Wait for parent
        assert os.read(cin, 3) == "ACK", "Faulty parent"

        sys.exit(0)

    os.close(cout)
    os.close(cin)

    # Wait for child
    assert os.read(pin, 3) == "SYN", "Faulty child"

    rdlock.lock()

    assert os.write(pout, "SYN")



if __name__ == "__main__":
    print >>sys.stderr, "Running basic interface tests..."
    test_interface()
    print >>sys.stderr, "Running RW-locking stuff not clear from the manpages..."
    test_rwlocking()
    print >>sys.stderr, "OK."
