#!/usr/bin/python
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
from __future__ import print_function
import sys

from lock import Lock


def test():

    # Create a Lock
    lock = Lock("test");

    # Should not be yet held.
    assert lock.held() == False

    # Go get it
    lock.acquire()

    # Second lock shall throw in debug mode.
    try:
        lock.acquire()
    except AssertionError as e:
        if str(e) != flock.WriteLock.ERROR_ISLOCKED:
            raise
    else:
        raise AssertionError("Reaquired a locked lock")

    lock.release()

    Lock.cleanup()

if __name__ == '__main__':
    print("Running self tests...", file=sys.stderr)
    test()
    print("OK.", file=sys.stderr)
