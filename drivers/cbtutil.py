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
#
# Helper functions pertaining to VHD operations
#

import util
import uuid

CBT_UTIL = "/usr/sbin/cbt-util"

def createCBTLog(fileName, size):
    """Create and initialise log file for tracking changed blocks"""
    cmd = [CBT_UTIL, "create", "-n", fileName, "-s", str(size)]
    _callCBTUtil(cmd)

def setCBTParent(fileName, parentUuid):
    """Set parent field in log file"""
    cmd = [CBT_UTIL, "set", "-n", fileName, "-p", str(parentUuid)]
    _callCBTUtil(cmd)

def getCBTParent(fileName):
    """Get parent field from log file"""
    cmd = [CBT_UTIL, "get", "-n", fileName, "-p"]
    ret =  _callCBTUtil(cmd)
    u = uuid.UUID(ret.strip())
    #TODO: Need to check for NULL UUID
    # Ideally, we want to do
    # if uuid.UUID(ret.strip()).int == 0
    #     return None
    # Pylint doesn't like this for reason though
    return str(u)

def setCBTChild(fileName, childUuid):
    """Set child field in log file"""
    cmd = [CBT_UTIL, "set", "-n", fileName, "-c", str(childUuid)]
    _callCBTUtil(cmd)

def getCBTChild(fileName):
    """Get parent field from log file"""
    cmd = [CBT_UTIL, "get", "-n", fileName, "-c"]
    ret =  _callCBTUtil(cmd)
    u = uuid.UUID(ret.strip())
    #TODO: Need to check for NULL UUID
    return str(u)

def setCBTConsistency(fileName, consistent):
    """Set consistency field in log file"""
    if consistent:
        flag = 1
    else:
        flag = 0
    cmd = [CBT_UTIL, "set", "-n", fileName, "-f", str(flag)]
    _callCBTUtil(cmd)

def getCBTConsistency(fileName):
    """Get consistency field from log file"""
    cmd = [CBT_UTIL, "get", "-n", fileName, "-f"]
    ret =  _callCBTUtil(cmd)
    return bool(int(ret.strip()))

def getCBTBitmap(fileName):
    """Get bitmap field from log file"""
    cmd = [CBT_UTIL, "get", "-n", fileName, "-b"]
    ret =  _callCBTUtil(cmd)
    return ret.strip()

def set_cbt_size(filename, size):
    """Set size field in log file"""
    cmd = [CBT_UTIL, "set", "-n", filename, "-s", str(size)]
    _callCBTUtil(cmd)

def _callCBTUtil(cmd):
    return util.ioretry(lambda: util.pread2(cmd))
