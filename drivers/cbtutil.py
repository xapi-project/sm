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

CBT_UTIL = "/usr/sbin/cbt-util"

def createCBTLog(fileName, size):
    """Create and initialise log file for tracking changed blocks"""
    cmd = [CBT_UTIL, "create", "-n", fileName, "-s", str(size)]
    util.ioretry(lambda: util.pread2(cmd))

def setCBTParent(fileName, parentUuid):
    """Set parent field in log file"""
    cmd = [CBT_UTIL, "set", "-n", fileName, "-p", str(parentUuid)]
    util.ioretry(lambda: util.pread2(cmd))

def setCBTChild(fileName, childUuid):
    """Set child field in log file"""
    cmd = [CBT_UTIL, "set", "-n", fileName, "-c", str(childUuid)]
    util.ioretry(lambda: util.pread2(cmd))

def setCBTConsistency(fileName, consistent):
    """Set consistency field in log file"""
    if consistent:
        flag = 1
    else:
        flag = 0
    cmd = [CBT_UTIL, "set", "-n", fileName, "-f", str(flag)]
    util.ioretry(lambda: util.pread2(cmd))
