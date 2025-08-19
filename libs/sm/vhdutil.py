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

import os
from sm.core import util
import errno
import zlib
import re
from sm.core import xs_errors
import time

MIN_VHD_SIZE = 2 * 1024 * 1024
MAX_VHD_SIZE = 2040 * 1024 * 1024 * 1024
MAX_VHD_JOURNAL_SIZE = 6 * 1024 * 1024  # 2MB VHD block size, max 2TB VHD size
MAX_CHAIN_SIZE = 30  # max VHD parent chain size
VHD_UTIL = "/usr/bin/vhd-util"
OPT_LOG_ERR = "--debug"
DEFAULT_VHD_BLOCK_SIZE = 2 * 1024 * 1024
VHD_FOOTER_SIZE = 512

# lock to lock the entire SR for short ops
LOCK_TYPE_SR = "sr"

VDI_TYPE_VHD = 'vhd'
VDI_TYPE_RAW = 'aio'

FILE_EXTN_VHD = ".vhd"
FILE_EXTN_RAW = ".raw"
FILE_EXTN = {
        VDI_TYPE_VHD: FILE_EXTN_VHD,
        VDI_TYPE_RAW: FILE_EXTN_RAW
}


class VHDInfo:
    uuid = ""
    path = ""
    sizeVirt = -1
    sizePhys = -1
    sizeAllocated = -1
    hidden = False
    parentUuid = ""
    parentPath = ""
    error = 0

    def __init__(self, uuid):
        self.uuid = uuid


def calcOverheadEmpty(virtual_size):
    """Calculate the VHD space overhead (metadata size) for an empty VDI of
    size virtual_size"""
    overhead = 0
    size_mb = virtual_size // (1024 * 1024)

    # Footer + footer copy + header + possible CoW parent locator fields
    overhead = 3 * 1024

    # BAT 4 Bytes per block segment
    overhead += (size_mb // 2) * 4
    overhead = util.roundup(512, overhead)

    # BATMAP 1 bit per block segment
    overhead += (size_mb // 2) // 8
    overhead = util.roundup(4096, overhead)

    return overhead


def calcOverheadBitmap(virtual_size, block_size):
    num_blocks = virtual_size // block_size
    if virtual_size % block_size:
        num_blocks += 1
    return num_blocks * 4096


def ioretry(cmd, text=True):
    return util.ioretry(lambda: util.pread2(cmd, text=text),
                        errlist=[errno.EIO, errno.EAGAIN])

def getBlockSize(path):
    cmd = [VHD_UTIL, "read", "-pn", path]
    ret = ioretry(cmd)
    for field in ret.split('\n'):
        field = field.lstrip()
        if not field.startswith("Block size"): continue
        return int(field.split(':')[1].lstrip().split()[0])
    raise util.SMException("Unable to find block size in VHD with path: {}".format(path))


def convertAllocatedSizeToBytes(size, block_size):
    return size * block_size


def getVHDInfo(path, extractUuidFunction, includeParent=True):
    """Get the VHD info. The parent info may optionally be omitted: vhd-util
    tries to verify the parent by opening it, which results in error if the VHD
    resides on an inactive LV"""
    opts = "-vsaf"
    if includeParent:
        opts += "p"
    cmd = [VHD_UTIL, "query", OPT_LOG_ERR, opts, "-n", path]
    ret = ioretry(cmd)
    fields = ret.strip().split('\n')
    uuid = extractUuidFunction(path)
    vhdInfo = VHDInfo(uuid)
    vhdInfo.sizeVirt = int(fields[0]) * 1024 * 1024
    vhdInfo.sizePhys = int(fields[1])
    nextIndex = 2
    if includeParent:
        if fields[nextIndex].find("no parent") == -1:
            vhdInfo.parentPath = fields[nextIndex]
            vhdInfo.parentUuid = extractUuidFunction(fields[nextIndex])
        nextIndex += 1
    vhdInfo.hidden = int(fields[nextIndex].replace("hidden: ", ""))
    vhdInfo.sizeAllocated = convertAllocatedSizeToBytes(
        int(fields[nextIndex+1]),
        getBlockSize(path)
    )
    vhdInfo.path = path
    return vhdInfo


def getVHDInfoLVM(lvName, extractUuidFunction, vgName):
    """Get the VHD info. This function does not require the container LV to be
    active, but uses lvs & vgs"""
    vhdInfo = None
    cmd = [VHD_UTIL, "scan", "-f", "-l", vgName, "-m", lvName]
    ret = ioretry(cmd)
    return _parseVHDInfo(ret, extractUuidFunction)


def getAllVHDs(pattern, extractUuidFunction, vgName=None, \
        parentsOnly=False, exitOnError=False):
    vhds = dict()
    cmd = [VHD_UTIL, "scan", "-f", "-m", pattern]
    if vgName:
        cmd.append("-l")
        cmd.append(vgName)
    if parentsOnly:
        cmd.append("-a")
    try:
        ret = ioretry(cmd)
    except Exception as e:
        util.SMlog("WARN: vhd scan failed: output: %s" % e)
        ret = ioretry(cmd + ["-c"])
        util.SMlog("WARN: vhd scan with NOFAIL flag, output: %s" % ret)
    for line in ret.split('\n'):
        if len(line.strip()) == 0:
            continue
        vhdInfo = _parseVHDInfo(line, extractUuidFunction)
        if vhdInfo:
            if vhdInfo.error != 0 and exitOnError:
                # Just return an empty dict() so the scan will be done
                # again by getParentChain. See CA-177063 for details on
                # how this has been discovered during the stress tests.
                return dict()
            vhds[vhdInfo.uuid] = vhdInfo
        else:
            util.SMlog("WARN: vhdinfo line doesn't parse correctly: %s" % line)
    return vhds


def getParentChain(lvName, extractUuidFunction, vgName):
    """Get the chain of all VHD parents of 'path'. Safe to call for raw VDI's
    as well"""
    chain = dict()
    vdis = dict()
    retries = 0
    while (not vdis):
        if retries > 60:
            util.SMlog('ERROR: getAllVHDs returned 0 VDIs after %d retries' % retries)
            util.SMlog('ERROR: the VHD metadata might be corrupted')
            break
        vdis = getAllVHDs(lvName, extractUuidFunction, vgName, True, True)
        if (not vdis):
            retries = retries + 1
            time.sleep(1)
    for uuid, vdi in vdis.items():
        chain[uuid] = vdi.path
    #util.SMlog("Parent chain for %s: %s" % (lvName, chain))
    return chain


def getParent(path, extractUuidFunction):
    cmd = [VHD_UTIL, "query", OPT_LOG_ERR, "-p", "-n", path]
    ret = ioretry(cmd)
    if ret.find("query failed") != -1 or ret.find("Failed opening") != -1:
        raise util.SMException("VHD query returned %s" % ret)
    if ret.find("no parent") != -1:
        return None
    return extractUuidFunction(ret)


def hasParent(path):
    """Check if the VHD has a parent. A VHD has a parent iff its type is
    'Differencing'. This function does not need the parent to actually
    be present (e.g. the parent LV to be activated)."""
    cmd = [VHD_UTIL, "read", OPT_LOG_ERR, "-p", "-n", path]
    ret = ioretry(cmd)
    # pylint: disable=no-member
    m = re.match(".*Disk type\s+: (\S+) hard disk.*", ret, flags=re.S)
    vhd_type = m.group(1)
    assert(vhd_type == "Differencing" or vhd_type == "Dynamic")
    return vhd_type == "Differencing"


def setParent(path, parentPath, parentRaw):
    normpath = os.path.normpath(parentPath)
    cmd = [VHD_UTIL, "modify", OPT_LOG_ERR, "-p", normpath, "-n", path]
    if parentRaw:
        cmd.append("-m")
    ioretry(cmd)


def getHidden(path):
    cmd = [VHD_UTIL, "query", OPT_LOG_ERR, "-f", "-n", path]
    ret = ioretry(cmd)
    hidden = int(ret.split(':')[-1].strip())
    return hidden


def setHidden(path, hidden=True):
    opt = "1"
    if not hidden:
        opt = "0"
    cmd = [VHD_UTIL, "set", OPT_LOG_ERR, "-n", path, "-f", "hidden", "-v", opt]
    ret = ioretry(cmd)


def getSizeVirt(path):
    cmd = [VHD_UTIL, "query", OPT_LOG_ERR, "-v", "-n", path]
    ret = ioretry(cmd)
    size = int(ret) * 1024 * 1024
    return size


def setSizeVirt(path, size, jFile):
    "resize VHD offline"
    size_mb = size // (1024 * 1024)
    cmd = [VHD_UTIL, "resize", OPT_LOG_ERR, "-s", str(size_mb), "-n", path,
            "-j", jFile]
    ioretry(cmd)


def setSizeVirtFast(path, size):
    "resize VHD online"
    size_mb = size // (1024 * 1024)
    cmd = [VHD_UTIL, "resize", OPT_LOG_ERR, "-s", str(size_mb), "-n", path, "-f"]
    ioretry(cmd)


def getMaxResizeSize(path):
    """get the max virtual size for fast resize"""
    cmd = [VHD_UTIL, "query", OPT_LOG_ERR, "-S", "-n", path]
    ret = ioretry(cmd)
    return int(ret)


def getSizePhys(path):
    cmd = [VHD_UTIL, "query", OPT_LOG_ERR, "-s", "-n", path]
    ret = ioretry(cmd)
    return int(ret)


def setSizePhys(path, size, debug=True):
    "set physical utilisation (applicable to VHD's on fixed-size files)"
    if debug:
        cmd = [VHD_UTIL, "modify", OPT_LOG_ERR, "-s", str(size), "-n", path]
    else:
        cmd = [VHD_UTIL, "modify", "-s", str(size), "-n", path]
    ioretry(cmd)


def getAllocatedSize(path):
    cmd = [VHD_UTIL, "query", OPT_LOG_ERR, '-a', '-n', path]
    ret = ioretry(cmd)
    return convertAllocatedSizeToBytes(int(ret), getBlockSize(path))

def killData(path):
    "zero out the disk (kill all data inside the VHD file)"
    cmd = [VHD_UTIL, "modify", OPT_LOG_ERR, "-z", "-n", path]
    ioretry(cmd)


def getDepth(path):
    "get the VHD parent chain depth"
    cmd = [VHD_UTIL, "query", OPT_LOG_ERR, "-d", "-n", path]
    text = ioretry(cmd)
    depth = -1
    if text.startswith("chain depth:"):
        depth = int(text.split(':')[1].strip())
    return depth


def getBlockBitmap(path):
    cmd = [VHD_UTIL, "read", OPT_LOG_ERR, "-B", "-n", path]
    text = ioretry(cmd, text=False)
    return zlib.compress(text)


def coalesce(path):
    """
    Coalesce the VHD, on success it returns the number of sectors coalesced
    """
    cmd = [VHD_UTIL, "coalesce", OPT_LOG_ERR, "-n", path]
    text = ioretry(cmd)
    match = re.match(r'^Coalesced (\d+) sectors', text)
    if match:
        return int(match.group(1))

    return 0


def create(path, size, static, msize=0):
    size_mb = size // (1024 * 1024)
    cmd = [VHD_UTIL, "create", OPT_LOG_ERR, "-n", path, "-s", str(size_mb)]
    if static:
        cmd.append("-r")
    if msize:
        cmd.append("-S")
        cmd.append(str(msize))
    ioretry(cmd)


def snapshot(path, parent, parentRaw, msize=0, checkEmpty=True):
    cmd = [VHD_UTIL, "snapshot", OPT_LOG_ERR, "-n", path, "-p", parent]
    if parentRaw:
        cmd.append("-m")
    if msize:
        cmd.append("-S")
        cmd.append(str(msize))
    if not checkEmpty:
        cmd.append("-e")
    ioretry(cmd)


def check(path, ignoreMissingFooter=False, fast=False):
    cmd = [VHD_UTIL, "check", OPT_LOG_ERR, "-n", path]
    if ignoreMissingFooter:
        cmd.append("-i")
    if fast:
        cmd.append("-B")
    try:
        ioretry(cmd)
        return True
    except util.CommandException:
        return False


def revert(path, jFile):
    cmd = [VHD_UTIL, "revert", OPT_LOG_ERR, "-n", path, "-j", jFile]
    ioretry(cmd)


def _parseVHDInfo(line, extractUuidFunction):
    vhdInfo = None
    valueMap = line.split()
    if len(valueMap) < 1 or valueMap[0].find("vhd=") == -1:
        return None
    for keyval in valueMap:
        (key, val) = keyval.split('=')
        if key == "vhd":
            uuid = extractUuidFunction(val)
            if not uuid:
                util.SMlog("***** malformed output, no UUID: %s" % valueMap)
                return None
            vhdInfo = VHDInfo(uuid)
            vhdInfo.path = val
        elif key == "scan-error":
            vhdInfo.error = line
            util.SMlog("***** VHD scan error: %s" % line)
            break
        elif key == "capacity":
            vhdInfo.sizeVirt = int(val)
        elif key == "size":
            vhdInfo.sizePhys = int(val)
        elif key == "hidden":
            vhdInfo.hidden = int(val)
        elif key == "parent" and val != "none":
            vhdInfo.parentPath = val
            vhdInfo.parentUuid = extractUuidFunction(val)
    return vhdInfo


def _getVHDParentNoCheck(path):
    cmd = ["vhd-util", "read", "-p", "-n", "%s" % path]
    text = util.pread(cmd)
    util.SMlog(text)
    for line in text.split('\n'):
        if line.find("decoded name :") != -1:
            val = line.split(':')[1].strip()
            vdi = val.replace("--", "-")[-40:]
            if vdi[1:].startswith("LV-"):
                vdi = vdi[1:]
            return vdi
    return None


def repair(path):
    """Repairs the VHD."""
    ioretry([VHD_UTIL, 'repair', '-n', path])


def validate_and_round_vhd_size(size, block_size):
    """ Take the supplied vhd size, in bytes, and check it is positive and less
    that the maximum supported size, rounding up to the next block boundary
    """
    if size < 0 or size > MAX_VHD_SIZE:
        raise xs_errors.XenError(
            'VDISize', opterr='VDI size ' +
                              'must be between 1 MB and %d MB' %
                              (MAX_VHD_SIZE // (1024 * 1024)))

    if size < MIN_VHD_SIZE:
        size = MIN_VHD_SIZE

    size = util.roundup(block_size, size)

    return size


def getKeyHash(path):
    """Extract the hash of the encryption key from the header of an encrypted VHD"""
    cmd = ["vhd-util", "key", "-p", "-n", path]
    ret = ioretry(cmd)
    ret = ret.strip()
    if ret == 'none':
        return None
    vals = ret.split()
    if len(vals) != 2:
        util.SMlog('***** malformed output from vhd-util'
                   ' for VHD {}: "{}"'.format(path, ret))
        return None
    [_nonce, key_hash] = vals
    return key_hash


def setKey(path, key_hash):
    """Set the encryption key for a VHD"""
    cmd = ["vhd-util", "key", "-s", "-n", path, "-H", key_hash]
    ioretry(cmd)
