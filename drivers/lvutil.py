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
# Miscellaneous LVM utility functions
#

import traceback
import re
import os
import errno
import time

from fairlock import Fairlock
import util
import xs_errors
import xml.dom.minidom
from lvhdutil import VG_LOCATION, VG_PREFIX
from constants import EXT_PREFIX
import lvmcache
import srmetadata

MDVOLUME_NAME = 'MGT'
VDI_UUID_TAG_PREFIX = 'vdi_'
LVM_BIN = os.path.isfile('/sbin/lvdisplay') and '/sbin' or '/usr/sbin'
CMD_VGS = "vgs"
CMD_VGCREATE = "vgcreate"
CMD_VGREMOVE = "vgremove"
CMD_VGCHANGE = "vgchange"
CMD_VGEXTEND = "vgextend"
CMD_PVS = "pvs"
CMD_PVCREATE = "pvcreate"
CMD_PVREMOVE = "pvremove"
CMD_PVRESIZE = "pvresize"
CMD_LVS = "lvs"
CMD_LVDISPLAY = "lvdisplay"
CMD_LVCREATE = "lvcreate"
CMD_LVREMOVE = "lvremove"
CMD_LVCHANGE = "lvchange"
CMD_LVRENAME = "lvrename"
CMD_LVRESIZE = "lvresize"
CMD_LVEXTEND = "lvextend"
CMD_DMSETUP = "/sbin/dmsetup"

MAX_OPERATION_DURATION = 15

LVM_SIZE_INCREMENT = 4 * 1024 * 1024
LV_TAG_HIDDEN = "hidden"
LVM_FAIL_RETRIES = 10

MASTER_LVM_CONF = '/etc/lvm/master'
DEF_LVM_CONF = '/etc/lvm'

VG_COMMANDS = frozenset({CMD_VGS, CMD_VGCREATE, CMD_VGREMOVE, CMD_VGCHANGE,
                         CMD_VGEXTEND})
PV_COMMANDS = frozenset({CMD_PVS, CMD_PVCREATE, CMD_PVREMOVE, CMD_PVRESIZE})
LV_COMMANDS = frozenset({CMD_LVS, CMD_LVDISPLAY, CMD_LVCREATE, CMD_LVREMOVE,
                         CMD_LVCHANGE, CMD_LVRENAME, CMD_LVRESIZE,
                         CMD_LVEXTEND})
DM_COMMANDS = frozenset({CMD_DMSETUP})

LVM_COMMANDS = VG_COMMANDS.union(PV_COMMANDS, LV_COMMANDS, DM_COMMANDS)

LVM_LOCK = 'lvm'


def extract_vgname(str_in):
    """Search for and return a VG name

        Search 'str_in' for a substring in the form of 'VG_XenStorage-<UUID>'. 
        If there are more than one VG names, the first is returned.

        Input:
            str_in -- (str) string to search for a VG name
                            in the format specified above.

        Return:
            vgname -- if found     -> (str)
                      if not found -> None

        Raise:
            TypeError
    """

    if not util.is_string(str_in):
        raise TypeError("'str_in' not of type 'str'.")

    i = str_in.find(VG_PREFIX)
    prefix = VG_PREFIX

    if i == -1:
        i = str_in.find(EXT_PREFIX)
        prefix = EXT_PREFIX

    uuid_start = i + len(prefix)
    re_obj = util.match_uuid(str_in[uuid_start:])

    if i != -1 and re_obj:
        return prefix + re_obj.group(0)  # vgname

    return None

LVM_RETRY_ERRORS = [
    "Incorrect checksum in metadata area header"
]


def lvmretry(func):
    def check_exception(exception):
        retry = False
        for error in LVM_RETRY_ERRORS:
            if error in exception.reason:
                retry = True
        return retry

    def decorated(*args, **kwargs):
        for i in range(LVM_FAIL_RETRIES):
            try:
                return func(*args, **kwargs)
            except util.CommandException as ce:
                retry = check_exception(ce)
                if not retry or (i == LVM_FAIL_RETRIES - 1):
                    raise

                time.sleep(1)

    decorated.__name__ = func.__name__
    return decorated


def cmd_lvm(cmd, pread_func=util.pread2, *args):
    """ Construct and run the appropriate lvm command.

        For PV commands, the full path to the device is required.

        Input:
            cmd -- (list) lvm command
                cmd[0]  -- (str) lvm command name
                cmd[1:] -- (str) lvm command parameters

            pread_func -- (function) the flavor of util.pread to use
                                     to execute the lvm command
                Default: util.pread2()

            *args -- extra arguments passed to cmd_lvm will be passed
                     to 'pread_func'

        Return:
            stdout -- (str) stdout after running the lvm command.

        Raise:
            util.CommandException
    """

    if type(cmd) is not list:
        util.SMlog("CMD_LVM: Argument 'cmd' not of type 'list'")
        return None
    if not len(cmd):
        util.SMlog("CMD_LVM: 'cmd' list is empty")
        return None

    lvm_cmd, lvm_args = cmd[0], cmd[1:]

    if lvm_cmd not in LVM_COMMANDS:
        util.SMlog("CMD_LVM: '{}' is not a valid lvm command".format(lvm_cmd))
        return None

    for arg in lvm_args:
        if not util.is_string(arg):
            util.SMlog("CMD_LVM: Not all lvm arguments are of type 'str'")
            return None

    with Fairlock("devicemapper"):
        start_time = time.time()
        stdout = pread_func([os.path.join(LVM_BIN, lvm_cmd)] + lvm_args, * args)
        end_time = time.time()

    if (end_time - start_time > MAX_OPERATION_DURATION):
        util.SMlog("***** Long LVM call of '%s' took %s" % (lvm_cmd, (end_time - start_time)))

    return stdout


class LVInfo:
    name = ""
    size = 0
    active = False
    open = False
    hidden = False
    readonly = False

    def __init__(self, name):
        self.name = name

    def toString(self):
        return "%s, size=%d, active=%s, open=%s, hidden=%s, ro=%s" % \
                (self.name, self.size, self.active, self.open, self.hidden, \
                self.readonly)


def _checkVG(vgname):
    try:
        cmd_lvm([CMD_VGS, "--readonly", vgname])
        return True
    except:
        return False


def _checkPV(pvname):
    try:
        cmd_lvm([CMD_PVS, pvname])
        return True
    except:
        return False


def _checkLV(path):
    try:
        cmd_lvm([CMD_LVDISPLAY, path])
        return True
    except:
        return False


def _getLVsize(path):
    try:
        lines = cmd_lvm([CMD_LVDISPLAY, "-c", path]).split(':')
        return int(lines[6]) * 512
    except:
        raise xs_errors.XenError('VDIUnavailable', \
              opterr='no such VDI %s' % path)


def _getVGstats(vgname):
    try:
        text = cmd_lvm([CMD_VGS, "--noheadings", "--nosuffix",
                        "--units", "b", vgname],
                        pread_func=util.pread).split()
        size = int(text[5])
        freespace = int(text[6])
        utilisation = size - freespace
        stats = {}
        stats['physical_size'] = size
        stats['physical_utilisation'] = utilisation
        stats['freespace'] = freespace
        return stats
    except util.CommandException as inst:
        raise xs_errors.XenError('VDILoad', \
              opterr='rvgstats failed error is %d' % inst.code)
    except ValueError:
        raise xs_errors.XenError('VDILoad', opterr='rvgstats failed')


def _getPVstats(dev):
    try:
        text = cmd_lvm([CMD_PVS, "--noheadings", "--nosuffix",
                        "--units", "b", dev],
                        pread_func=util.pread).split()
        size = int(text[4])
        freespace = int(text[5])
        utilisation = size - freespace
        stats = {}
        stats['physical_size'] = size
        stats['physical_utilisation'] = utilisation
        stats['freespace'] = freespace
        return stats
    except util.CommandException as inst:
        raise xs_errors.XenError('VDILoad', \
              opterr='pvstats failed error is %d' % inst.code)
    except ValueError:
        raise xs_errors.XenError('VDILoad', opterr='rvgstats failed')


# Retrieves the UUID of the SR that corresponds to the specified Physical
# Volume (pvname). Each element in prefix_list is checked whether it is a
# prefix of Volume Groups that correspond to the specified PV. If so, the
# prefix is stripped from the matched VG name and the remainder is returned
# (effectively the SR UUID). If no match if found, the empty string is
# returned.
# E.g.
#   PV         VG                          Fmt  Attr PSize   PFree
#  /dev/sda4  VG_XenStorage-some-hex-value lvm2 a-   224.74G 223.73G
# will return "some-hex-value".
def _get_sr_uuid(pvname, prefix_list):
    try:
        return match_VG(cmd_lvm([CMD_PVS, "--noheadings",
                        "-o", "vg_name", pvname]), prefix_list)
    except:
        return ""


# Retrieves the names of the Physical Volumes which are used by the specified
# Volume Group
# e.g.
#   PV         VG                          Fmt  Attr PSize   PFree
#  /dev/sda4  VG_XenStorage-some-hex-value lvm2 a-   224.74G 223.73G
# will return "/dev/sda4" when given the argument "VG_XenStorage-some-hex-value".
def get_pv_for_vg(vgname):
    try:
        result = cmd_lvm([CMD_PVS, "--noheadings",
                          '-S', 'vg_name=%s' % vgname, '-o', 'name'])
        return [x.strip() for x in result.splitlines()]
    except util.CommandException:
        return []


# Tries to match any prefix contained in prefix_list in s. If matched, the
# remainder string is returned, else the empty string is returned. E.g. if s is
# "VG_XenStorage-some-hex-value" and prefix_list contains "VG_XenStorage-",
# "some-hex-value" is returned.
#
# TODO Items in prefix_list are expected to be found at the beginning of the
# target string, though if any of them is found inside it a match will be
# produced. This could be remedied by making the regular expression more
# specific.
def match_VG(s, prefix_list):
    for val in prefix_list:
        regex = re.compile(val)
        if regex.search(s, 0):
            return s.split(val)[1]
    return ""


# Retrieves the devices an SR is composed of. A dictionary is returned, indexed
# by the SR UUID, where each SR UUID is mapped to a comma-separated list of
# devices. Exceptions are ignored.
def scan_srlist(prefix, root):
    VGs = {}
    for dev in root.split(','):
        try:
            sr_uuid = _get_sr_uuid(dev, [prefix]).strip(' \n')
            if len(sr_uuid):
                if sr_uuid in VGs:
                    VGs[sr_uuid] += ",%s" % dev
                else:
                    VGs[sr_uuid] = dev
        except Exception as e:
            util.logException("exception (ignored): %s" % e)
            continue
    return VGs


# Converts an SR list to an XML document with the following structure:
# <SRlist>
#   <SR>
#       <UUID>...</UUID>
#       <Devlist>...</Devlist>
#       <size>...</size>
#       <!-- If includeMetadata is set to True, the following additional nodes
#       are supplied. -->
#       <name_label>...</name_label>
#       <name_description>...</name_description>
#       <pool_metadata_detected>...</pool_metadata_detected>
#   </SR>
#
#   <SR>...</SR>
# </SRlist>
#
# Arguments:
#   VGs: a dictionary containing the SR UUID to device list mappings
#   prefix: the prefix that if prefixes the SR UUID the VG is produced
#   includeMetadata (optional): include additional information
def srlist_toxml(VGs, prefix, includeMetadata=False):
    dom = xml.dom.minidom.Document()
    element = dom.createElement("SRlist")
    dom.appendChild(element)

    for val in VGs:
        entry = dom.createElement('SR')
        element.appendChild(entry)

        subentry = dom.createElement("UUID")
        entry.appendChild(subentry)
        textnode = dom.createTextNode(val)
        subentry.appendChild(textnode)

        subentry = dom.createElement("Devlist")
        entry.appendChild(subentry)
        textnode = dom.createTextNode(VGs[val])
        subentry.appendChild(textnode)

        subentry = dom.createElement("size")
        entry.appendChild(subentry)
        size = str(_getVGstats(prefix + val)['physical_size'])
        textnode = dom.createTextNode(size)
        subentry.appendChild(textnode)

        if includeMetadata:
            metadataVDI = None

            # add SR name_label
            mdpath = os.path.join(VG_LOCATION, VG_PREFIX + val)
            mdpath = os.path.join(mdpath, MDVOLUME_NAME)
            mgtVolActivated = False
            try:
                if not os.path.exists(mdpath):
                    # probe happens out of band with attach so this volume
                    # may not have been activated at this point
                    lvmCache = lvmcache.LVMCache(VG_PREFIX + val)
                    lvmCache.activateNoRefcount(MDVOLUME_NAME)
                    mgtVolActivated = True

                sr_metadata = \
                    srmetadata.LVMMetadataHandler(mdpath, \
                                                        False).getMetadata()[0]
                subentry = dom.createElement("name_label")
                entry.appendChild(subentry)
                textnode = dom.createTextNode(sr_metadata[srmetadata.NAME_LABEL_TAG])
                subentry.appendChild(textnode)

                # add SR description
                subentry = dom.createElement("name_description")
                entry.appendChild(subentry)
                textnode = dom.createTextNode(sr_metadata[srmetadata.NAME_DESCRIPTION_TAG])
                subentry.appendChild(textnode)

                # add metadata VDI UUID
                metadataVDI = srmetadata.LVMMetadataHandler(mdpath, \
                                    False).findMetadataVDI()
                subentry = dom.createElement("pool_metadata_detected")
                entry.appendChild(subentry)
                if metadataVDI is not None:
                    subentry.appendChild(dom.createTextNode("true"))
                else:
                    subentry.appendChild(dom.createTextNode("false"))
            finally:
                if mgtVolActivated:
                    # deactivate only if we activated it
                    lvmCache.deactivateNoRefcount(MDVOLUME_NAME)

    return dom.toprettyxml()


def _openExclusive(dev, retry):
    try:
        return os.open("%s" % dev, os.O_RDWR | os.O_EXCL)
    except OSError as ose:
        opened_by = ''
        if ose.errno == 16:
            if retry:
                util.SMlog('Device %s is busy, settle and one shot retry' %
                           dev)
                util.pread2(['/usr/sbin/udevadm', 'settle'])
                return _openExclusive(dev, False)
            else:
                util.SMlog('Device %s is busy after retry' % dev)

        util.SMlog('Opening device %s failed with %d' % (dev, ose.errno))
        raise xs_errors.XenError(
            'SRInUse', opterr=('Device %s in use, please check your existing '
                               + 'SRs for an instance of this device') % dev)


def createVG(root, vgname):
    systemroot = util.getrootdev()
    rootdev = root.split(',')[0]

    # Create PVs for each device
    for dev in root.split(','):
        if dev in [systemroot, '%s1' % systemroot, '%s2' % systemroot]:
            raise xs_errors.XenError('Rootdev', \
                  opterr=('Device %s contains core system files, ' \
                          + 'please use another device') % dev)
        if not os.path.exists(dev):
            raise xs_errors.XenError('InvalidDev', \
                  opterr=('Device %s does not exist') % dev)

        f = _openExclusive(dev, True)
        os.close(f)
        try:
            # Overwrite the disk header, try direct IO first
            cmd = [util.CMD_DD, "if=/dev/zero", "of=%s" % dev, "bs=1M",
                    "count=10", "oflag=direct"]
            util.pread2(cmd)
        except util.CommandException as inst:
            if inst.code == errno.EPERM:
                try:
                    # Overwrite the disk header, try normal IO
                    cmd = [util.CMD_DD, "if=/dev/zero", "of=%s" % dev,
                            "bs=1M", "count=10"]
                    util.pread2(cmd)
                except util.CommandException as inst:
                    raise xs_errors.XenError('LVMWrite', \
                          opterr='device %s' % dev)
            else:
                raise xs_errors.XenError('LVMWrite', \
                      opterr='device %s' % dev)

        if not (dev == rootdev):
            try:
                cmd_lvm([CMD_PVCREATE, "-ff", "-y", "--metadatasize", "10M", dev])
            except util.CommandException as inst:
                raise xs_errors.XenError('LVMPartCreate',
                                         opterr='error is %d' % inst.code)

    # Create VG on first device
    try:
        cmd_lvm([CMD_VGCREATE, "--metadatasize", "10M", vgname, rootdev])
    except:
        raise xs_errors.XenError('LVMGroupCreate')

    # Then add any additional devs into the VG
    for dev in root.split(',')[1:]:
        try:
            cmd_lvm([CMD_VGEXTEND, vgname, dev])
        except util.CommandException as inst:
            # One of the PV args failed, delete SR
            try:
                cmd_lvm([CMD_VGREMOVE, vgname])
            except:
                pass
            raise xs_errors.XenError('LVMGroupCreate')

    try:
        cmd_lvm([CMD_VGCHANGE, "-an", vgname])
    except util.CommandException as inst:
        raise xs_errors.XenError('LVMUnMount', opterr='errno is %d' % inst.code)

    # End block

def removeVG(root, vgname):
    # Check PVs match VG
    try:
        for dev in root.split(','):
            txt = cmd_lvm([CMD_PVS, dev])
            if txt.find(vgname) == -1:
                raise xs_errors.XenError('LVMNoVolume', \
                      opterr='volume is %s' % vgname)
    except util.CommandException as inst:
        raise xs_errors.XenError('PVSfailed', \
              opterr='error is %d' % inst.code)

    try:
        cmd_lvm([CMD_VGREMOVE, vgname])

        for dev in root.split(','):
            cmd_lvm([CMD_PVREMOVE, dev])
    except util.CommandException as inst:
        raise xs_errors.XenError('LVMDelete', \
              opterr='errno is %d' % inst.code)


def resizePV(dev):
    try:
        cmd_lvm([CMD_PVRESIZE, dev])
    except util.CommandException as inst:
        util.SMlog("Failed to grow the PV, non-fatal")


def setActiveVG(path, active):
    "activate or deactivate VG 'path'"
    val = "n"
    if active:
        val = "y"
    text = cmd_lvm([CMD_VGCHANGE, "-a" + val, path])


@lvmretry
def create(name, size, vgname, tag=None, size_in_percentage=None):
    if size_in_percentage:
        cmd = [CMD_LVCREATE, "-n", name, "-l", size_in_percentage, vgname]
    else:
        size_mb = size // (1024 * 1024)
        cmd = [CMD_LVCREATE, "-n", name, "-L", str(size_mb), vgname]
    if tag:
        cmd.extend(["--addtag", tag])

    cmd.extend(['-W', 'n'])
    cmd_lvm(cmd)


def remove(path, config_param=None):
    # see deactivateNoRefcount()
    for i in range(LVM_FAIL_RETRIES):
        try:
            _remove(path, config_param)
            break
        except util.CommandException as e:
            if i >= LVM_FAIL_RETRIES - 1:
                raise
            util.SMlog("*** lvremove failed on attempt #%d" % i)
    _lvmBugCleanup(path)


@lvmretry
def _remove(path, config_param=None):
    CONFIG_TAG = "--config"
    cmd = [CMD_LVREMOVE, "-f", path]
    if config_param:
        cmd.extend([CONFIG_TAG, "devices{" + config_param + "}"])
    ret = cmd_lvm(cmd)


@lvmretry
def rename(path, newName):
    cmd_lvm([CMD_LVRENAME, path, newName], pread_func=util.pread)


@lvmretry
def setReadonly(path, readonly):
    val = "r"
    if not readonly:
        val += "w"
    ret = cmd_lvm([CMD_LVCHANGE, path, "-p", val], pread_func=util.pread)


def exists(path):
    (rc, stdout, stderr) = cmd_lvm([CMD_LVS, "--noheadings", path], pread_func=util.doexec)
    return rc == 0


@lvmretry
def setSize(path, size, confirm):
    sizeMB = size // (1024 * 1024)
    if confirm:
        cmd_lvm([CMD_LVRESIZE, "-L", str(sizeMB), path], util.pread3, "y\n")
    else:
        cmd_lvm([CMD_LVRESIZE, "-L", str(sizeMB), path], pread_func=util.pread)


@lvmretry
def setHidden(path, hidden=True):
    opt = "--addtag"
    if not hidden:
        opt = "--deltag"
    cmd_lvm([CMD_LVCHANGE, opt, LV_TAG_HIDDEN, path])


@lvmretry
def _activate(path):
    cmd = [CMD_LVCHANGE, "-ay", path]
    cmd_lvm(cmd)
    if not _checkActive(path):
        raise util.CommandException(-1, str(cmd), "LV not activated")


def activateNoRefcount(path, refresh):
    _activate(path)
    if refresh:
        # Override slave mode lvm.conf for this command
        os.environ['LVM_SYSTEM_DIR'] = MASTER_LVM_CONF
        text = cmd_lvm([CMD_LVCHANGE, "--refresh", path])
        mapperDevice = path[5:].replace("-", "--").replace("/", "-")
        cmd = [CMD_DMSETUP, "table", mapperDevice]
        with Fairlock("devicemapper"):
            ret = util.pread(cmd)
        util.SMlog("DM table for %s: %s" % (path, ret.strip()))
        # Restore slave mode lvm.conf
        os.environ['LVM_SYSTEM_DIR'] = DEF_LVM_CONF


def deactivateNoRefcount(path):
    # LVM has a bug where if an "lvs" command happens to run at the same time
    # as "lvchange -an", it might hold the device in use and cause "lvchange
    # -an" to fail. Thus, we need to retry if "lvchange -an" fails. Worse yet,
    # the race could lead to "lvchange -an" starting to deactivate (removing
    # the symlink), failing to "dmsetup remove" the device, and still returning
    # success. Thus, we need to check for the device mapper file existence if
    # "lvchange -an" returns success.
    for i in range(LVM_FAIL_RETRIES):
        try:
            _deactivate(path)
            break
        except util.CommandException:
            if i >= LVM_FAIL_RETRIES - 1:
                raise
            util.SMlog("*** lvchange -an failed on attempt #%d" % i)
    _lvmBugCleanup(path)


@lvmretry
def _deactivate(path):
    text = cmd_lvm([CMD_LVCHANGE, "-an", path])


def _checkActive(path):
    if util.pathexists(path):
        return True

    util.SMlog("_checkActive: %s does not exist!" % path)
    symlinkExists = os.path.lexists(path)
    util.SMlog("_checkActive: symlink exists: %s" % symlinkExists)

    mapperDeviceExists = False
    mapperDevice = path[5:].replace("-", "--").replace("/", "-")
    cmd = [CMD_DMSETUP, "status", mapperDevice]
    try:
        with Fairlock("devicemapper"):
            ret = util.pread2(cmd)
        mapperDeviceExists = True
        util.SMlog("_checkActive: %s: %s" % (mapperDevice, ret))
    except util.CommandException:
        util.SMlog("_checkActive: device %s does not exist" % mapperDevice)

    mapperPath = "/dev/mapper/" + mapperDevice
    mapperPathExists = util.pathexists(mapperPath)
    util.SMlog("_checkActive: path %s exists: %s" % \
            (mapperPath, mapperPathExists))

    if mapperDeviceExists and mapperPathExists and not symlinkExists:
        # we can fix this situation manually here
        try:
            util.SMlog("_checkActive: attempt to create the symlink manually.")
            os.symlink(mapperPath, path)
        except OSError as e:
            util.SMlog("ERROR: failed to symlink!")
            if e.errno != errno.EEXIST:
                raise
        if util.pathexists(path):
            util.SMlog("_checkActive: created the symlink manually")
            return True

    return False


def _lvmBugCleanup(path):
    # the device should not exist at this point. If it does, this was an LVM
    # bug, and we manually clean up after LVM here
    mapperDevice = path[5:].replace("-", "--").replace("/", "-")
    mapperPath = "/dev/mapper/" + mapperDevice

    nodeExists = False
    cmd_st = [CMD_DMSETUP, "status", mapperDevice]
    cmd_rm = [CMD_DMSETUP, "remove", mapperDevice]
    cmd_rf = [CMD_DMSETUP, "remove", mapperDevice, "--force"]

    try:
        with Fairlock("devicemapper"):
            util.pread(cmd_st, expect_rc=1)
    except util.CommandException as e:
        if e.code == 0:
            nodeExists = True

    if not util.pathexists(mapperPath) and not nodeExists:
        return

    util.SMlog("_lvmBugCleanup: seeing dm file %s" % mapperPath)

    # destroy the dm device
    if nodeExists:
        util.SMlog("_lvmBugCleanup: removing dm device %s" % mapperDevice)
        for i in range(LVM_FAIL_RETRIES):
            try:
                with Fairlock("devicemapper"):
                    util.pread2(cmd_rm)
                break
            except util.CommandException as e:
                if i < LVM_FAIL_RETRIES - 1:
                    util.SMlog("Failed on try %d, retrying" % i)
                    try:
                        with Fairlock("devicemapper"):
                            util.pread(cmd_st, expect_rc=1)
                        util.SMlog("_lvmBugCleanup: dm device {}"
                                   " removed".format(mapperDevice)
                                   )
                        break
                    except:
                        cmd_rm = cmd_rf
                        time.sleep(1)
                else:
                    # make sure the symlink is still there for consistency
                    if not os.path.lexists(path):
                        os.symlink(mapperPath, path)
                        util.SMlog("_lvmBugCleanup: restored symlink %s" % path)
                    raise e

    if util.pathexists(mapperPath):
        os.unlink(mapperPath)
        util.SMlog("_lvmBugCleanup: deleted devmapper file %s" % mapperPath)

    # delete the symlink
    if os.path.lexists(path):
        os.unlink(path)
        util.SMlog("_lvmBugCleanup: deleted symlink %s" % path)


# mdpath is of format /dev/VG-SR-UUID/MGT
# or in other words /VG_LOCATION/VG_PREFIXSR-UUID/MDVOLUME_NAME
def ensurePathExists(mdpath):
    if not os.path.exists(mdpath):
        vgname = mdpath.split('/')[2]
        lvmCache = lvmcache.LVMCache(vgname)
        lvmCache.activateNoRefcount(MDVOLUME_NAME)


def removeDevMapperEntry(path, strict=True):
    try:
        # remove devmapper entry using dmsetup
        cmd = [CMD_DMSETUP, "remove", path]
        cmd_lvm(cmd)
        return True
    except Exception as e:
        if not strict:
            cmd = [CMD_DMSETUP, "status", path]
            try:
                with Fairlock("devicemapper"):
                    util.pread(cmd, expect_rc=1)
                return True
            except:
                pass  # Continuining will fail and log the right way
        ret = util.pread2(["lsof", path])
        util.SMlog("removeDevMapperEntry: dmsetup remove failed for file %s " \
                   "with error %s, and lsof ret is %s." % (path, str(e), ret))
        return False
