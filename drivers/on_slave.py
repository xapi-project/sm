#!/usr/bin/python3
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
# A plugin for synchronizing slaves when something changes on the Master

import sys
sys.path.append("/opt/xensource/sm/")
from sm.core import util
from sm.core import lock
from sm.lvmcache import LVMCache
from sm.core import scsiutil


def multi(session, args):
    """Perform several actions in one call (to save on round trips)"""
    util.SMlog("on-slave.multi: %s" % args)
    vgName = args["vgName"]
    lvmCache = LVMCache(vgName)
    i = 1
    while True:
        action = args.get("action%d" % i)
        if not action:
            break
        util.SMlog("on-slave.action %d: %s" % (i, action))
        if action == "activate":
            try:
                lvmCache.activate(args["ns%d" % i], args["uuid%d" % i],
                                  args["lvName%d" % i], False)
            except util.CommandException:
                util.SMlog("on-slave.activate failed")
                raise
        elif action == "deactivate":
            try:
                lvmCache.deactivate(args["ns%d" % i], args["uuid%d" % i],
                        args["lvName%d" % i], False)
            except util.SMException:
                util.SMlog("on-slave.deactivate failed")
                raise
        elif action == "deactivateNoRefcount":
            try:
                lvmCache.deactivateNoRefcount(args["lvName%d" % i])
            except util.SMException:
                util.SMlog("on-slave.deactivateNoRefcount failed")
                raise
        elif action == "refresh":
            try:
                lvmCache.activateNoRefcount(args["lvName%d" % i], True)
            except util.CommandException:
                util.SMlog("on-slave.refresh failed")
                raise
        elif action == "cleanupLockAndRefcount":
            from sm.refcounter import RefCounter
            lock.Lock.cleanup(args["uuid%d" % i], args["ns%d" % i])
            RefCounter.reset(args["uuid%d" % i], args["ns%d" % i])
        else:
            raise util.SMException("unrecognized action: %s" % action)
        i += 1
    return str(True)


def _is_open(session, args):
    """Check if VDI <args["vdiUuid"]> is open by a tapdisk on this host"""
    import SRCommand
    import SR
    import NFSSR
    import EXTSR
    import LVHDSR
    import blktap2

    util.SMlog("on-slave.is_open: %s" % args)
    vdiUuid = args["vdiUuid"]
    srRef = args["srRef"]
    srRec = session.xenapi.SR.get_record(srRef)
    srType = srRec["type"]

    # FIXME: ugly hacks to create a VDI object without a real SRCommand to
    # avoid having to refactor the core files
    if srType.startswith("lvm"):
        srType = "lvhd"
    cmd = SRCommand.SRCommand(None)
    cmd.driver_info = {"capabilities": None}
    cmd.dconf = {"server": None, "device": "/HACK"}
    cmd.params = {"command": None}

    driver = SR.driver(srType)
    sr = driver(cmd, srRec["uuid"])
    vdi = sr.vdi(vdiUuid)
    tapdisk = blktap2.Tapdisk.find_by_path(vdi.path)
    util.SMlog("Tapdisk for %s: %s" % (vdi.path, tapdisk))
    if tapdisk:
        return "True"
    return "False"


def is_open(session, args):
    try:
        return _is_open(session, args)
    except:
        util.logException("is_open")
        raise


def refresh_lun_size_by_SCSIid(session, args):
    """Refresh the size of LUNs backing the SCSIid on the local node."""
    util.SMlog("on-slave.refresh_lun_size_by_SCSIid(,%s)" % args)
    if scsiutil.refresh_lun_size_by_SCSIid(args['SCSIid']):
        util.SMlog("on-slave.refresh_lun_size_by_SCSIid with %s succeeded"
                   % args)
        return "True"
    else:
        util.SMlog("on-slave.refresh_lun_size_by_SCSIid with %s failed" % args)
        return "False"


if __name__ == "__main__":
    import XenAPIPlugin
    XenAPIPlugin.dispatch({
        "multi": multi,
        "is_open": is_open,
        "refresh_lun_size_by_SCSIid": refresh_lun_size_by_SCSIid})
