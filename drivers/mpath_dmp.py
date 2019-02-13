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

import util
import xs_errors
import iscsilib
import mpath_cli
import os
import glob
import time
import scsiutil
import wwid_conf
import errno

iscsi_mpath_file = "/etc/iscsi/iscsid-mpath.conf"
iscsi_default_file = "/etc/iscsi/iscsid-default.conf"
iscsi_file = "/etc/iscsi/iscsid.conf"

DMPBIN = "/sbin/multipath"
DEVMAPPERPATH = "/dev/mapper"
DEVBYIDPATH = "/dev/disk/by-id"
DEVBYSCSIPATH = "/dev/disk/by-scsibus"
DEVBYMPPPATH = "/dev/disk/by-mpp"
SYSFS_PATH='/sys/class/scsi_host'
MP_INUSEDIR = "/dev/disk/mpInuse"

MPPGETAIDLNOBIN = "/opt/xensource/bin/xe-get-arrayid-lunnum"

def _is_mpath_daemon_running():
    cmd = ["/sbin/pidof", "-s", "/sbin/multipathd"]
    (rc,stdout,stderr) = util.doexec(cmd)
    return (rc==0)

def activate_MPdev(sid, dst):
    try:
        os.mkdir(MP_INUSEDIR)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise
    path = os.path.join(MP_INUSEDIR, sid)
    cmd = ['ln', '-sf', dst, path]
    util.pread2(cmd)

def deactivate_MPdev(sid):
    path = os.path.join(MP_INUSEDIR, sid)
    if os.path.exists(path):
        os.unlink(path)
        
def reset(sid,explicit_unmap=False,delete_nodes=False):
    util.SMlog("Resetting LUN %s" % sid)
    _resetDMP(sid,explicit_unmap,delete_nodes)

def _delete_node(dev):
    try:
        path = '/sys/block/' + dev + '/device/delete'
        f = os.open(path, os.O_WRONLY)
        os.write(f,'1')
        os.close(f)
    except:
        util.SMlog("Failed to delete %s" % dev)
    
def _resetDMP(sid,explicit_unmap=False,delete_nodes=False):
# If mpath has been turned on since the sr/vdi was attached, we
# might be trying to unmap it before the daemon has been started
# This is unnecessary (and will fail) so just return.
    deactivate_MPdev(sid)
    if not _is_mpath_daemon_running():
        util.SMlog("Warning: Trying to unmap mpath device when multipathd not running")
        return

# If the multipath daemon is running, but we were initially plugged
# with multipathing set to no, there may be no map for us in the multipath
# tables. In that case, list_paths will return [], but remove_map might
# throw an exception. Catch it and ignore it.
    if explicit_unmap:
        util.retry(lambda: util.pread2(['/usr/sbin/multipath', '-f', sid]),
                   maxretry = 3, period = 4)
        util.retry(lambda: util.pread2(['/usr/sbin/multipath', '-W']), maxretry = 3,
                   period = 4)
    else:
        mpath_cli.ensure_map_gone(sid)

    path = "/dev/mapper/%s" % sid
    
    if not util.wait_for_nopath(path, 10):
        util.SMlog("MPATH: WARNING - path did not disappear [%s]" % path)
    else:
        util.SMlog("MPATH: path disappeared [%s]" % path)

# expecting e.g. ["/dev/sda","/dev/sdb"] or ["/dev/disk/by-scsibus/...whatever" (links to the real devices)]
def __map_explicit(devices):
    for device in devices:
        realpath = os.path.realpath(device)
        base = os.path.basename(realpath)
        util.SMlog("Adding mpath path '%s'" % base)
        try:
            mpath_cli.add_path(base)
        except:
            util.SMlog("WARNING: exception raised while attempting to add path %s" % base)

def map_by_scsibus(sid,npaths=0):
    # Synchronously creates/refreshs the MP map for a single SCSIid.
    # Gathers the device vector from /dev/disk/by-scsibus - we expect
    # there to be 'npaths' paths

    util.SMlog("map_by_scsibus: sid=%s" % sid)

    devices = []

    # Wait for up to 60 seconds for n devices to appear
    for attempt in range(0,60):
        devices = scsiutil._genReverseSCSIidmap(sid)

        # If we've got the right number of paths, or we don't know
        # how many devices there ought to be, tell multipathd about
        # the paths, and return.
        if(len(devices)>=npaths or npaths==0):
            # Enable this device's sid: it could be blacklisted
            # We expect devices to be blacklisted according to their
            # wwid only. We go through the list of paths until we have
            # a definite answer about the device's blacklist status.
            # If the path we are checking is down, we cannot tell.
            for dev in devices:
                try:
                    if wwid_conf.is_blacklisted(dev):
                        try:
                            wwid_conf.edit_wwid(sid)
                        except:
                            util.SMlog("WARNING: exception raised while "
                                       "attempting to modify multipath.conf")
                        try:
                            mpath_cli.reconfigure()
                        except:
                            util.SMlog("WARNING: exception raised while "
                                       "attempting to reconfigure")
                        time.sleep(5)

                    break
                except wwid_conf.WWIDException as e:
                    util.SMlog(e.errstr)
            else:
                util.SMlog("Device 'SCSI_id: {}' is inaccessible; "
                           "All paths are down.".format(sid))

            __map_explicit(devices)
            return

        time.sleep(1)

    __map_explicit(devices)
    
def refresh(sid,npaths):
    # Refresh the multipath status
    util.SMlog("Refreshing LUN %s" % sid)
    if len(sid):
        path = DEVBYIDPATH + "/scsi-" + sid
        if not os.path.exists(path):
            scsiutil.rescan(scsiutil._genHostList(""))
            if not util.wait_for_path(path,60):
                raise xs_errors.XenError('Device not appeared yet')
        _refresh_DMP(sid,npaths)
    else:
        raise xs_errors.XenError('MPath not written yet')


def _is_valid_multipath_device(sid):

    # Check if device is already multipathed
    (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath', '-ll', sid])
    if not stdout+stderr:
        (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath', '-a', sid])
        if ret < 0:
            util.SMlog("Failed to add {}: wwid could be explicitly "
                       "blacklisted\n Continue with multipath disabled for "
                       "this SR".format(sid))
            return False

        by_scsid_path = "/dev/disk/by-scsid/"+sid
        if os.path.exists(by_scsid_path):
            devs = os.listdir(by_scsid_path)
        else:
            util.SMlog("Device {} is not ready yet, skipping multipath check"
                       .format(by_scsid_path))
            return False
        ret = 1
        # Some paths might be down, check all associated devices
        for dev in devs:
            devpath = os.path.join(by_scsid_path, dev)
            real_path = util.get_real_path(devpath)
            (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath', '-c',
                                                 real_path])
            if ret == 0:
                break

        if ret == 1:
            # This is very fragile but it is not a good sign to fail without
            # any output. At least until multipath 0.4.9, for example,
            # multipath -c fails without any log if it is able to retrieve the
            # wwid of the device.
            # In this case it is better to fail immediately.
            if not stdout+stderr:
                # Attempt to cleanup wwids file before raising
                try:
                    (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath',
                                                         '-w', sid])
                except OSError:
                    util.SMlog("Error removing {} from wwids file".format(sid))
                raise xs_errors.XenError('MultipathGenericFailure',
                                         '"multipath -c" failed without any'
                                         ' output on {}'.format(real_path))
            util.SMlog("When dealing with {} multipath status returned:\n "
                       "{}{} Continue with multipath disabled for this SR"
                       .format(sid, stdout, stderr))
            return False
    return True


def _refresh_DMP(sid, npaths):
    if not _is_valid_multipath_device(sid):
        return
    util.retry(lambda: util.pread2(['/usr/sbin/multipath', '-r', sid]), maxretry = 3,
                           period = 4)
    path = os.path.join(DEVMAPPERPATH, sid)
    util.wait_for_path(path, 10)
    if not os.path.exists(path):
        raise xs_errors.XenError('DMP failed to activate mapper path')
    lvm_path = "/dev/disk/by-scsid/"+sid+"/mapper"
    util.wait_for_path(lvm_path, 10)
    activate_MPdev(sid, path)

def activate():
    util.SMlog("MPATH: multipath activate called")
    cmd = ['ln', '-sf', iscsi_mpath_file, iscsi_file]
    try:
        if os.path.exists(iscsi_mpath_file):
            # Only do this if using our customized open-iscsi package
            util.pread2(cmd)
    except util.CommandException, ce:
        if not ce.reason.endswith(': File exists'):
            raise

    # If we've got no active sessions, and the deamon is already running,
    # we're ok to restart the daemon
    if iscsilib.is_iscsi_daemon_running():
        if not iscsilib._checkAnyTGT():
            iscsilib.restart_daemon()

    if not _is_mpath_daemon_running():
        util.SMlog("Warning: multipath daemon not running.  Starting daemon!")
        cmd = ["service", "multipathd", "start"]
        util.pread2(cmd)

    for i in range(0,120):
        if mpath_cli.is_working():
            util.SMlog("MPATH: dm-multipath activated.")
            return
        time.sleep(1)

    util.SMlog("Failed to communicate with the multipath daemon!")
    raise xs_errors.XenError('MultipathdCommsFailure')    

def deactivate():
    util.SMlog("MPATH: multipath deactivate called")
    cmd = ['ln', '-sf', iscsi_default_file, iscsi_file]
    if os.path.exists(iscsi_default_file):
        # Only do this if using our customized open-iscsi package
        util.pread2(cmd)

    if _is_mpath_daemon_running():
        # Flush the multipath nodes
        for sid in mpath_cli.list_maps():
            reset(sid,True)
        
    # Disable any active MPP LUN maps (except the root dev)
    systemroot = os.path.realpath(util.getrootdev())
    for dev in glob.glob(DEVBYMPPPATH + "/*"):
        if os.path.realpath(dev) != systemroot:
            sid = os.path.basename(dev).split('-')[0]
            reset(sid)
        else:
            util.SMlog("MPP: Found root dev node, not resetting")

    # Check the ISCSI daemon doesn't have any active sessions, if not,
    # restart in the new mode
    if iscsilib.is_iscsi_daemon_running() and not iscsilib._checkAnyTGT():
        iscsilib.restart_daemon()
        
    util.SMlog("MPATH: multipath deactivated.")

def path(SCSIid):
    if _is_valid_multipath_device(SCSIid) and _is_mpath_daemon_running():
        path = os.path.join(MP_INUSEDIR, SCSIid)
        return path
    else:
        return DEVBYIDPATH + "/scsi-" + SCSIid

def status(SCSIid):
    pass

def get_TargetID_LunNUM(SCSIid):
    devices = scsiutil._genReverseSCSIidmap(SCSIid)
    cmd = [MPPGETAIDLNOBIN, devices[0]]
    return util.pread2(cmd).split('\n')[0]
