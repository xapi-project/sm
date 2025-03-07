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

from sm.core import util
from sm.core import xs_errors
from sm.core import iscsi as iscsilib
from sm.core import mpath_cli
import os
import glob
import time
from sm.core import scsiutil
from sm.core import wwid_conf
import errno
from fairlock import Fairlock

DMPBIN = "/sbin/multipath"
DEVMAPPERPATH = "/dev/mapper"
DEVBYIDPATH = "/dev/disk/by-id"
DEVBYSCSIPATH = "/dev/disk/by-scsibus"
SYSFS_PATH = '/sys/class/scsi_host'
MP_INUSEDIR = "/dev/disk/mpInuse"


def _is_mpath_daemon_running():
    cmd = ["/usr/bin/systemctl", "is-active", "multipathd"]
    rc, stdout, stderr = util.doexec(cmd)
    return rc == 0


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
    util.set_scheduler(os.path.realpath(path))


def deactivate_MPdev(sid):
    path = os.path.join(MP_INUSEDIR, sid)
    if os.path.exists(path):
        os.unlink(path)

# The explicit_unmap argument used to default to False, but since every caller
# either needs it to be True or explicitly specifies, changed it to True by
# default.
def reset(sid, explicit_unmap=True):
    util.SMlog("Resetting LUN %s" % sid)
    _resetDMP(sid, explicit_unmap)


def _resetDMP(sid, explicit_unmap=False):
# If mpath has been turned on since the sr/vdi was attached, we
# might be trying to unmap it before the daemon has been started
# This is unnecessary (and will fail) so just return.
    deactivate_MPdev(sid)
    if not _is_mpath_daemon_running():
        util.SMlog("Warning: Trying to unmap mpath device when multipathd not running")
        return

    if explicit_unmap:
        util.SMlog("_resetDMP, explicit unmap")
        with Fairlock("devicemapper"):
            util.SMlog(f"removing {sid} from multipath and wwids")
            util.retry(
                lambda: util.pread2(['/usr/sbin/multipath', '-w', sid]),
                maxretry=3, period=4)
            util.retry(lambda: util.pread2(['/usr/sbin/multipath', '-f', sid]),
                       maxretry=3, period=4)
            util.retry(lambda: util.pread2(['/usr/sbin/multipath', '-W']),
                       maxretry=3, period=4)
    else:
        mpath_cli.ensure_map_gone(sid)

    path = "/dev/mapper/%s" % sid

    if not util.wait_for_nopath(path, 10):
        util.SMlog("MPATH: WARNING - path did not disappear [%s]" % path)
    else:
        util.SMlog("MPATH: path disappeared [%s]" % path)


def refresh(sid, npaths=0):
    """
    Refresh the multipath status

    The npaths argument is defaulted to 0 because it is never actually used
    (_refresh_DMP() doesn't use it either) and this makes the call compatible
    with sm-core-libs, which doesn't supply the argument.
    """

    util.SMlog("Refreshing LUN %s" % sid)
    if len(sid):
        path = DEVBYIDPATH + "/scsi-" + sid
        if not os.path.exists(path):
            scsiutil.rescan(scsiutil._genHostList(""))
            if not util.wait_for_path(path, 60):
                raise xs_errors.XenError('MultipathDeviceNotAppeared', path)
        _refresh_DMP(sid, npaths)
    else:
        raise xs_errors.XenError('MultipathDeviceNoScsiid')


def _is_valid_multipath_device(sid):

    # Check if device is already multipathed
    with Fairlock("devicemapper"):
        (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath', '-l', sid])
    if not stdout + stderr:
        with Fairlock("devicemapper"):
            (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath', '-ll', sid])
    if not stdout + stderr:
        with Fairlock("devicemapper"):
            (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath', '-a', sid])
        if ret < 0:
            util.SMlog("Failed to add {}: wwid could be explicitly "
                    "blacklisted\n Continue with multipath disabled for "
                    "this SR".format(sid))
            return False

        by_scsid_path = "/dev/disk/by-scsid/" + sid
        if os.path.exists(by_scsid_path):
            devs = os.listdir(by_scsid_path)
        else:
            util.SMlog("Device {} is not ready yet, skipping multipath check".format(by_scsid_path))
            return False
        ret = 1
        # Some paths might be down, check all associated devices
        for dev in devs:
            devpath = os.path.join(by_scsid_path, dev)
            real_path = os.path.realpath(devpath)
            with Fairlock("devicemapper"):
                (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath', '-c', real_path])
                if ret == 0:
                    break

        if ret == 1:
            # This is very fragile but it is not a good sign to fail without
            # any output. At least until multipath 0.4.9, for example,
            # multipath -c fails without any log if it is able to retrieve the
            # wwid of the device.
            # In this case it is better to fail immediately.
            if not stdout + stderr:
                # Attempt to cleanup wwids file before raising
                try:
                    with Fairlock("devicemapper"):
                        (ret, stdout, stderr) = util.doexec(['/usr/sbin/multipath', '-w', sid])
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
    path = os.path.join(DEVMAPPERPATH, sid)
    # If the mapper path doesn't exist force a reload in multipath
    if not os.path.exists(path):
        with Fairlock("devicemapper"):
            util.retry(lambda: util.pread2(['/usr/sbin/multipath', '-r', sid]), maxretry=3, period=4)
        util.wait_for_path(path, 30)
    if not os.path.exists(path):
        raise xs_errors.XenError('MultipathMapperPathMissing',
                                 'Device mapper path {} not found'.format(
                                     path))
    lvm_path = "/dev/disk/by-scsid/" + sid + "/mapper"
    util.wait_for_path(lvm_path, 30)
    activate_MPdev(sid, path)


def activate():
    util.SMlog("MPATH: multipath activate called")

    # If we've got no active sessions, and the deamon is already running,
    # we're ok to restart the daemon
    if iscsilib.is_iscsi_daemon_running():
        if not iscsilib._checkAnyTGT():
            iscsilib.restart_daemon()

    if not _is_mpath_daemon_running():
        util.SMlog("Warning: multipath daemon not running.  Starting daemon!")
        cmd = ["/usr/bin/systemctl", "start", "multipathd.service"]
        util.pread2(cmd)

    for i in range(0, 120):
        if mpath_cli.is_working():
            util.SMlog("MPATH: dm-multipath activated.")
            return
        time.sleep(1)

    util.SMlog("Failed to communicate with the multipath daemon!")
    raise xs_errors.XenError('MultipathdCommsFailure')


def deactivate():
    util.SMlog("MPATH: multipath deactivate called")

    if _is_mpath_daemon_running():
        # Flush the multipath nodes
        for sid in mpath_cli.list_maps():
            reset(sid, True)

    # Check the ISCSI daemon doesn't have any active sessions, if not,
    # restart in the new mode
    if iscsilib.is_iscsi_daemon_running() and not iscsilib._checkAnyTGT():
        iscsilib.restart_daemon()

    util.kickpipe_mpathcount()
    util.SMlog("MPATH: multipath deactivated.")


def path(SCSIid):
    if _is_valid_multipath_device(SCSIid) and _is_mpath_daemon_running():
        path = os.path.join(MP_INUSEDIR, SCSIid)
        return path
    else:
        return DEVBYIDPATH + "/scsi-" + SCSIid
