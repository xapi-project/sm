#!/usr/bin/env python
#
# Original work copyright (C) Citrix systems
# Modified work copyright (C) Vates SAS and XCP-ng community
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

import errno
import os
import syslog as _syslog
import xmlrpclib
from syslog import syslog

# careful with the import order here
# FileSR has a circular dependency: FileSR- > blktap2 -> lvutil -> EXTSR -> FileSR
# importing in this order seems to avoid triggering the issue.
import SR
import SRCommand
import FileSR
# end of careful
import cleanup
import util
import vhdutil
import xs_errors
from lock import Lock

CAPABILITIES = ["SR_PROBE", "SR_UPDATE", "SR_CACHING",
                "VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH",
                "VDI_UPDATE", "VDI_CLONE", "VDI_SNAPSHOT", "VDI_RESIZE", "VDI_MIRROR",
                "VDI_GENERATE_CONFIG",
                "VDI_RESET_ON_BOOT/2", "ATOMIC_PAUSE"]

CONFIGURATION = [['server', 'Full path to share on gluster server (required, ex: "192.168.0.12:/gv0")'],
                 ['backupservers', 'list of servers separated by ":"'],
                 ['fetchattempts', 'number of attempts to fetch files before switching to the backup server']
                 ]

DRIVER_INFO = {
    'name': 'GlusterFS VHD',
    'description': 'SR plugin which stores disks as VHD files on a GlusterFS storage',
    'vendor': 'Vates SAS',
    'copyright': '(C) 2020 Vates SAS',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
}

DRIVER_CONFIG = {"ATTACH_FROM_CONFIG_WITH_TAPDISK": True}

# The mountpoint for the directory when performing an sr_probe.  All probes
# are guaranteed to be serialised by xapi, so this single mountpoint is fine.
PROBE_MOUNTPOINT = os.path.join(SR.MOUNT_BASE, "probe")


class GlusterFSException(Exception):
    def __init__(self, errstr):
        self.errstr = errstr


# mountpoint = /var/run/sr-mount/GlusterFS/<glusterfs_server_name>/uuid
# linkpath = mountpoint/uuid - path to SR directory on share
# path = /var/run/sr-mount/uuid - symlink to SR directory on share
class GlusterFSSR(FileSR.FileSR):
    """Gluster file-based storage repository"""

    DRIVER_TYPE = 'glusterfs'

    def handles(sr_type):
        # fudge, because the parent class (FileSR) checks for smb to alter its behavior
        return sr_type == GlusterFSSR.DRIVER_TYPE or sr_type == 'smb'

    handles = staticmethod(handles)

    def load(self, sr_uuid):
        if not self._is_glusterfs_available():
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='glusterfs is not installed'
            )

        self.ops_exclusive = FileSR.OPS_EXCLUSIVE
        self.lock = Lock(vhdutil.LOCK_TYPE_SR, self.uuid)
        self.sr_vditype = SR.DEFAULT_TAP
        self.driver_config = DRIVER_CONFIG
        if 'server' not in self.dconf:
            raise xs_errors.XenError('ConfigServerMissing')
        self.remoteserver = self.dconf['server']
        if self.sr_ref and self.session is not None:
            self.sm_config = self.session.xenapi.SR.get_sm_config(self.sr_ref)
        else:
            self.sm_config = self.srcmd.params.get('sr_sm_config') or {}
        self.mountpoint = os.path.join(SR.MOUNT_BASE, 'GlusterFS', self.remoteserver.split(':')[0], sr_uuid)
        self.linkpath = os.path.join(self.mountpoint, sr_uuid or "")
        self.path = os.path.join(SR.MOUNT_BASE, sr_uuid)
        self._check_o_direct()

    def checkmount(self):
        return util.ioretry(lambda: ((util.pathexists(self.mountpoint) and
                                      util.ismount(self.mountpoint)) and
                                     util.pathexists(self.linkpath)))

    def mount(self, mountpoint=None):
        """Mount the remote gluster export at 'mountpoint'"""
        if mountpoint is None:
            mountpoint = self.mountpoint
        elif not util.is_string(mountpoint) or mountpoint == "":
            raise GlusterFSException("mountpoint not a string object")

        try:
            if not util.ioretry(lambda: util.isdir(mountpoint)):
                util.ioretry(lambda: util.makedirs(mountpoint))
        except util.CommandException, inst:
            raise GlusterFSException("Failed to make directory: code is %d" % inst.code)
        try:
            options = []
            if 'backupservers' in self.dconf:
                options.append('backup-volfile-servers=' + self.dconf['backupservers'])
            if 'fetchattempts' in self.dconf:
                options.append('fetch-attempts=' + self.dconf['fetchattempts'])
            if options:
                options = ['-o', ','.join(options)]
            command = ["mount", '-t', 'glusterfs', self.remoteserver, mountpoint] + options
            util.ioretry(lambda: util.pread(command), errlist=[errno.EPIPE, errno.EIO], maxretry=2, nofail=True)
        except util.CommandException, inst:
            syslog(_syslog.LOG_ERR, 'GlusterFS mount failed ' + inst.__str__())
            raise GlusterFSException("mount failed with return code %d" % inst.code)

        # Sanity check to ensure that the user has at least RO access to the
        # mounted share. Windows sharing and security settings can be tricky.
        try:
            util.listdir(mountpoint)
        except util.CommandException:
            try:
                self.unmount(mountpoint, True)
            except GlusterFSException:
                util.logException('GlusterFSSR.unmount()')
            raise GlusterFSException("Permission denied. Please check user privileges.")

    def unmount(self, mountpoint, rmmountpoint):
        try:
            util.pread(["umount", mountpoint])
        except util.CommandException, inst:
            raise GlusterFSException("umount failed with return code %d" % inst.code)
        if rmmountpoint:
            try:
                os.rmdir(mountpoint)
            except OSError, inst:
                raise GlusterFSException("rmdir failed with error '%s'" % inst.strerror)

    def attach(self, sr_uuid):
        if not self.checkmount():
            try:
                self.mount()
                os.symlink(self.linkpath, self.path)
            except GlusterFSException, exc:
                raise SR.SROSError(12, exc.errstr)
        self.attached = True

    def probe(self):
        try:
            self.mount(PROBE_MOUNTPOINT)
            sr_list = filter(util.match_uuid, util.listdir(PROBE_MOUNTPOINT))
            self.unmount(PROBE_MOUNTPOINT, True)
        except (util.CommandException, xs_errors.XenError):
            raise
        # Create a dictionary from the SR uuids to feed SRtoXML()
        sr_dict = {sr_uuid: {} for sr_uuid in sr_list}
        return util.SRtoXML(sr_dict)

    def detach(self, sr_uuid):
        if not self.checkmount():
            return
        util.SMlog("Aborting GC/coalesce")
        cleanup.abort(self.uuid)
        # Change directory to avoid unmount conflicts
        os.chdir(SR.MOUNT_BASE)
        self.unmount(self.mountpoint, True)
        os.unlink(self.path)
        self.attached = False

    def create(self, sr_uuid, size):
        if self.checkmount():
            raise SR.SROSError(113, 'GlusterFS mount point already attached')

        try:
            self.mount()
        except GlusterFSException, exc:
            # noinspection PyBroadException
            try:
                os.rmdir(self.mountpoint)
            except:
                # we have no recovery strategy
                pass
            raise SR.SROSError(111, "GlusterFS mount error [opterr=%s]" % exc.errstr)

        if util.ioretry(lambda: util.pathexists(self.linkpath)):
            if len(util.ioretry(lambda: util.listdir(self.linkpath))) != 0:
                self.detach(sr_uuid)
                raise xs_errors.XenError('SRExists')
        else:
            try:
                util.ioretry(lambda: util.makedirs(self.linkpath))
                os.symlink(self.linkpath, self.path)
            except util.CommandException, inst:
                if inst.code != errno.EEXIST:
                    try:
                        self.unmount(self.mountpoint, True)
                    except GlusterFSException:
                        util.logException('GlusterFSSR.unmount()')
                    raise SR.SROSError(116,
                                       "Failed to create GlusterFS SR. remote directory creation error: {}".format(
                                           os.strerror(inst.code)))
        self.detach(sr_uuid)

    def delete(self, sr_uuid):
        # try to remove/delete non VDI contents first
        super(GlusterFSSR, self).delete(sr_uuid)
        try:
            if self.checkmount():
                self.detach(sr_uuid)
            self.mount()
            if util.ioretry(lambda: util.pathexists(self.linkpath)):
                util.ioretry(lambda: os.rmdir(self.linkpath))
            self.unmount(self.mountpoint, True)
        except util.CommandException, inst:
            self.detach(sr_uuid)
            if inst.code != errno.ENOENT:
                raise SR.SROSError(114, "Failed to remove GlusterFS mount point")

    def vdi(self, uuid, loadLocked=False):
        return GlusterFSFileVDI(self, uuid)

    @staticmethod
    def _is_glusterfs_available():
        import distutils.spawn
        return distutils.spawn.find_executable('glusterfs')


class GlusterFSFileVDI(FileSR.FileVDI):
    def attach(self, sr_uuid, vdi_uuid):
        if not hasattr(self, 'xenstore_data'):
            self.xenstore_data = {}

        self.xenstore_data['storage-type'] = GlusterFSSR.DRIVER_TYPE

        return super(GlusterFSFileVDI, self).attach(sr_uuid, vdi_uuid)

    def generate_config(self, sr_uuid, vdi_uuid):
        util.SMlog("SMBFileVDI.generate_config")
        if not util.pathexists(self.path):
            raise xs_errors.XenError('VDIUnavailable')
        resp = {'device_config': self.sr.dconf,
                'sr_uuid': sr_uuid,
                'vdi_uuid': vdi_uuid,
                'sr_sm_config': self.sr.sm_config,
                'command': 'vdi_attach_from_config'}
        # Return the 'config' encoded within a normal XMLRPC response so that
        # we can use the regular response/error parsing code.
        config = xmlrpclib.dumps(tuple([resp]), "vdi_attach_from_config")
        return xmlrpclib.dumps((config,), "", True)

    def attach_from_config(self, sr_uuid, vdi_uuid):
        try:
            if not util.pathexists(self.sr.path):
                self.sr.attach(sr_uuid)
        except:
            util.logException("SMBFileVDI.attach_from_config")
            raise xs_errors.XenError('SRUnavailable',
                                     opterr='Unable to attach from config')


if __name__ == '__main__':
    SRCommand.run(GlusterFSSR, DRIVER_INFO)
else:
    SR.registerSR(GlusterFSSR)
