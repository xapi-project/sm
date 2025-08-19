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
# FileSR: local-file storage repository
#         matches with drivers/FileSR

import os
import errno
import time
import glob
from uuid import uuid4
import xmlrpc.client

import XenAPI # pylint: disable=import-error

from sm import SR
from sm import VDI
from sm import vhdutil
from sm import cleanup
from sm import blktap2
from sm.core import util
from sm.core import scsiutil
from sm.core import xs_errors
from sm.core.lock import Lock
from sm.constants import CBTLOG_TAG

geneology = {}
CAPABILITIES = ["SR_PROBE", "SR_UPDATE", \
                "VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH", \
                "VDI_CLONE", "VDI_SNAPSHOT", "VDI_RESIZE", "VDI_MIRROR",
                "VDI_GENERATE_CONFIG", "ATOMIC_PAUSE", "VDI_CONFIG_CBT",
                "VDI_ACTIVATE", "VDI_DEACTIVATE", "THIN_PROVISIONING"]

CONFIGURATION = [['location', 'local directory path (required)']]

DRIVER_INFO = {
    'name': 'Local Path VHD',
    'description': 'SR plugin which represents disks as VHD files stored on a local path',
    'vendor': 'Citrix Systems Inc',
    'copyright': '(C) 2008 Citrix Systems Inc',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
    }

JOURNAL_FILE_PREFIX = ".journal-"

OPS_EXCLUSIVE = [
        "sr_create", "sr_delete", "sr_probe", "sr_attach", "sr_detach",
        "sr_scan", "vdi_init", "vdi_create", "vdi_delete", "vdi_attach",
        "vdi_detach", "vdi_resize_online", "vdi_snapshot", "vdi_clone"]

DRIVER_CONFIG = {"ATTACH_FROM_CONFIG_WITH_TAPDISK": True}


class FileSR(SR.SR):
    """Local file storage repository"""

    SR_TYPE = "file"

    def handles(srtype):
        return srtype == 'file'
    handles = staticmethod(handles)

    def _check_o_direct(self):
        if self.sr_ref and self.session is not None:
            other_config = self.session.xenapi.SR.get_other_config(self.sr_ref)
            o_direct = other_config.get("o_direct")
            self.o_direct = o_direct is not None and o_direct == "true"
        else:
            self.o_direct = True

    def __init__(self, srcmd, sr_uuid):
        # We call SR.SR.__init__ explicitly because
        # "super" sometimes failed due to circular imports
        SR.SR.__init__(self, srcmd, sr_uuid)
        self._check_o_direct()

    def load(self, sr_uuid):
        self.ops_exclusive = OPS_EXCLUSIVE
        self.lock = Lock(vhdutil.LOCK_TYPE_SR, self.uuid)
        self.sr_vditype = vhdutil.VDI_TYPE_VHD
        if 'location' not in self.dconf or  not self.dconf['location']:
            raise xs_errors.XenError('ConfigLocationMissing')
        self.remotepath = self.dconf['location']
        self.path = os.path.join(SR.MOUNT_BASE, sr_uuid)
        self.linkpath = self.path
        self.mountpoint = self.path
        self.attached = False
        self.driver_config = DRIVER_CONFIG

    def create(self, sr_uuid, size):
        """ Create the SR.  The path must not already exist, or if it does, 
        it must be empty.  (This accounts for the case where the user has
        mounted a device onto a directory manually and want to use this as the
        root of a file-based SR.) """
        try:
            if util.ioretry(lambda: util.pathexists(self.remotepath)):
                if len(util.ioretry(lambda: util.listdir(self.remotepath))) != 0:
                    raise xs_errors.XenError('SRExists')
            else:
                try:
                    util.ioretry(lambda: os.mkdir(self.remotepath))
                except util.CommandException as inst:
                    if inst.code == errno.EEXIST:
                        raise xs_errors.XenError('SRExists')
                    else:
                        raise xs_errors.XenError('FileSRCreate', \
                              opterr='directory creation failure %d' \
                              % inst.code)
        except:
            raise xs_errors.XenError('FileSRCreate')

    def delete(self, sr_uuid):
        self.attach(sr_uuid)
        cleanup.gc_force(self.session, self.uuid)

        # check to make sure no VDIs are present; then remove old
        # files that are non VDI's
        try:
            if util.ioretry(lambda: util.pathexists(self.path)):
                #Load the VDI list
                self._loadvdis()
                for uuid in self.vdis:
                    if not self.vdis[uuid].deleted:
                        raise xs_errors.XenError('SRNotEmpty', \
                              opterr='VDIs still exist in SR')

                # remove everything else, there are no vdi's
                for name in util.ioretry(lambda: util.listdir(self.path)):
                    fullpath = os.path.join(self.path, name)
                    try:
                        util.ioretry(lambda: os.unlink(fullpath))
                    except util.CommandException as inst:
                        if inst.code != errno.ENOENT and \
                           inst.code != errno.EISDIR:
                            raise xs_errors.XenError('FileSRDelete', \
                                  opterr='failed to remove %s error %d' \
                                  % (fullpath, inst.code))
            self.detach(sr_uuid)
        except util.CommandException as inst:
            self.detach(sr_uuid)
            raise xs_errors.XenError('FileSRDelete', \
                  opterr='error %d' % inst.code)

    def attach(self, sr_uuid, bind=True):
        if not self._checkmount():
            try:
                util.ioretry(lambda: util.makedirs(self.path, mode=0o700))
            except util.CommandException as inst:
                if inst.code != errno.EEXIST:
                    raise xs_errors.XenError("FileSRCreate", \
                                             opterr='fail to create mount point. Errno is %s' % inst.code)
            try:
                cmd = ["mount", self.remotepath, self.path]
                if bind:
                    cmd.append("--bind")
                util.pread(cmd)
                os.chmod(self.path, mode=0o0700)
            except util.CommandException as inst:
                raise xs_errors.XenError('FileSRCreate', \
                                         opterr='fail to mount FileSR. Errno is %s' % inst.code)
        self.attached = True

    def detach(self, sr_uuid):
        if self._checkmount():
            try:
                util.SMlog("Aborting GC/coalesce")
                cleanup.abort(self.uuid)
                os.chdir(SR.MOUNT_BASE)
                util.pread(["umount", self.path])
                os.rmdir(self.path)
            except Exception as e:
                raise xs_errors.XenError('SRInUse', opterr=str(e))
        self.attached = False

    def scan(self, sr_uuid):
        if not self._checkmount():
            raise xs_errors.XenError('SRUnavailable', \
                  opterr='no such directory %s' % self.path)

        if not self.vdis:
            self._loadvdis()

        if not self.passthrough:
            self.physical_size = self._getsize()
            self.physical_utilisation = self._getutilisation()

        for uuid in list(self.vdis.keys()):
            if self.vdis[uuid].deleted:
                del self.vdis[uuid]

        # CA-15607: make sure we are robust to the directory being unmounted beneath
        # us (eg by a confused user). Without this we might forget all our VDI references
        # which would be a shame.
        # For SMB SRs, this path is mountpoint
        mount_path = self.path
        if self.handles("smb"):
            mount_path = self.mountpoint

        if not self.handles("file") and not os.path.ismount(mount_path):
            util.SMlog("Error: FileSR.scan called but directory %s isn't a mountpoint" % mount_path)
            raise xs_errors.XenError('SRUnavailable', \
                                     opterr='not mounted %s' % mount_path)

        self._kickGC()

        # default behaviour from here on
        super(FileSR, self).scan(sr_uuid)

    def update(self, sr_uuid):
        if not self._checkmount():
            raise xs_errors.XenError('SRUnavailable', \
                  opterr='no such directory %s' % self.path)
        self._update(sr_uuid, 0)

    def _update(self, sr_uuid, virt_alloc_delta):
        valloc = int(self.session.xenapi.SR.get_virtual_allocation(self.sr_ref))
        self.virtual_allocation = valloc + virt_alloc_delta
        self.physical_size = self._getsize()
        self.physical_utilisation = self._getutilisation()
        self._db_update()

    def content_type(self, sr_uuid):
        return super(FileSR, self).content_type(sr_uuid)

    def vdi(self, uuid):
        return FileVDI(self, uuid)

    def added_vdi(self, vdi):
        self.vdis[vdi.uuid] = vdi

    def deleted_vdi(self, uuid):
        if uuid in self.vdis:
            del self.vdis[uuid]

    def replay(self, uuid):
        try:
            file = open(self.path + "/filelog.txt", "r")
            data = file.readlines()
            file.close()
            self._process_replay(data)
        except:
            raise xs_errors.XenError('SRLog')

    def _loadvdis(self):
        if self.vdis:
            return

        pattern = os.path.join(self.path, "*%s" % vhdutil.FILE_EXTN_VHD)
        try:
            self.vhds = vhdutil.getAllVHDs(pattern, FileVDI.extractUuid)
        except util.CommandException as inst:
            raise xs_errors.XenError('SRScan', opterr="error VHD-scanning " \
                    "path %s (%s)" % (self.path, inst))
        try:
            list_vhds = [FileVDI.extractUuid(v) for v in util.ioretry(lambda: glob.glob(pattern))]
            if len(self.vhds) != len(list_vhds):
                util.SMlog("VHD scan returns %d VHDs: %s" % (len(self.vhds), sorted(self.vhds)))
                util.SMlog("VHD list returns %d VHDs: %s" % (len(list_vhds), sorted(list_vhds)))
        except:
            pass
        for uuid in self.vhds.keys():
            if self.vhds[uuid].error:
                raise xs_errors.XenError('SRScan', opterr='uuid=%s' % uuid)
            self.vdis[uuid] = self.vdi(uuid)
            # Get the key hash of any encrypted VDIs:
            vhd_path = os.path.join(self.path, self.vhds[uuid].path)
            key_hash = vhdutil.getKeyHash(vhd_path)
            self.vdis[uuid].sm_config_override['key_hash'] = key_hash

        # raw VDIs and CBT log files
        files = util.ioretry(lambda: util.listdir(self.path))
        for fn in files:
            if fn.endswith(vhdutil.FILE_EXTN_RAW):
                uuid = fn[:-(len(vhdutil.FILE_EXTN_RAW))]
                self.vdis[uuid] = self.vdi(uuid)
            elif fn.endswith(CBTLOG_TAG):
                cbt_uuid = fn.split(".")[0]
                # If an associated disk exists, update CBT status
                # else create new VDI of type cbt_metadata
                if cbt_uuid in self.vdis:
                    self.vdis[cbt_uuid].cbt_enabled = True
                else:
                    new_vdi = self.vdi(cbt_uuid)
                    new_vdi.ty = "cbt_metadata"
                    new_vdi.cbt_enabled = True
                    self.vdis[cbt_uuid] = new_vdi

        # Mark parent VDIs as Read-only and generate virtual allocation
        self.virtual_allocation = 0
        for uuid, vdi in self.vdis.items():
            if vdi.parent:
                if vdi.parent in self.vdis:
                    self.vdis[vdi.parent].read_only = True
                if vdi.parent in geneology:
                    geneology[vdi.parent].append(uuid)
                else:
                    geneology[vdi.parent] = [uuid]
            if not vdi.hidden:
                self.virtual_allocation += (vdi.size)

        # now remove all hidden leaf nodes from self.vdis so that they are not
        # introduced into the Agent DB when SR is synchronized. With the
        # asynchronous GC, a deleted VDI might stay around until the next
        # SR.scan, so if we don't ignore hidden leaves we would pick up
        # freshly-deleted VDIs as newly-added VDIs
        for uuid in list(self.vdis.keys()):
            if uuid not in geneology and self.vdis[uuid].hidden:
                util.SMlog("Scan found hidden leaf (%s), ignoring" % uuid)
                del self.vdis[uuid]

    def _getsize(self):
        path = self.path
        if self.handles("smb"):
            path = self.linkpath
        return util.get_fs_size(path)

    def _getutilisation(self):
        return util.get_fs_utilisation(self.path)

    def _replay(self, logentry):
        # all replay commands have the same 5,6,7th arguments
        # vdi_command, sr-uuid, vdi-uuid
        back_cmd = logentry[5].replace("vdi_", "")
        target = self.vdi(logentry[7])
        cmd = getattr(target, back_cmd)
        args = []
        for item in logentry[6:]:
            item = item.replace("\n", "")
            args.append(item)
        ret = cmd( * args)
        if ret:
            print(ret)

    def _compare_args(self, a, b):
        try:
            if a[2] != "log:":
                return 1
            if b[2] != "end:" and b[2] != "error:":
                return 1
            if a[3] != b[3]:
                return 1
            if a[4] != b[4]:
                return 1
            return 0
        except:
            return 1

    def _process_replay(self, data):
        logentries = []
        for logentry in data:
            logentry = logentry.split(" ")
            logentries.append(logentry)
        # we are looking for a log entry that has a log but no end or error
        # wkcfix -- recreate (adjusted) logfile
        index = 0
        while index < len(logentries) - 1:
            if self._compare_args(logentries[index], logentries[index + 1]):
                self._replay(logentries[index])
            else:
                # skip the paired one
                index += 1
            # next
            index += 1

    def _kickGC(self):
        util.SMlog("Kicking GC")
        cleanup.start_gc_service(self.uuid)

    def _isbind(self):
        # os.path.ismount can't deal with bind mount
        st1 = os.stat(self.path)
        st2 = os.stat(self.remotepath)
        return st1.st_dev == st2.st_dev and st1.st_ino == st2.st_ino

    def _checkmount(self):
        mount_path = self.path
        if self.handles("smb"):
            mount_path = self.mountpoint

        return util.ioretry(lambda: util.pathexists(mount_path) and \
                                (util.ismount(mount_path) or \
                                 util.pathexists(self.remotepath) and self._isbind()))

    # Override in SharedFileSR.
    def _check_hardlinks(self):
        return True

class FileVDI(VDI.VDI):
    PARAM_VHD = "vhd"
    PARAM_RAW = "raw"
    VDI_TYPE = {
            PARAM_VHD: vhdutil.VDI_TYPE_VHD,
            PARAM_RAW: vhdutil.VDI_TYPE_RAW
    }

    def _find_path_with_retries(self, vdi_uuid, maxretry=5, period=2.0):
        vhd_path = os.path.join(self.sr.path, "%s.%s" % \
                                (vdi_uuid, self.PARAM_VHD))
        raw_path = os.path.join(self.sr.path, "%s.%s" % \
                                (vdi_uuid, self.PARAM_RAW))
        cbt_path = os.path.join(self.sr.path, "%s.%s" %
                                (vdi_uuid, CBTLOG_TAG))
        found = False
        tries = 0
        while tries < maxretry and not found:
            tries += 1
            if util.ioretry(lambda: util.pathexists(vhd_path)):
                self.vdi_type = vhdutil.VDI_TYPE_VHD
                self.path = vhd_path
                found = True
            elif util.ioretry(lambda: util.pathexists(raw_path)):
                self.vdi_type = vhdutil.VDI_TYPE_RAW
                self.path = raw_path
                self.hidden = False
                found = True
            elif util.ioretry(lambda: util.pathexists(cbt_path)):
                self.vdi_type = CBTLOG_TAG
                self.path = cbt_path
                self.hidden = False
                found = True

            if not found:
                util.SMlog("VHD %s not found, retry %s of %s" % (vhd_path, tries, maxretry))
                time.sleep(period)

        return found

    def load(self, vdi_uuid):
        self.lock = self.sr.lock

        self.sr.srcmd.params['o_direct'] = self.sr.o_direct

        if self.sr.srcmd.cmd == "vdi_create":
            self.vdi_type = vhdutil.VDI_TYPE_VHD
            self.key_hash = None
            if "vdi_sm_config" in self.sr.srcmd.params:
                if "key_hash" in self.sr.srcmd.params["vdi_sm_config"]:
                    self.key_hash = self.sr.srcmd.params["vdi_sm_config"]["key_hash"]

                if "type" in self.sr.srcmd.params["vdi_sm_config"]:
                    vdi_type = self.sr.srcmd.params["vdi_sm_config"]["type"]
                    if not self.VDI_TYPE.get(vdi_type):
                        raise xs_errors.XenError('VDIType',
                                opterr='Invalid VDI type %s' % vdi_type)
                    self.vdi_type = self.VDI_TYPE[vdi_type]
            self.path = os.path.join(self.sr.path, "%s%s" %
                (vdi_uuid, vhdutil.FILE_EXTN[self.vdi_type]))
        else:
            found = self._find_path_with_retries(vdi_uuid)
            if not found:
                if self.sr.srcmd.cmd == "vdi_delete":
                    # Could be delete for CBT log file
                    self.path = os.path.join(self.sr.path, "%s.%s" %
                                             (vdi_uuid, self.PARAM_VHD))
                    return
                if self.sr.srcmd.cmd == "vdi_attach_from_config":
                    return
                raise xs_errors.XenError('VDIUnavailable',
                                         opterr="VDI %s not found" % vdi_uuid)


        if self.vdi_type == vhdutil.VDI_TYPE_VHD and \
                self.sr.__dict__.get("vhds") and self.sr.vhds.get(vdi_uuid):
            # VHD info already preloaded: use it instead of querying directly
            vhdInfo = self.sr.vhds[vdi_uuid]
            self.utilisation = vhdInfo.sizePhys
            self.size = vhdInfo.sizeVirt
            self.hidden = vhdInfo.hidden
            if self.hidden:
                self.managed = False
            self.parent = vhdInfo.parentUuid
            if self.parent:
                self.sm_config_override = {'vhd-parent': self.parent}
            else:
                self.sm_config_override = {'vhd-parent': None}
            return

        try:
            # Change to the SR directory in case parent
            # locator field path has changed
            os.chdir(self.sr.path)
        except Exception as chdir_exception:
            util.SMlog("Unable to change to SR directory, SR unavailable, %s" %
                       str(chdir_exception))
            raise xs_errors.XenError('SRUnavailable', opterr=str(chdir_exception))

        if util.ioretry(
                lambda: util.pathexists(self.path),
                errlist=[errno.EIO, errno.ENOENT]):
            try:
                st = util.ioretry(lambda: os.stat(self.path),
                                  errlist=[errno.EIO, errno.ENOENT])
                self.utilisation = int(st.st_size)
            except util.CommandException as inst:
                if inst.code == errno.EIO:
                    raise xs_errors.XenError('VDILoad', \
                          opterr='Failed load VDI information %s' % self.path)
                else:
                    util.SMlog("Stat failed for %s, %s" % (
                        self.path, str(inst)))
                    raise xs_errors.XenError('VDIType', \
                          opterr='Invalid VDI type %s' % self.vdi_type)

            if self.vdi_type == vhdutil.VDI_TYPE_RAW:
                self.exists = True
                self.size = self.utilisation
                self.sm_config_override = {'type': self.PARAM_RAW}
                return

            if self.vdi_type == CBTLOG_TAG:
                self.exists = True
                self.size = self.utilisation
                return

            try:
                # The VDI might be activated in R/W mode so the VHD footer
                # won't be valid, use the back-up one instead.
                diskinfo = util.ioretry(
                    lambda: self._query_info(self.path, True),
                    errlist=[errno.EIO, errno.ENOENT])

                if 'parent' in diskinfo:
                    self.parent = diskinfo['parent']
                    self.sm_config_override = {'vhd-parent': self.parent}
                else:
                    self.sm_config_override = {'vhd-parent': None}
                    self.parent = ''
                self.size = int(diskinfo['size']) * 1024 * 1024
                self.hidden = int(diskinfo['hidden'])
                if self.hidden:
                    self.managed = False
                self.exists = True
            except util.CommandException as inst:
                raise xs_errors.XenError('VDILoad', \
                      opterr='Failed load VDI information %s' % self.path)

    def update(self, sr_uuid, vdi_location):
        self.load(vdi_location)
        vdi_ref = self.sr.srcmd.params['vdi_ref']
        self.sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        self._db_update()

    def create(self, sr_uuid, vdi_uuid, size):
        if util.ioretry(lambda: util.pathexists(self.path)):
            raise xs_errors.XenError('VDIExists')

        if self.vdi_type == vhdutil.VDI_TYPE_VHD:
            try:
                size = vhdutil.validate_and_round_vhd_size(
                    int(size),
                    vhdutil.DEFAULT_VHD_BLOCK_SIZE
                )
                mb = 1024 * 1024
                size_mb = size // mb
                util.ioretry(lambda: self._create(str(size_mb), self.path))
                self.size = util.ioretry(lambda: self._query_v(self.path))
            except util.CommandException as inst:
                raise xs_errors.XenError('VDICreate',
                        opterr='error %d' % inst.code)
        else:
            f = open(self.path, 'w')
            f.truncate(int(size))
            f.close()
            self.size = size

        self.sr.added_vdi(self)

        st = util.ioretry(lambda: os.stat(self.path))
        self.utilisation = int(st.st_size)
        if self.vdi_type == vhdutil.VDI_TYPE_RAW:
            self.sm_config = {"type": self.PARAM_RAW}

        self._db_introduce()
        self.sr._update(self.sr.uuid, self.size)
        return super(FileVDI, self).get_params()

    def delete(self, sr_uuid, vdi_uuid, data_only=False):
        if not util.ioretry(lambda: util.pathexists(self.path)):
            return super(FileVDI, self).delete(sr_uuid, vdi_uuid, data_only)

        if self.attached:
            raise xs_errors.XenError('VDIInUse')

        try:
            util.force_unlink(self.path)
        except Exception as e:
            raise xs_errors.XenError(
                'VDIDelete',
                opterr='Failed to unlink file during deleting VDI: %s' % str(e))

        self.sr.deleted_vdi(vdi_uuid)
        # If this is a data_destroy call, don't remove from XAPI db
        if not data_only:
            self._db_forget()
        self.sr._update(self.sr.uuid, -self.size)
        self.sr.lock.cleanupAll(vdi_uuid)
        self.sr._kickGC()
        return super(FileVDI, self).delete(sr_uuid, vdi_uuid, data_only)

    def attach(self, sr_uuid, vdi_uuid):
        if self.path is None:
            self._find_path_with_retries(vdi_uuid)
        if not self._checkpath(self.path):
            raise xs_errors.XenError('VDIUnavailable', \
                  opterr='VDI %s unavailable %s' % (vdi_uuid, self.path))
        try:
            self.attached = True

            if not hasattr(self, 'xenstore_data'):
                self.xenstore_data = {}

            self.xenstore_data.update(scsiutil.update_XS_SCSIdata(vdi_uuid, \
                                                                      scsiutil.gen_synthetic_page_data(vdi_uuid)))

            if self.sr.handles("file"):
                # XXX: PR-1255: if these are constants then they should
                # be returned by the attach API call, not persisted in the
                # pool database.
                self.xenstore_data['storage-type'] = 'ext'
            return super(FileVDI, self).attach(sr_uuid, vdi_uuid)
        except util.CommandException as inst:
            raise xs_errors.XenError('VDILoad', opterr='error %d' % inst.code)

    def detach(self, sr_uuid, vdi_uuid):
        self.attached = False

    def resize(self, sr_uuid, vdi_uuid, size):
        if not self.exists:
            raise xs_errors.XenError('VDIUnavailable', \
                  opterr='VDI %s unavailable %s' % (vdi_uuid, self.path))

        if self.vdi_type != vhdutil.VDI_TYPE_VHD:
            raise xs_errors.XenError('Unimplemented')

        if self.hidden:
            raise xs_errors.XenError('VDIUnavailable', opterr='hidden VDI')

        if size < self.size:
            util.SMlog('vdi_resize: shrinking not supported: ' + \
                    '(current size: %d, new size: %d)' % (self.size, size))
            raise xs_errors.XenError('VDISize', opterr='shrinking not allowed')

        if size == self.size:
            return VDI.VDI.get_params(self)

        # We already checked it is a VDI_TYPE_VHD
        size = vhdutil.validate_and_round_vhd_size(int(size), self.block_size)
        
        jFile = JOURNAL_FILE_PREFIX + self.uuid
        try:
            vhdutil.setSizeVirt(self.path, size, jFile)
        except:
            # Revert the operation
            vhdutil.revert(self.path, jFile)
            raise xs_errors.XenError('VDISize', opterr='resize operation failed')

        old_size = self.size
        self.size = vhdutil.getSizeVirt(self.path)
        st = util.ioretry(lambda: os.stat(self.path))
        self.utilisation = int(st.st_size)

        self._db_update()
        self.sr._update(self.sr.uuid, self.size - old_size)
        super(FileVDI, self).resize_cbt(self.sr.uuid, self.uuid, self.size)
        return VDI.VDI.get_params(self)

    def clone(self, sr_uuid, vdi_uuid):
        return self._do_snapshot(sr_uuid, vdi_uuid, VDI.SNAPSHOT_DOUBLE)

    def compose(self, sr_uuid, vdi1, vdi2):
        if self.vdi_type != vhdutil.VDI_TYPE_VHD:
            raise xs_errors.XenError('Unimplemented')
        parent_fn = vdi1 + vhdutil.FILE_EXTN[vhdutil.VDI_TYPE_VHD]
        parent_path = os.path.join(self.sr.path, parent_fn)
        assert(util.pathexists(parent_path))
        vhdutil.setParent(self.path, parent_path, False)
        vhdutil.setHidden(parent_path)
        self.sr.session.xenapi.VDI.set_managed(self.sr.srcmd.params['args'][0], False)
        util.pread2([vhdutil.VHD_UTIL, "modify", "-p", parent_path,
            "-n", self.path])
        # Tell tapdisk the chain has changed
        if not blktap2.VDI.tap_refresh(self.session, sr_uuid, vdi2):
            raise util.SMException("failed to refresh VDI %s" % self.uuid)
        util.SMlog("VDI.compose: relinked %s->%s" % (vdi2, vdi1))

    def reset_leaf(self, sr_uuid, vdi_uuid):
        if self.vdi_type != vhdutil.VDI_TYPE_VHD:
            raise xs_errors.XenError('Unimplemented')

        # safety check
        if not vhdutil.hasParent(self.path):
            raise util.SMException("ERROR: VDI %s has no parent, " + \
                    "will not reset contents" % self.uuid)

        vhdutil.killData(self.path)

    def _do_snapshot(self, sr_uuid, vdi_uuid, snap_type,
                     _=False, secondary=None, cbtlog=None):
        # If cbt enabled, save file consistency state
        if cbtlog is not None:
            if blktap2.VDI.tap_status(self.session, vdi_uuid):
                consistency_state = False
            else:
                consistency_state = True
            util.SMlog("Saving log consistency state of %s for vdi: %s" %
                       (consistency_state, vdi_uuid))
        else:
            consistency_state = None

        if self.vdi_type != vhdutil.VDI_TYPE_VHD:
            raise xs_errors.XenError('Unimplemented')

        if not blktap2.VDI.tap_pause(self.session, sr_uuid, vdi_uuid):
            raise util.SMException("failed to pause VDI %s" % vdi_uuid)
        try:
            return self._snapshot(snap_type, cbtlog, consistency_state)
        finally:
            self.disable_leaf_on_secondary(vdi_uuid, secondary=secondary)
            blktap2.VDI.tap_unpause(self.session, sr_uuid, vdi_uuid, secondary)

    def _rename(self, src, dst):
        util.SMlog("FileVDI._rename %s to %s" % (src, dst))
        util.ioretry(lambda: os.rename(src, dst))

    def _link(self, src, dst):
        util.SMlog("FileVDI._link %s to %s" % (src, dst))
        os.link(src, dst)

    def _unlink(self, path):
        util.SMlog("FileVDI._unlink %s" % (path))
        os.unlink(path)

    def _create_new_parent(self, src, newsrc):
        if self.sr._check_hardlinks():
            self._link(src, newsrc)
        else:
            self._rename(src, newsrc)

    def __fist_enospace(self):
        raise util.CommandException(28, "vhd-util snapshot", reason="No space")

    def _snapshot(self, snap_type, cbtlog=None, cbt_consistency=None):
        util.SMlog("FileVDI._snapshot for %s (type %s)" % (self.uuid, snap_type))

        args = []
        args.append("vdi_clone")
        args.append(self.sr.uuid)
        args.append(self.uuid)

        dest = None
        dst = None
        if snap_type == VDI.SNAPSHOT_DOUBLE:
            dest = util.gen_uuid()
            dst = os.path.join(self.sr.path, "%s.%s" % (dest, self.vdi_type))
            args.append(dest)

        if self.hidden:
            raise xs_errors.XenError('VDIClone', opterr='hidden VDI')

        depth = vhdutil.getDepth(self.path)
        if depth == -1:
            raise xs_errors.XenError('VDIUnavailable', \
                  opterr='failed to get VHD depth')
        elif depth >= vhdutil.MAX_CHAIN_SIZE:
            raise xs_errors.XenError('SnapshotChainTooLong')

        newuuid = util.gen_uuid()
        src = self.path
        newsrc = os.path.join(self.sr.path, "%s.%s" % (newuuid, self.vdi_type))
        newsrcname = "%s.%s" % (newuuid, self.vdi_type)

        if not self._checkpath(src):
            raise xs_errors.XenError('VDIUnavailable', \
                  opterr='VDI %s unavailable %s' % (self.uuid, src))

        # wkcfix: multiphase
        util.start_log_entry(self.sr.path, self.path, args)

        # We assume the filehandle has been released
        try:
            self._create_new_parent(src, newsrc)

            # Create the snapshot under a temporary name, then rename
            # it afterwards. This avoids a small window where it exists
            # but is invalid. We do not need to do this for
            # snap_type == VDI.SNAPSHOT_DOUBLE because dst never existed
            # before so nobody will try to query it.
            tmpsrc = "%s.%s" % (src, "new")
            # Fault injection site to fail the snapshot with ENOSPACE
            util.fistpoint.activate_custom_fn(
                "FileSR_fail_snap1",
                self.__fist_enospace)
            util.ioretry(lambda: self._snap(tmpsrc, newsrcname))
            # SMB3 can return EACCES if we attempt to rename over the
            # hardlink leaf too quickly after creating it.
            util.ioretry(lambda: self._rename(tmpsrc, src),
                         errlist=[errno.EIO, errno.EACCES])
            if snap_type == VDI.SNAPSHOT_DOUBLE:
                # Fault injection site to fail the snapshot with ENOSPACE
                util.fistpoint.activate_custom_fn(
                    "FileSR_fail_snap2",
                    self.__fist_enospace)
                util.ioretry(lambda: self._snap(dst, newsrcname))
            # mark the original file (in this case, its newsrc)
            # as hidden so that it does not show up in subsequent scans
            util.ioretry(lambda: self._mark_hidden(newsrc))

            #Verify parent locator field of both children and delete newsrc if unused
            introduce_parent = True
            try:
                srcparent = util.ioretry(lambda: self._query_p_uuid(src))
                dstparent = None
                if snap_type == VDI.SNAPSHOT_DOUBLE:
                    dstparent = util.ioretry(lambda: self._query_p_uuid(dst))
                if srcparent != newuuid and \
                        (snap_type == VDI.SNAPSHOT_SINGLE or \
                        snap_type == VDI.SNAPSHOT_INTERNAL or \
                        dstparent != newuuid):
                    util.ioretry(lambda: self._unlink(newsrc))
                    introduce_parent = False
            except:
                pass

            # Introduce the new VDI records
            leaf_vdi = None
            if snap_type == VDI.SNAPSHOT_DOUBLE:
                leaf_vdi = VDI.VDI(self.sr, dest)  # user-visible leaf VDI
                leaf_vdi.read_only = False
                leaf_vdi.location = dest
                leaf_vdi.size = self.size
                leaf_vdi.utilisation = self.utilisation
                leaf_vdi.sm_config = {}
                leaf_vdi.sm_config['vhd-parent'] = dstparent
                # If the parent is encrypted set the key_hash
                # for the new snapshot disk
                vdi_ref = self.sr.srcmd.params['vdi_ref']
                sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
                if "key_hash" in sm_config:
                    leaf_vdi.sm_config['key_hash'] = sm_config['key_hash']
                # If we have CBT enabled on the VDI,
                # set CBT status for the new snapshot disk
                if cbtlog:
                    leaf_vdi.cbt_enabled = True

            base_vdi = None
            if introduce_parent:
                base_vdi = VDI.VDI(self.sr, newuuid)  # readonly parent
                base_vdi.label = "base copy"
                base_vdi.read_only = True
                base_vdi.location = newuuid
                base_vdi.size = self.size
                base_vdi.utilisation = self.utilisation
                base_vdi.sm_config = {}
                grandparent = util.ioretry(lambda: self._query_p_uuid(newsrc))
                if grandparent.find("no parent") == -1:
                    base_vdi.sm_config['vhd-parent'] = grandparent

            try:
                if snap_type == VDI.SNAPSHOT_DOUBLE:
                    leaf_vdi_ref = leaf_vdi._db_introduce()
                    util.SMlog("vdi_clone: introduced VDI: %s (%s)" % \
                            (leaf_vdi_ref, dest))

                if introduce_parent:
                    base_vdi_ref = base_vdi._db_introduce()
                    self.session.xenapi.VDI.set_managed(base_vdi_ref, False)
                    util.SMlog("vdi_clone: introduced VDI: %s (%s)" % (base_vdi_ref, newuuid))
                vdi_ref = self.sr.srcmd.params['vdi_ref']
                sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
                sm_config['vhd-parent'] = srcparent
                self.session.xenapi.VDI.set_sm_config(vdi_ref, sm_config)
            except Exception as e:
                util.SMlog("vdi_clone: caught error during VDI.db_introduce: %s" % (str(e)))
                # Note it's too late to actually clean stuff up here: the base disk has
                # been marked as deleted already.
                util.end_log_entry(self.sr.path, self.path, ["error"])
                raise
        except util.CommandException as inst:
            # XXX: it might be too late if the base disk has been marked as deleted!
            self._clonecleanup(src, dst, newsrc)
            util.end_log_entry(self.sr.path, self.path, ["error"])
            raise xs_errors.XenError('VDIClone',
                  opterr='VDI clone failed error %d' % inst.code)

        # Update cbt files if user created snapshot (SNAPSHOT_DOUBLE)
        if snap_type == VDI.SNAPSHOT_DOUBLE and cbtlog:
            try:
                self._cbt_snapshot(dest, cbt_consistency)
            except:
                # CBT operation failed.
                util.end_log_entry(self.sr.path, self.path, ["error"])
                raise

        util.end_log_entry(self.sr.path, self.path, ["done"])
        if snap_type != VDI.SNAPSHOT_INTERNAL:
            self.sr._update(self.sr.uuid, self.size)
        # Return info on the new user-visible leaf VDI
        ret_vdi = leaf_vdi
        if not ret_vdi:
            ret_vdi = base_vdi
        if not ret_vdi:
            ret_vdi = self
        return ret_vdi.get_params()

    def get_params(self):
        if not self._checkpath(self.path):
            raise xs_errors.XenError('VDIUnavailable', \
                  opterr='VDI %s unavailable %s' % (self.uuid, self.path))
        return super(FileVDI, self).get_params()

    def _snap(self, child, parent):
        cmd = [SR.TAPDISK_UTIL, "snapshot", vhdutil.VDI_TYPE_VHD, child, parent]
        text = util.pread(cmd)

    def _clonecleanup(self, src, dst, newsrc):
        try:
            if dst:
                util.ioretry(lambda: self._unlink(dst))
        except util.CommandException as inst:
            pass
        try:
            if util.ioretry(lambda: util.pathexists(newsrc)):
                stats = os.stat(newsrc)
                # Check if we have more than one link to newsrc
                if (stats.st_nlink > 1):
                    util.ioretry(lambda: self._unlink(newsrc))
                elif not self._is_hidden(newsrc):
                    self._rename(newsrc, src)
        except util.CommandException as inst:
            pass

    def _checkpath(self, path):
        try:
            if not util.ioretry(lambda: util.pathexists(path)):
                return False
            return True
        except util.CommandException as inst:
            raise xs_errors.XenError('EIO', \
                  opterr='IO error checking path %s' % path)

    def _query_v(self, path):
        cmd = [SR.TAPDISK_UTIL, "query", vhdutil.VDI_TYPE_VHD, "-v", path]
        return int(util.pread(cmd)) * 1024 * 1024

    def _query_p_uuid(self, path):
        cmd = [SR.TAPDISK_UTIL, "query", vhdutil.VDI_TYPE_VHD, "-p", path]
        parent = util.pread(cmd)
        parent = parent[:-1]
        ls = parent.split('/')
        return ls[len(ls) - 1].replace(vhdutil.FILE_EXTN_VHD, '')

    def _query_info(self, path, use_bkp_footer=False):
        diskinfo = {}
        qopts = '-vpf'
        if use_bkp_footer:
            qopts += 'b'
        cmd = [SR.TAPDISK_UTIL, "query", vhdutil.VDI_TYPE_VHD, qopts, path]
        txt = util.pread(cmd).split('\n')
        diskinfo['size'] = txt[0]
        lst = [txt[1].split('/')[-1].replace(vhdutil.FILE_EXTN_VHD, "")]
        for val in filter(util.exactmatch_uuid, lst):
            diskinfo['parent'] = val
        diskinfo['hidden'] = txt[2].split()[1]
        return diskinfo

    def _create(self, size, path):
        cmd = [SR.TAPDISK_UTIL, "create", vhdutil.VDI_TYPE_VHD, size, path]
        text = util.pread(cmd)
        if self.key_hash:
            vhdutil.setKey(path, self.key_hash)

    def _mark_hidden(self, path):
        vhdutil.setHidden(path, True)
        self.hidden = 1

    def _is_hidden(self, path):
        return vhdutil.getHidden(path) == 1

    def extractUuid(path):
        fileName = os.path.basename(path)
        uuid = fileName.replace(vhdutil.FILE_EXTN_VHD, "")
        return uuid
    extractUuid = staticmethod(extractUuid)

    def generate_config(self, sr_uuid, vdi_uuid):
        """
        Generate the XML config required to attach and activate
        a VDI for use when XAPI is not running. Attach and
        activation is handled by vdi_attach_from_config below.
        """
        util.SMlog("FileVDI.generate_config")
        if not util.pathexists(self.path):
            raise xs_errors.XenError('VDIUnavailable')
        resp = {}
        resp['device_config'] = self.sr.dconf
        resp['sr_uuid'] = sr_uuid
        resp['vdi_uuid'] = vdi_uuid
        resp['command'] = 'vdi_attach_from_config'
        # Return the 'config' encoded within a normal XMLRPC response so that
        # we can use the regular response/error parsing code.
        config = xmlrpc.client.dumps(tuple([resp]), "vdi_attach_from_config")
        return xmlrpc.client.dumps((config, ), "", True)

    def attach_from_config(self, sr_uuid, vdi_uuid):
        """
        Attach and activate a VDI using config generated by
        vdi_generate_config above. This is used for cases such as
        the HA state-file and the redo-log.
        """
        util.SMlog("FileVDI.attach_from_config")
        try:
            if not util.pathexists(self.sr.path):
                self.sr.attach(sr_uuid)
        except:
            util.logException("FileVDI.attach_from_config")
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='Unable to attach from config'
            )

    def _create_cbt_log(self):
        # Create CBT log file
        # Name: <vdi_uuid>.cbtlog
        #Handle if file already exists
        log_path = self._get_cbt_logpath(self.uuid)
        open_file = open(log_path, "w+")
        open_file.close()
        return super(FileVDI, self)._create_cbt_log()

    def _delete_cbt_log(self):
        logPath = self._get_cbt_logpath(self.uuid)
        try:
            os.remove(logPath)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    def _cbt_log_exists(self, logpath):
        return util.pathexists(logpath)


class SharedFileSR(FileSR):
    """
    FileSR subclass for SRs that use shared network storage
    """

    def _check_writable(self):
        """
        Checks that the filesystem being used by the SR can be written to,
        raising an exception if it can't.
        """
        test_name = os.path.join(self.path, str(uuid4()))
        try:
            open(test_name, 'ab').close()
        except OSError as e:
            util.SMlog("Cannot write to SR file system: %s" % e)
            raise xs_errors.XenError('SharedFileSystemNoWrite')
        finally:
            util.force_unlink(test_name)

    def _raise_hardlink_error(self):
        raise OSError(524, "Unknown error 524")

    def _check_hardlinks(self):
        hardlink_conf = self._read_hardlink_conf()
        if hardlink_conf is not None:
            return hardlink_conf

        test_name = os.path.join(self.path, str(uuid4()))
        open(test_name, 'ab').close()

        link_name = '%s.new' % test_name
        try:
            # XSI-1100: Let tests simulate failure of the link operation
            util.fistpoint.activate_custom_fn(
                "FileSR_fail_hardlink",
                self._raise_hardlink_error)

            os.link(test_name, link_name)
            self._write_hardlink_conf(supported=True)
            return True
        except OSError:
            self._write_hardlink_conf(supported=False)

            msg = "File system for SR %s does not support hardlinks, crash " \
                "consistency of snapshots cannot be assured" % self.uuid
            util.SMlog(msg, priority=util.LOG_WARNING)
            # Note: session can be not set during attach/detach_from_config calls.
            if self.session:
                try:
                    self.session.xenapi.message.create(
                        "sr_does_not_support_hardlinks", 2, "SR", self.uuid,
                        msg)
                except XenAPI.Failure:
                    # Might already be set and checking has TOCTOU issues
                    pass
        finally:
            util.force_unlink(link_name)
            util.force_unlink(test_name)

        return False

    def _get_hardlink_conf_path(self):
        return os.path.join(self.path, 'sm-hardlink.conf')

    def _read_hardlink_conf(self):
        try:
            with open(self._get_hardlink_conf_path(), 'r') as f:
                try:
                    return bool(int(f.read()))
                except Exception as e:
                    # If we can't read, assume the file is empty and test for hardlink support.
                    return None
        except IOError as e:
            if e.errno == errno.ENOENT:
                # If the config file doesn't exist, assume we want to support hardlinks.
                return None
            util.SMlog('Failed to read hardlink conf: {}'.format(e))
            # Can be caused by a concurrent access, not a major issue.
            return None

    def _write_hardlink_conf(self, supported):
        try:
            with open(self._get_hardlink_conf_path(), 'w') as f:
                f.write('1' if supported else '0')
        except Exception as e:
            # Can be caused by a concurrent access, not a major issue.
            util.SMlog('Failed to write hardlink conf: {}'.format(e))

# SR registration at import
SR.registerSR(FileSR)
