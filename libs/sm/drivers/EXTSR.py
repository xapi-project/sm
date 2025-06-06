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
# EXTSR: Based on local-file storage repository, mounts ext3 partition
#        matches with drivers/EXTSR

import os

from sm import SR
from sm import lvutil
from sm import vhdutil
from sm.constants import EXT_PREFIX
from sm.drivers import FileSR
from sm.SR import deviceCheck
from sm.core import util
from sm.core import scsiutil
from sm.core import xs_errors
from sm.core.lock import Lock

CAPABILITIES = ["SR_PROBE", "SR_UPDATE", "SR_SUPPORTS_LOCAL_CACHING",
                "VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH",
                "VDI_UPDATE", "VDI_CLONE", "VDI_SNAPSHOT", "VDI_RESIZE", "VDI_MIRROR",
                "VDI_GENERATE_CONFIG",
                "VDI_RESET_ON_BOOT/2", "ATOMIC_PAUSE", "VDI_CONFIG_CBT",
                "VDI_ACTIVATE", "VDI_DEACTIVATE", "THIN_PROVISIONING", "VDI_READ_CACHING"]

CONFIGURATION = [['device', 'local device path (required) (e.g. /dev/sda3)']]

DRIVER_INFO = {
    'name': 'Local EXT3 VHD',
    'description': 'SR plugin which represents disks as VHD files stored on a local EXT3 filesystem, created inside an LVM volume',
    'vendor': 'Citrix Systems Inc',
    'copyright': '(C) 2008 Citrix Systems Inc',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
    }

DRIVER_CONFIG = {"ATTACH_FROM_CONFIG_WITH_TAPDISK": True}


class EXTSR(FileSR.FileSR):
    """EXT3 Local file storage repository"""

    def handles(srtype):
        return srtype == 'ext'
    handles = staticmethod(handles)

    def load(self, sr_uuid):
        self.ops_exclusive = FileSR.OPS_EXCLUSIVE
        self.lock = Lock(vhdutil.LOCK_TYPE_SR, self.uuid)
        self.sr_vditype = SR.DEFAULT_TAP

        self.path = os.path.join(SR.MOUNT_BASE, sr_uuid)
        self.vgname = EXT_PREFIX + sr_uuid
        self.remotepath = os.path.join("/dev", self.vgname, sr_uuid)
        self.attached = self._checkmount()
        self.driver_config = DRIVER_CONFIG

    def delete(self, sr_uuid):
        super(EXTSR, self).delete(sr_uuid)

        # Check PVs match VG
        try:
            for dev in self.dconf['device'].split(','):
                cmd = ["pvs", dev]
                txt = util.pread2(cmd)
                if txt.find(self.vgname) == -1:
                    raise xs_errors.XenError('VolNotFound',
                                             opterr='volume is %s' % self.vgname)
        except util.CommandException as inst:
            raise xs_errors.XenError('PVSfailed',
                                     opterr='error is %d' % inst.code)

        # Remove LV, VG and pv
        try:
            cmd = ["lvremove", "-f", self.remotepath]
            util.pread2(cmd)

            cmd = ["vgremove", self.vgname]
            util.pread2(cmd)

            for dev in self.dconf['device'].split(','):
                cmd = ["pvremove", dev]
                util.pread2(cmd)
        except util.CommandException as inst:
            raise xs_errors.XenError('LVMDelete',
                                     opterr='errno is %d' % inst.code)

    def attach(self, sr_uuid):
        if not self._checkmount():
            try:
                #Activate LV
                cmd = ['lvchange', '-ay', self.remotepath]
                util.pread2(cmd)
            except util.CommandException as inst:
                raise xs_errors.XenError(
                    'LVMMount',
                    opterr='Unable to activate LV. Errno is %d' % inst.code)

            try:
                util.pread(["fsck", "-a", self.remotepath])
            except util.CommandException as inst:
                if inst.code == 1:
                    util.SMlog("FSCK detected and corrected FS errors. Not fatal.")
                else:
                    raise xs_errors.XenError(
                        'LVMMount',
                        opterr='FSCK failed on %s. Errno is %d' % (self.remotepath, inst.code))

            super(EXTSR, self).attach(sr_uuid, bind=False)

        self.attached = True

        #Update SCSIid string
        scsiutil.add_serial_record(
            self.session, self.sr_ref,
            scsiutil.devlist_to_serialstring(self.dconf['device'].split(',')))

        # Set the block scheduler
        for dev in self.dconf['device'].split(','):
            self.block_setscheduler(dev)

    def detach(self, sr_uuid):
        super(EXTSR, self).detach(sr_uuid)
        try:
            # deactivate SR
            cmd = ["lvchange", "-an", self.remotepath]
            util.pread2(cmd)
        except util.CommandException as inst:
            raise xs_errors.XenError(
                'LVMUnMount',
                opterr='lvm -an failed errno is %d' % inst.code)

    @deviceCheck
    def probe(self):
        return lvutil.srlist_toxml(lvutil.scan_srlist(EXT_PREFIX, self.dconf['device']),
                EXT_PREFIX)

    @deviceCheck
    def create(self, sr_uuid, size):
        if self._checkmount():
            raise xs_errors.XenError('SRExists')

        # Check none of the devices already in use by other PBDs
        if util.test_hostPBD_devs(self.session, sr_uuid, self.dconf['device']):
            raise xs_errors.XenError('SRInUse')

        # Check serial number entry in SR records
        for dev in self.dconf['device'].split(','):
            if util.test_scsiserial(self.session, dev):
                raise xs_errors.XenError('SRInUse')

        if not lvutil._checkVG(self.vgname):
            lvutil.createVG(self.dconf['device'], self.vgname)

        if lvutil._checkLV(self.remotepath):
            raise xs_errors.XenError('SRExists')

        try:
            numdevs = len(self.dconf['device'].split(','))
            cmd = ["lvcreate", "-n", sr_uuid]
            if numdevs > 1:
                lowest = -1
                for dev in self.dconf['device'].split(','):
                    stats = lvutil._getPVstats(dev)
                    if lowest < 0  or stats['freespace'] < lowest:
                        lowest = stats['freespace']
                size_mb = (lowest // (1024 * 1024)) * numdevs

                # Add stripe parameter to command
                cmd += ["-i", str(numdevs), "-I", "2048"]
            else:
                stats = lvutil._getVGstats(self.vgname)
                size_mb = stats['freespace'] // (1024 * 1024)
            assert(size_mb > 0)
            cmd += ["-L", str(size_mb), self.vgname]
            cmd += ["-W", "y", "--yes"]
            text = util.pread(cmd)

            cmd = ["lvchange", "-ay", self.remotepath]
            text = util.pread(cmd)
        except util.CommandException as inst:
            raise xs_errors.XenError(
                'LVMCreate',
                opterr='lv operation, error %d' % inst.code)
        except AssertionError:
            raise xs_errors.XenError(
                'SRNoSpace',
                opterr='Insufficient space in VG %s' % self.vgname)

        try:
            util.pread2(["mkfs.ext4", "-F", self.remotepath])
        except util.CommandException as inst:
            raise xs_errors.XenError('LVMFilesystem',
                                     opterr='mkfs failed error %d' % inst.code)

        #Update serial number string
        scsiutil.add_serial_record(
            self.session, self.sr_ref,
            scsiutil.devlist_to_serialstring(self.dconf['device'].split(',')))

    def vdi(self, uuid):
        return EXTFileVDI(self, uuid)


class EXTFileVDI(FileSR.FileVDI):
    def attach(self, sr_uuid, vdi_uuid):
        if not hasattr(self, 'xenstore_data'):
            self.xenstore_data = {}

        self.xenstore_data["storage-type"] = "ext"

        return super(EXTFileVDI, self).attach(sr_uuid, vdi_uuid)


# SR registration at import
SR.registerSR(EXTSR)
