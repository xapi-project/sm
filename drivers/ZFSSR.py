#!/usr/bin/env python
#
# Copyright (C) 2020  Vates SAS
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import SR
import SRCommand

import FileSR

import util
import xs_errors

CAPABILITIES = [
    'SR_PROBE',
    'SR_UPDATE',
    'VDI_CREATE',
    'VDI_DELETE',
    'VDI_ATTACH',
    'VDI_DETACH',
    'VDI_CLONE',
    'VDI_SNAPSHOT',
    'VDI_RESIZE',
    'VDI_MIRROR',
    'VDI_GENERATE_CONFIG',
    'ATOMIC_PAUSE',
    'VDI_CONFIG_CBT',
    'VDI_ACTIVATE',
    'VDI_DEACTIVATE',
    'THIN_PROVISIONING'
]

CONFIGURATION = [
    ['location', 'local ZFS directory path (required)']
]

DRIVER_INFO = {
    'name': 'Local ZFS VHD',
    'description':
        'SR plugin which represents disks as VHD files stored on a ZFS disk',
    'vendor': 'Vates SAS',
    'copyright': '(C) 2020 Vates SAS',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
}


class ZFSSR(FileSR.FileSR):
    DRIVER_TYPE = 'zfs'

    @staticmethod
    def handles(type):
        return type == ZFSSR.DRIVER_TYPE

    def load(self, sr_uuid):
        if not self._is_zfs_available():
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='zfs is not installed or module is not loaded'
            )
        return super(ZFSSR, self).load(sr_uuid)

    def create(self, sr_uuid, size):
        if not self._is_zfs_path(self.remotepath):
            raise xs_errors.XenError(
                'ZFSSRCreate',
                opterr='Cannot create SR, path is not a ZFS mountpoint'
            )
        return super(ZFSSR, self).create(sr_uuid, size)

    def delete(self, sr_uuid):
        if not self._checkmount():
            raise xs_errors.XenError(
                'ZFSSRDelete',
                opterr='ZFS SR is not mounted or uses an invalid FS type'
            )
        return super(ZFSSR, self).delete(sr_uuid)

    def attach(self, sr_uuid):
        if not self._is_zfs_path(self.remotepath):
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='Invalid ZFS path'
            )
        return super(ZFSSR, self).attach(sr_uuid)

    def detach(self, sr_uuid):
        return super(ZFSSR, self).detach(sr_uuid)

    def vdi(self, uuid, loadLocked=False):
        return ZFSFileVDI(self, uuid)

    # Ensure _checkmount is overridden to prevent bad behaviors in FileSR.
    def _checkmount(self):
        return super(ZFSSR, self)._checkmount() and \
            self._is_zfs_path(self.remotepath)

    @staticmethod
    def _is_zfs_path(path):
        cmd = ['findmnt', '-o', 'FSTYPE', '-n', path]
        fs_type = util.pread2(cmd).split('\n')[0]
        return fs_type == 'zfs'

    @staticmethod
    def _is_zfs_available():
        import distutils.spawn
        return distutils.spawn.find_executable('zfs') and \
            util.pathexists('/sys/module/zfs/initstate')


class ZFSFileVDI(FileSR.FileVDI):
    def attach(self, sr_uuid, vdi_uuid):
        if not hasattr(self, 'xenstore_data'):
            self.xenstore_data = {}

        self.xenstore_data['storage-type'] = ZFSSR.DRIVER_TYPE

        return super(ZFSFileVDI, self).attach(sr_uuid, vdi_uuid)


if __name__ == '__main__':
    SRCommand.run(ZFSSR, DRIVER_INFO)
else:
    SR.registerSR(ZFSSR)
