#!/usr/bin/env python
#
# Copyright (C) 2020  Vates SAS - ronan.abhamon@vates.fr
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

import base64
import distutils.util
import errno
import json
import socket
import util
import vhdutil
import xs_errors

MANAGER_PLUGIN = 'linstor-manager'


def linstorhostcall(local_method, remote_method):
    def decorated(func):
        def wrapper(*args, **kwargs):
            self = args[0]
            vdi_uuid = args[1]

            device_path = self._linstor.build_device_path(
                self._linstor.get_volume_name(vdi_uuid)
            )

            # A. Try a call using directly the DRBD device to avoid
            # remote request.

            # Try to read locally if the device is not in use or if the device
            # is up to date and not diskless.
            (node_names, in_use) = \
                self._linstor.find_up_to_date_diskfull_nodes(vdi_uuid)

            try:
                if not in_use or socket.gethostname() in node_names:
                    return local_method(device_path, *args[2:], **kwargs)
            except util.CommandException as e:
                # EMEDIUMTYPE constant (124) is not available in python2.
                if e.code != errno.EROFS and e.code != 124:
                    raise

            # B. Execute the plugin on master or slave.
            def exec_remote_method():
                host_ref = self._get_readonly_host(
                    vdi_uuid, device_path, node_names
                )
                args = {
                    'devicePath': device_path,
                    'groupName': self._linstor.group_name
                }
                args.update(**kwargs)

                try:
                    response = self._session.xenapi.host.call_plugin(
                        host_ref, MANAGER_PLUGIN, remote_method, args
                    )
                except Exception as e:
                    util.SMlog('call-plugin ({} with {}) exception: {}'.format(
                        remote_method, args, e
                    ))
                    raise

                util.SMlog('call-plugin ({} with {}) returned: {}'.format(
                    remote_method, args, response
                ))
                if response == 'False':
                    raise xs_errors.XenError(
                        'VDIUnavailable',
                        opterr='Plugin {} failed'.format(MANAGER_PLUGIN)
                    )
                kwargs['response'] = response

            util.retry(exec_remote_method, 5, 3)
            return func(*args, **kwargs)
        return wrapper
    return decorated


class LinstorVhdUtil:
    def __init__(self, session, linstor):
        self._session = session
        self._linstor = linstor

    @linstorhostcall(vhdutil.check, 'check')
    def check(self, vdi_uuid, **kwargs):
        return distutils.util.strtobool(kwargs['response'])

    def get_vhd_info(self, vdi_uuid, include_parent=True):
        kwargs = {'includeParent': str(include_parent)}
        return self._get_vhd_info(vdi_uuid, self._extract_uuid, **kwargs)

    @linstorhostcall(vhdutil.getVHDInfo, 'getVHDInfo')
    def _get_vhd_info(self, vdi_uuid, *args, **kwargs):
        obj = json.loads(kwargs['response'])

        vhd_info = vhdutil.VHDInfo(vdi_uuid)
        vhd_info.sizeVirt = obj['sizeVirt']
        vhd_info.sizePhys = obj['sizePhys']
        if 'parentPath' in obj:
            vhd_info.parentPath = obj['parentPath']
            vhd_info.parentUuid = obj['parentUuid']
        vhd_info.hidden = obj['hidden']
        vhd_info.path = obj['path']

        return vhd_info

    @linstorhostcall(vhdutil.hasParent, 'hasParent')
    def has_parent(self, vdi_uuid, **kwargs):
        return distutils.util.strtobool(kwargs['response'])

    def get_parent(self, vdi_uuid):
        return self._get_parent(vdi_uuid, self._extract_uuid)

    @linstorhostcall(vhdutil.getParent, 'getParent')
    def _get_parent(self, vdi_uuid, *args, **kwargs):
        return kwargs['response']

    @linstorhostcall(vhdutil.getSizeVirt, 'getSizeVirt')
    def get_size_virt(self, vdi_uuid, **kwargs):
        return int(kwargs['response'])

    @linstorhostcall(vhdutil.getSizePhys, 'getSizePhys')
    def get_size_phys(self, vdi_uuid, **kwargs):
        return int(kwargs['response'])

    @linstorhostcall(vhdutil.getDepth, 'getDepth')
    def get_depth(self, vdi_uuid, **kwargs):
        return int(kwargs['response'])

    @linstorhostcall(vhdutil.getKeyHash, 'getKeyHash')
    def get_key_hash(self, vdi_uuid, **kwargs):
        return kwargs['response'] or None

    @linstorhostcall(vhdutil.getBlockBitmap, 'getBlockBitmap')
    def get_block_bitmap(self, vdi_uuid, **kwargs):
        return base64.b64decode(kwargs['response'])

    # --------------------------------------------------------------------------
    # Helpers.
    # --------------------------------------------------------------------------

    def _extract_uuid(self, device_path):
        # TODO: Remove new line in the vhdutil module. Not here.
        return self._linstor.get_volume_uuid_from_device_path(
            device_path.rstrip('\n')
        )

    def _get_readonly_host(self, vdi_uuid, device_path, node_names):
        """
        When vhd-util is called to fetch VDI info we must find a
        diskfull DRBD disk to read the data. It's the goal of this function.
        Why? Because when a VHD is open in RO mode, the LVM layer is used
        directly to bypass DRBD verifications (we can have only one process
        that reads/writes to disk with DRBD devices).
        """

        if not node_names:
            raise xs_errors.XenError(
                'VDIUnavailable',
                opterr='Unable to find diskfull node: {} (path={})'
                .format(vdi_uuid, device_path)
            )

        hosts = self._session.xenapi.host.get_all_records()
        for host_ref, host_record in hosts.items():
            if host_record['hostname'] in node_names:
                return host_ref

        raise xs_errors.XenError(
            'VDIUnavailable',
            opterr='Unable to find a valid host from VDI: {} (path={})'
            .format(vdi_uuid, device_path)
        )
