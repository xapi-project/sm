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

from constants import CBTLOG_TAG

try:
    from linstorjournaler import LinstorJournaler
    from linstorvhdutil import LinstorVhdUtil
    from linstorvolumemanager \
        import LinstorVolumeManager, LinstorVolumeManagerError
    LINSTOR_AVAILABLE = True
except ImportError:
    LINSTOR_AVAILABLE = False

from lock import Lock
import blktap2
import cleanup
import errno
import functools
import scsiutil
import SR
import SRCommand
import time
import traceback
import util
import VDI
import vhdutil
import xmlrpclib
import xs_errors

from srmetadata import \
    NAME_LABEL_TAG, NAME_DESCRIPTION_TAG, IS_A_SNAPSHOT_TAG, SNAPSHOT_OF_TAG, \
    TYPE_TAG, VDI_TYPE_TAG, READ_ONLY_TAG, SNAPSHOT_TIME_TAG, \
    METADATA_OF_POOL_TAG

HIDDEN_TAG = 'hidden'

# ==============================================================================

# TODO: Supports 'VDI_INTRODUCE', 'VDI_RESET_ON_BOOT/2', 'SR_TRIM',
# 'VDI_CONFIG_CBT', 'SR_PROBE'

CAPABILITIES = [
    'ATOMIC_PAUSE',
    'SR_UPDATE',
    'VDI_CREATE',
    'VDI_DELETE',
    'VDI_UPDATE',
    'VDI_ATTACH',
    'VDI_DETACH',
    'VDI_ACTIVATE',
    'VDI_DEACTIVATE',
    'VDI_CLONE',
    'VDI_MIRROR',
    'VDI_RESIZE',
    'VDI_SNAPSHOT',
    'VDI_GENERATE_CONFIG'
]

CONFIGURATION = [
    ['group-name', 'LVM group name'],
    ['hosts', 'host names to use'],
    ['redundancy', 'replication count'],
    ['provisioning', '"thin" or "thick" are accepted']
]

DRIVER_INFO = {
    'name': 'LINSTOR resources on XCP-ng',
    'description': 'SR plugin which uses Linstor to manage VDIs',
    'vendor': 'Vates',
    'copyright': '(C) 2020 Vates',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
}

DRIVER_CONFIG = {'ATTACH_FROM_CONFIG_WITH_TAPDISK': False}

OPS_EXCLUSIVE = [
    'sr_create', 'sr_delete', 'sr_attach', 'sr_detach', 'sr_scan',
    'sr_update', 'vdi_create', 'vdi_delete', 'vdi_clone', 'vdi_snapshot'
]

# ==============================================================================
# Misc helpers used by LinstorSR and linstor-thin plugin.
# ==============================================================================


def compute_volume_size(virtual_size, image_type):
    if image_type == vhdutil.VDI_TYPE_VHD:
        # All LINSTOR VDIs have the metadata area preallocated for
        # the maximum possible virtual size (for fast online VDI.resize).
        meta_overhead = vhdutil.calcOverheadEmpty(LinstorVDI.MAX_SIZE)
        bitmap_overhead = vhdutil.calcOverheadBitmap(virtual_size)
        virtual_size += meta_overhead + bitmap_overhead
    elif image_type != vhdutil.VDI_TYPE_RAW:
        raise Exception('Invalid image type: {}'.format(image_type))

    return LinstorVolumeManager.round_up_volume_size(virtual_size)


def try_lock(lock):
    for i in range(20):
        if lock.acquireNoblock():
            return
        time.sleep(1)
    raise util.SRBusyException()


def attach_thin(session, journaler, linstor, sr_uuid, vdi_uuid):
    volume_metadata = linstor.get_volume_metadata(vdi_uuid)
    image_type = volume_metadata.get(VDI_TYPE_TAG)
    if image_type == vhdutil.VDI_TYPE_RAW:
        return

    lock = Lock(vhdutil.LOCK_TYPE_SR, sr_uuid)
    try:
        try_lock(lock)

        device_path = linstor.get_device_path(vdi_uuid)

        # If the virtual VHD size is lower than the LINSTOR volume size,
        # there is nothing to do.
        vhd_size = compute_volume_size(
            LinstorVhdUtil(session, linstor).get_size_virt(vdi_uuid),
            image_type
        )

        volume_info = linstor.get_volume_info(vdi_uuid)
        volume_size = volume_info.virtual_size

        if vhd_size > volume_size:
            inflate(
                journaler, linstor, vdi_uuid, device_path,
                vhd_size, volume_size
            )
    finally:
        lock.release()


def detach_thin(session, linstor, sr_uuid, vdi_uuid):
    volume_metadata = linstor.get_volume_metadata(vdi_uuid)
    image_type = volume_metadata.get(VDI_TYPE_TAG)
    if image_type == vhdutil.VDI_TYPE_RAW:
        return

    lock = Lock(vhdutil.LOCK_TYPE_SR, sr_uuid)
    try:
        try_lock(lock)

        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        vbds = session.xenapi.VBD.get_all_records_where(
            'field "VDI" = "{}"'.format(vdi_ref)
        )

        num_plugged = 0
        for vbd_rec in vbds.values():
            if vbd_rec['currently_attached']:
                num_plugged += 1
                if num_plugged > 1:
                    raise xs_errors.XenError(
                        'VDIUnavailable',
                        opterr='Cannot deflate VDI {}, already used by '
                        'at least 2 VBDs'.format(vdi_uuid)
                    )

        device_path = linstor.get_device_path(vdi_uuid)
        new_volume_size = LinstorVolumeManager.round_up_volume_size(
            LinstorVhdUtil(session, linstor).get_size_phys(device_path)
        )

        volume_info = linstor.get_volume_info(vdi_uuid)
        old_volume_size = volume_info.virtual_size
        deflate(vdi_uuid, device_path, new_volume_size, old_volume_size)
    finally:
        lock.release()


def inflate(journaler, linstor, vdi_uuid, vdi_path, new_size, old_size):
    # Only inflate if the LINSTOR volume capacity is not enough.
    new_size = LinstorVolumeManager.round_up_volume_size(new_size)
    if new_size <= old_size:
        return

    util.SMlog(
        'Inflate {} (new VHD size={}, previous={})'
        .format(vdi_uuid, new_size, old_size)
    )

    journaler.create(
        LinstorJournaler.INFLATE, vdi_uuid, old_size
    )
    linstor.resize_volume(vdi_uuid, new_size)

    if not util.zeroOut(
        vdi_path, new_size - vhdutil.VHD_FOOTER_SIZE,
        vhdutil.VHD_FOOTER_SIZE
    ):
        raise xs_errors.XenError(
            'EIO',
            opterr='Failed to zero out VHD footer {}'.format(vdi_path)
        )

    vhdutil.setSizePhys(vdi_path, new_size, False)
    journaler.remove(LinstorJournaler.INFLATE, vdi_uuid)


def deflate(vdi_uuid, vdi_path, new_size, old_size):
    new_size = LinstorVolumeManager.round_up_volume_size(new_size)
    if new_size >= old_size:
        return

    util.SMlog(
        'Deflate {} (new size={}, previous={})'
        .format(vdi_uuid, new_size, old_size)
    )

    vhdutil.setSizePhys(vdi_path, new_size)
    # TODO: Change the LINSTOR volume size using linstor.resize_volume.


# ==============================================================================

# Usage example:
# xe sr-create type=linstor name-label=linstor-sr
# host-uuid=d2deba7a-c5ad-4de1-9a20-5c8df3343e93
# device-config:hosts=node-linstor1,node-linstor2,node-linstor3
# device-config:group-name=vg_loop device-config:redundancy=2


class LinstorSR(SR.SR):
    DRIVER_TYPE = 'linstor'

    PROVISIONING_TYPES = ['thin', 'thick']
    PROVISIONING_DEFAULT = 'thin'

    MANAGER_PLUGIN = 'linstor-manager'

    # --------------------------------------------------------------------------
    # SR methods.
    # --------------------------------------------------------------------------

    @staticmethod
    def handles(type):
        return type == LinstorSR.DRIVER_TYPE

    def load(self, sr_uuid):
        if not LINSTOR_AVAILABLE:
            raise util.SMException(
                'Can\'t load LinstorSR: LINSTOR libraries are missing'
            )

        # Check parameters.
        if 'hosts' not in self.dconf or not self.dconf['hosts']:
            raise xs_errors.XenError('LinstorConfigHostsMissing')
        if 'group-name' not in self.dconf or not self.dconf['group-name']:
            raise xs_errors.XenError('LinstorConfigGroupNameMissing')
        if 'redundancy' not in self.dconf or not self.dconf['redundancy']:
            raise xs_errors.XenError('LinstorConfigRedundancyMissing')

        self.driver_config = DRIVER_CONFIG

        # Check provisioning config.
        provisioning = self.dconf.get('provisioning')
        if provisioning:
            if provisioning in self.PROVISIONING_TYPES:
                self._provisioning = provisioning
            else:
                raise xs_errors.XenError(
                    'InvalidArg',
                    opterr='Provisioning parameter must be one of {}'.format(
                        self.PROVISIONING_TYPES
                    )
                )
        else:
            self._provisioning = self.PROVISIONING_DEFAULT

        # Note: We don't have access to the session field if the
        # 'vdi_attach_from_config' command is executed.
        self._has_session = self.sr_ref and self.session is not None
        if self._has_session:
            self.sm_config = self.session.xenapi.SR.get_sm_config(self.sr_ref)
        else:
            self.sm_config = self.srcmd.params.get('sr_sm_config') or {}

        provisioning = self.sm_config.get('provisioning')
        if provisioning in self.PROVISIONING_TYPES:
            self._provisioning = provisioning

        # Define properties for SR parent class.
        self.ops_exclusive = OPS_EXCLUSIVE
        self.path = LinstorVolumeManager.DEV_ROOT_PATH
        self.lock = Lock(vhdutil.LOCK_TYPE_SR, self.uuid)
        self.sr_vditype = SR.DEFAULT_TAP

        self._hosts = self.dconf['hosts'].split(',')
        self._redundancy = int(self.dconf['redundancy'] or 1)
        self._linstor = None  # Ensure that LINSTOR attribute exists.
        self._journaler = None

        self._is_master = False
        if 'SRmaster' in self.dconf and self.dconf['SRmaster'] == 'true':
            self._is_master = True
        self._group_name = self.dconf['group-name']

        self._master_uri = None
        self._vdi_shared_locked = False

        self._initialized = False

    def _locked_load(method):
        @functools.wraps(method)
        def wrap(self, *args, **kwargs):
            if self._initialized:
                return method(self, *args, **kwargs)
            self._initialized = True

            if not self._has_session:
                if self.srcmd.cmd == 'vdi_attach_from_config':
                    # We must have a valid LINSTOR instance here without using
                    # the XAPI.
                    self._master_uri = 'linstor://{}'.format(
                        util.get_master_address()
                    )
                    self._journaler = LinstorJournaler(
                        self._master_uri, self._group_name, logger=util.SMlog
                    )

                    try:
                        self._linstor = LinstorVolumeManager(
                            self._master_uri,
                            self._group_name,
                            logger=util.SMlog
                        )
                        return
                    except Exception as e:
                        util.SMlog(
                            'Ignore exception. Failed to build LINSTOR '
                            'instance without session: {}'.format(e)
                        )
                return

            self._master_uri = 'linstor://{}'.format(
                util.get_master_rec(self.session)['address']
            )

            if not self._is_master:
                if self.cmd in [
                    'sr_create', 'sr_delete', 'sr_update', 'sr_probe',
                    'sr_scan', 'vdi_create', 'vdi_delete', 'vdi_resize',
                    'vdi_snapshot', 'vdi_clone'
                ]:
                    util.SMlog('{} blocked for non-master'.format(self.cmd))
                    raise xs_errors.XenError('LinstorMaster')

                # Because the LINSTOR KV objects cache all values, we must lock
                # the VDI before the LinstorJournaler/LinstorVolumeManager
                # instantiation and before any action on the master to avoid a
                # bad read. The lock is also necessary to avoid strange
                # behaviors if the GC is executed during an action on a slave.
                if self.cmd.startswith('vdi_'):
                    self._shared_lock_vdi(self.srcmd.params['vdi_uuid'])
                    self._vdi_shared_locked = True

            self._journaler = LinstorJournaler(
                self._master_uri, self._group_name, logger=util.SMlog
            )

            # Ensure ports are opened and LINSTOR controller/satellite
            # are activated.
            if self.srcmd.cmd == 'sr_create':
                # TODO: Disable if necessary
                self._enable_linstor_on_all_hosts(status=True)

            try:
                # Try to open SR if exists.
                self._linstor = LinstorVolumeManager(
                    self._master_uri,
                    self._group_name,
                    repair=self._is_master,
                    logger=util.SMlog
                )
                self._vhdutil = LinstorVhdUtil(self.session, self._linstor)
            except Exception as e:
                if self.srcmd.cmd == 'sr_create' or \
                        self.srcmd.cmd == 'sr_detach':
                    # Ignore exception in this specific case: sr_create.
                    # At this moment the LinstorVolumeManager cannot be
                    # instantiated. Concerning the sr_detach command, we must
                    # ignore LINSTOR exceptions (if the volume group doesn't
                    # exist for example after a bad user action).
                    pass
                else:
                    raise xs_errors.XenError('SRUnavailable', opterr=str(e))

            if self._linstor:
                try:
                    hosts = self._linstor.disconnected_hosts
                except Exception as e:
                    raise xs_errors.XenError('SRUnavailable', opterr=str(e))

                if hosts:
                    util.SMlog('Failed to join node(s): {}'.format(hosts))

                try:
                    # If the command is a SR command on the master, we must
                    # load all VDIs and clean journal transactions.
                    # We must load the VDIs in the snapshot case too.
                    if self._is_master and self.cmd not in [
                        'vdi_attach', 'vdi_detach',
                        'vdi_activate', 'vdi_deactivate',
                        'vdi_epoch_begin', 'vdi_epoch_end',
                        'vdi_update', 'vdi_destroy'
                    ]:
                        self._load_vdis()
                        self._undo_all_journal_transactions()
                        self._linstor.remove_resourceless_volumes()

                    self._synchronize_metadata()
                except Exception as e:
                    util.SMlog(
                        'Ignoring exception in LinstorSR.load: {}'.format(e)
                    )
                    util.SMlog(traceback.format_exc())

            return method(self, *args, **kwargs)

        return wrap

    @_locked_load
    def cleanup(self):
        if self._vdi_shared_locked:
            self._shared_lock_vdi(self.srcmd.params['vdi_uuid'], locked=False)

    @_locked_load
    def create(self, uuid, size):
        util.SMlog('LinstorSR.create for {}'.format(self.uuid))

        if self._redundancy > len(self._hosts):
            raise xs_errors.XenError(
                'LinstorSRCreate',
                opterr='Redundancy greater than host count'
            )

        xenapi = self.session.xenapi
        srs = xenapi.SR.get_all_records_where(
            'field "type" = "{}"'.format(self.DRIVER_TYPE)
        )
        srs = dict(filter(lambda e: e[1]['uuid'] != self.uuid, srs.items()))

        for sr in srs.values():
            for pbd in sr['PBDs']:
                device_config = xenapi.PBD.get_device_config(pbd)
                group_name = device_config.get('group-name')
                if group_name and group_name == self._group_name:
                    raise xs_errors.XenError(
                        'LinstorSRCreate',
                        opterr='group name must be unique'
                    )

        # Create SR.
        # Throw if the SR already exists.
        try:
            self._linstor = LinstorVolumeManager.create_sr(
                self._master_uri,
                self._group_name,
                self._hosts,
                self._redundancy,
                thin_provisioning=self._provisioning == 'thin',
                logger=util.SMlog
            )
            self._vhdutil = LinstorVhdUtil(self.session, self._linstor)
        except Exception as e:
            util.SMlog('Failed to create LINSTOR SR: {}'.format(e))
            raise xs_errors.XenError('LinstorSRCreate', opterr=str(e))

    @_locked_load
    def delete(self, uuid):
        util.SMlog('LinstorSR.delete for {}'.format(self.uuid))
        cleanup.gc_force(self.session, self.uuid)

        if self.vdis:
            raise xs_errors.XenError('SRNotEmpty')

        try:
            # TODO: Use specific exceptions. If the LINSTOR group doesn't
            # exist, we can remove it without problem.

            # TODO: Maybe remove all volumes unused by the SMAPI.
            # We must ensure it's a safe idea...

            self._linstor.destroy()
            Lock.cleanupAll(self.uuid)
        except Exception as e:
            util.SMlog('Failed to delete LINSTOR SR: {}'.format(e))
            raise xs_errors.XenError(
                'LinstorSRDelete',
                opterr=str(e)
            )

    @_locked_load
    def update(self, uuid):
        util.SMlog('LinstorSR.update for {}'.format(self.uuid))

        # Well, how can we update a SR if it doesn't exist? :thinking:
        if not self._linstor:
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='no such volume group: {}'.format(self._group_name)
            )

        self._update_stats(0)

        # Update the SR name and description only in LINSTOR metadata.
        xenapi = self.session.xenapi
        self._linstor.metadata = {
            NAME_LABEL_TAG: util.to_plain_string(
                xenapi.SR.get_name_label(self.sr_ref)
            ),
            NAME_DESCRIPTION_TAG: util.to_plain_string(
                xenapi.SR.get_name_description(self.sr_ref)
            )
        }

    @_locked_load
    def attach(self, uuid):
        util.SMlog('LinstorSR.attach for {}'.format(self.uuid))

        if not self._linstor:
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='no such group: {}'.format(self._group_name)
            )

    @_locked_load
    def detach(self, uuid):
        util.SMlog('LinstorSR.detach for {}'.format(self.uuid))
        cleanup.abort(self.uuid)

    @_locked_load
    def probe(self):
        util.SMlog('LinstorSR.probe for {}'.format(self.uuid))
        # TODO

    @_locked_load
    def scan(self, uuid):
        util.SMlog('LinstorSR.scan for {}'.format(self.uuid))
        if not self._linstor:
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='no such volume group: {}'.format(self._group_name)
            )

        self._update_physical_size()

        for vdi_uuid in self.vdis.keys():
            if self.vdis[vdi_uuid].deleted:
                del self.vdis[vdi_uuid]

        # Update the database before the restart of the GC to avoid
        # bad sync in the process if new VDIs have been introduced.
        ret = super(LinstorSR, self).scan(self.uuid)
        self._kick_gc()
        return ret

    @_locked_load
    def vdi(self, uuid):
        return LinstorVDI(self, uuid)

    _locked_load = staticmethod(_locked_load)

    # --------------------------------------------------------------------------
    # Lock.
    # --------------------------------------------------------------------------

    def _shared_lock_vdi(self, vdi_uuid, locked=True):
        pools = self.session.xenapi.pool.get_all()
        master = self.session.xenapi.pool.get_master(pools[0])

        method = 'lockVdi'
        args = {
            'groupName': self._group_name,
            'srUuid': self.uuid,
            'vdiUuid': vdi_uuid,
            'locked': str(locked)
        }

        ret = self.session.xenapi.host.call_plugin(
            master, self.MANAGER_PLUGIN, method, args
        )
        util.SMlog(
            'call-plugin ({} with {}) returned: {}'
            .format(method, args, ret)
        )
        if ret == 'False':
            raise xs_errors.XenError(
                'VDIUnavailable',
                opterr='Plugin {} failed'.format(self.MANAGER_PLUGIN)
            )

    # --------------------------------------------------------------------------
    # Network.
    # --------------------------------------------------------------------------

    def _enable_linstor(self, host, status):
        method = 'enable'
        args = {'enabled': str(bool(status))}

        ret = self.session.xenapi.host.call_plugin(
            host, self.MANAGER_PLUGIN, method, args
        )
        util.SMlog(
            'call-plugin ({} with {}) returned: {}'.format(method, args, ret)
        )
        if ret == 'False':
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='Plugin {} failed'.format(self.MANAGER_PLUGIN)
            )

    def _enable_linstor_on_master(self, status):
        pools = self.session.xenapi.pool.get_all()
        master = self.session.xenapi.pool.get_master(pools[0])
        self._enable_linstor(master, status)

    def _enable_linstor_on_all_hosts(self, status):
        self._enable_linstor_on_master(status)
        for slave in util.get_all_slaves(self.session):
            self._enable_linstor(slave, status)

    # --------------------------------------------------------------------------
    # Metadata.
    # --------------------------------------------------------------------------

    def _synchronize_metadata_and_xapi(self):
        try:
            # First synch SR parameters.
            self.update(self.uuid)

            # Now update the VDI information in the metadata if required.
            xenapi = self.session.xenapi
            volumes_metadata = self._linstor.volumes_with_metadata
            for vdi_uuid, volume_metadata in volumes_metadata.items():
                try:
                    vdi_ref = xenapi.VDI.get_by_uuid(vdi_uuid)
                except Exception:
                    # May be the VDI is not in XAPI yet dont bother.
                    continue

                label = util.to_plain_string(
                    xenapi.VDI.get_name_label(vdi_ref)
                )
                description = util.to_plain_string(
                    xenapi.VDI.get_name_description(vdi_ref)
                )

                if (
                    volume_metadata.get(NAME_LABEL_TAG) != label or
                    volume_metadata.get(NAME_DESCRIPTION_TAG) != description
                ):
                    self._linstor.update_volume_metadata(vdi_uuid, {
                        NAME_LABEL_TAG: label,
                        NAME_DESCRIPTION_TAG: description
                    })
        except Exception as e:
            raise xs_errors.XenError(
                'MetadataError',
                opterr='Error synching SR Metadata and XAPI: {}'.format(e)
            )

    def _synchronize_metadata(self):
        if not self._is_master:
            return

        util.SMlog('Synchronize metadata...')
        if self.cmd == 'sr_attach':
            try:
                util.SMlog(
                    'Synchronize SR metadata and the state on the storage.'
                )
                self._synchronize_metadata_and_xapi()
            except Exception as e:
                util.SMlog('Failed to synchronize metadata: {}'.format(e))

    # --------------------------------------------------------------------------
    # Stats.
    # --------------------------------------------------------------------------

    def _update_stats(self, virt_alloc_delta):
        valloc = int(self.session.xenapi.SR.get_virtual_allocation(
            self.sr_ref
        ))

        # Update size attributes of the SR parent class.
        self.virtual_allocation = valloc + virt_alloc_delta

        # Physical size contains the total physical size.
        # i.e. the sum of the sizes of all devices on all hosts, not the AVG.
        self._update_physical_size()

        # Notify SR parent class.
        self._db_update()

    def _update_physical_size(self):
        # Physical size contains the total physical size.
        # i.e. the sum of the sizes of all devices on all hosts, not the AVG.
        self.physical_size = self._linstor.physical_size

        # `self._linstor.physical_free_size` contains the total physical free
        # memory. If Thin provisioning is used we can't use it, we must use
        # LINSTOR volume size to gives a good idea of the required
        # usable memory to the users.
        self.physical_utilisation = self._linstor.total_allocated_volume_size

        # If Thick provisioning is used, we can use this line instead:
        # self.physical_utilisation = \
        #     self.physical_size - self._linstor.physical_free_size

    # --------------------------------------------------------------------------
    # VDIs.
    # --------------------------------------------------------------------------

    def _load_vdis(self):
        if self.vdis:
            return

        # 1. Get existing VDIs in XAPI.
        xenapi = self.session.xenapi
        xapi_vdi_uuids = set()
        for vdi in xenapi.SR.get_VDIs(self.sr_ref):
            xapi_vdi_uuids.add(xenapi.VDI.get_uuid(vdi))

        # 2. Get volumes info.
        all_volume_info = self._linstor.volumes_with_info
        volumes_metadata = self._linstor.volumes_with_metadata

        # 3. Get CBT vdis.
        # See: https://support.citrix.com/article/CTX230619
        cbt_vdis = set()
        for volume_metadata in volumes_metadata.values():
            cbt_uuid = volume_metadata.get(CBTLOG_TAG)
            if cbt_uuid:
                cbt_vdis.add(cbt_uuid)

        introduce = False

        if self.cmd == 'sr_scan':
            has_clone_entries = list(self._journaler.get_all(
                LinstorJournaler.CLONE
            ).items())

            if has_clone_entries:
                util.SMlog(
                    'Cannot introduce VDIs during scan because it exists '
                    'CLONE entries in journaler on SR {}'.format(self.uuid)
                )
            else:
                introduce = True

        # 4. Now check all volume info.
        vdi_to_snaps = {}
        for vdi_uuid, volume_info in all_volume_info.items():
            if vdi_uuid.startswith(cleanup.SR.TMP_RENAME_PREFIX):
                continue

            # 4.a. Check if the VDI in LINSTOR is in XAPI VDIs.
            if vdi_uuid not in xapi_vdi_uuids:
                if not introduce:
                    continue

                volume_metadata = volumes_metadata.get(vdi_uuid)
                if not volume_metadata:
                    util.SMlog(
                        'Skipping volume {} because no metadata could be found'
                        .format(vdi_uuid)
                    )
                    continue

                util.SMlog(
                    'Trying to introduce VDI {} as it is present in '
                    'LINSTOR and not in XAPI...'
                    .format(vdi_uuid)
                )

                try:
                    self._linstor.get_device_path(vdi_uuid)
                except Exception as e:
                    util.SMlog(
                        'Cannot introduce {}, unable to get path: {}'
                        .format(vdi_uuid, e)
                    )
                    continue

                name_label = volume_metadata.get(NAME_LABEL_TAG) or ''
                type = volume_metadata.get(TYPE_TAG) or 'user'
                vdi_type = volume_metadata.get(VDI_TYPE_TAG)

                if not vdi_type:
                    util.SMlog(
                        'Cannot introduce {} '.format(vdi_uuid) +
                        'without vdi_type'
                    )
                    continue

                sm_config = {
                    'vdi_type': vdi_type
                }

                if vdi_type == vhdutil.VDI_TYPE_RAW:
                    managed = not volume_metadata.get(HIDDEN_TAG)
                elif vdi_type == vhdutil.VDI_TYPE_VHD:
                    vhd_info = self._vhdutil.get_vhd_info(vdi_uuid)
                    managed = not vhd_info.hidden
                    if vhd_info.parentUuid:
                        sm_config['vhd-parent'] = vhd_info.parentUuid
                else:
                    util.SMlog(
                        'Cannot introduce {} with invalid VDI type {}'
                        .format(vdi_uuid, vdi_type)
                    )
                    continue

                util.SMlog(
                    'Introducing VDI {} '.format(vdi_uuid) +
                    ' (name={}, virtual_size={}, physical_size={})'.format(
                        name_label,
                        volume_info.virtual_size,
                        volume_info.physical_size
                    )
                )

                vdi_ref = xenapi.VDI.db_introduce(
                    vdi_uuid,
                    name_label,
                    volume_metadata.get(NAME_DESCRIPTION_TAG) or '',
                    self.sr_ref,
                    type,
                    False,  # sharable
                    bool(volume_metadata.get(READ_ONLY_TAG)),
                    {},  # other_config
                    vdi_uuid,  # location
                    {},  # xenstore_data
                    sm_config,
                    managed,
                    str(volume_info.virtual_size),
                    str(volume_info.physical_size)
                )

                is_a_snapshot = volume_metadata.get(IS_A_SNAPSHOT_TAG)
                xenapi.VDI.set_is_a_snapshot(vdi_ref, bool(is_a_snapshot))
                if is_a_snapshot:
                    xenapi.VDI.set_snapshot_time(
                        vdi_ref,
                        xmlrpclib.DateTime(
                            volume_metadata[SNAPSHOT_TIME_TAG] or
                            '19700101T00:00:00Z'
                        )
                    )

                    snap_uuid = volume_metadata[SNAPSHOT_OF_TAG]
                    if snap_uuid in vdi_to_snaps:
                        vdi_to_snaps[snap_uuid].append(vdi_uuid)
                    else:
                        vdi_to_snaps[snap_uuid] = [vdi_uuid]

            # 4.b. Add the VDI in the list.
            vdi = self.vdi(vdi_uuid)
            self.vdis[vdi_uuid] = vdi

            if vdi.vdi_type == vhdutil.VDI_TYPE_VHD:
                vdi.sm_config_override['key_hash'] = \
                    self._vhdutil.get_key_hash(vdi_uuid)

            # 4.c. Update CBT status of disks either just added
            # or already in XAPI.
            cbt_uuid = volume_metadata.get(CBTLOG_TAG)
            if cbt_uuid in cbt_vdis:
                vdi_ref = xenapi.VDI.get_by_uuid(vdi_uuid)
                xenapi.VDI.set_cbt_enabled(vdi_ref, True)
                # For existing VDIs, update local state too.
                # Scan in base class SR updates existing VDIs
                # again based on local states.
                self.vdis[vdi_uuid].cbt_enabled = True
                cbt_vdis.remove(cbt_uuid)

        # 5. Now set the snapshot statuses correctly in XAPI.
        for src_uuid in vdi_to_snaps:
            try:
                src_ref = xenapi.VDI.get_by_uuid(src_uuid)
            except Exception:
                # The source VDI no longer exists, continue.
                continue

            for snap_uuid in vdi_to_snaps[src_uuid]:
                try:
                    # This might fail in cases where its already set.
                    snap_ref = xenapi.VDI.get_by_uuid(snap_uuid)
                    xenapi.VDI.set_snapshot_of(snap_ref, src_ref)
                except Exception as e:
                    util.SMlog('Setting snapshot failed: {}'.format(e))

        # TODO: Check correctly how to use CBT.
        # Update cbt_enabled on the right VDI, check LVM/FileSR code.

        # 6. If we have items remaining in this list,
        # they are cbt_metadata VDI that XAPI doesn't know about.
        # Add them to self.vdis and they'll get added to the DB.
        for cbt_uuid in cbt_vdis:
            new_vdi = self.vdi(cbt_uuid)
            new_vdi.ty = 'cbt_metadata'
            new_vdi.cbt_enabled = True
            self.vdis[cbt_uuid] = new_vdi

        # 7. Update virtual allocation, build geneology and remove useless VDIs
        self.virtual_allocation = 0

        # 8. Build geneology.
        geneology = {}

        for vdi_uuid, vdi in self.vdis.items():
            if vdi.parent:
                if vdi.parent in self.vdis:
                    self.vdis[vdi.parent].read_only = True
                if vdi.parent in geneology:
                    geneology[vdi.parent].append(vdi_uuid)
                else:
                    geneology[vdi.parent] = [vdi_uuid]
            if not vdi.hidden:
                self.virtual_allocation += vdi.utilisation

        # 9. Remove all hidden leaf nodes to avoid introducing records that
        # will be GC'ed.
        for vdi_uuid in self.vdis.keys():
            if vdi_uuid not in geneology and self.vdis[vdi_uuid].hidden:
                util.SMlog(
                    'Scan found hidden leaf ({}), ignoring'.format(vdi_uuid)
                )
                del self.vdis[vdi_uuid]

    # --------------------------------------------------------------------------
    # Journals.
    # --------------------------------------------------------------------------

    def _get_vdi_path_and_parent(self, vdi_uuid, volume_name):
        try:
            device_path = self._linstor.build_device_path(volume_name)
            if not util.pathexists(device_path):
                return (None, None)

            # If it's a RAW VDI, there is no parent.
            volume_metadata = self._linstor.get_volume_metadata(vdi_uuid)
            vdi_type = volume_metadata[VDI_TYPE_TAG]
            if vdi_type == vhdutil.VDI_TYPE_RAW:
                return (device_path, None)

            # Otherwise it's a VHD and a parent can exist.
            if not self._vhdutil.check(vdi_uuid):
                return (None, None)

            vhd_info = self._vhdutil.get_vhd_info(vdi_uuid)
            if vhd_info:
                return (device_path, vhd_info.parentUuid)
        except Exception as e:
            util.SMlog(
                'Failed to get VDI path and parent, ignoring: {}'
                .format(e)
            )
        return (None, None)

    def _undo_all_journal_transactions(self):
        util.SMlog('Undoing all journal transactions...')
        self.lock.acquire()
        try:
            self._handle_interrupted_inflate_ops()
            self._handle_interrupted_clone_ops()
            pass
        finally:
            self.lock.release()

    def _handle_interrupted_inflate_ops(self):
        transactions = self._journaler.get_all(LinstorJournaler.INFLATE)
        for vdi_uuid, old_size in transactions.items():
            self._handle_interrupted_inflate(vdi_uuid, old_size)
            self._journaler.remove(LinstorJournaler.INFLATE, vdi_uuid)

    def _handle_interrupted_clone_ops(self):
        transactions = self._journaler.get_all(LinstorJournaler.CLONE)
        for vdi_uuid, old_size in transactions.items():
            self._handle_interrupted_clone(vdi_uuid, old_size)
            self._journaler.remove(LinstorJournaler.CLONE, vdi_uuid)

    def _handle_interrupted_inflate(self, vdi_uuid, old_size):
        util.SMlog(
            '*** INTERRUPTED INFLATE OP: for {} ({})'
            .format(vdi_uuid, old_size)
        )

        vdi = self.vdis.get(vdi_uuid)
        if not vdi:
            util.SMlog('Cannot deflate missing VDI {}'.format(vdi_uuid))
            return

        current_size = self._linstor.get_volume_info(self.uuid).virtual_size
        util.zeroOut(
            vdi.path,
            current_size - vhdutil.VHD_FOOTER_SIZE,
            vhdutil.VHD_FOOTER_SIZE
        )
        deflate(vdi_uuid, vdi.path, old_size, current_size)

    def _handle_interrupted_clone(
        self, vdi_uuid, clone_info, force_undo=False
    ):
        util.SMlog(
            '*** INTERRUPTED CLONE OP: for {} ({})'
            .format(vdi_uuid, clone_info)
        )

        base_uuid, snap_uuid = clone_info.split('_')

        # Use LINSTOR data because new VDIs may not be in the XAPI.
        volume_names = self._linstor.volumes_with_name

        # Check if we don't have a base VDI. (If clone failed at startup.)
        if base_uuid not in volume_names:
            if vdi_uuid in volume_names:
                util.SMlog('*** INTERRUPTED CLONE OP: nothing to do')
                return
            raise util.SMException(
                'Base copy {} not present, but no original {} found'
                .format(base_uuid, vdi_uuid)
            )

        if force_undo:
            util.SMlog('Explicit revert')
            self._undo_clone(
                 volume_names, vdi_uuid, base_uuid, snap_uuid
            )
            return

        # If VDI or snap uuid is missing...
        if vdi_uuid not in volume_names or \
                (snap_uuid and snap_uuid not in volume_names):
            util.SMlog('One or both leaves missing => revert')
            self._undo_clone(volume_names, vdi_uuid, base_uuid, snap_uuid)
            return

        vdi_path, vdi_parent_uuid = self._get_vdi_path_and_parent(
            vdi_uuid, volume_names[vdi_uuid]
        )
        snap_path, snap_parent_uuid = self._get_vdi_path_and_parent(
            snap_uuid, volume_names[snap_uuid]
        )

        if not vdi_path or (snap_uuid and not snap_path):
            util.SMlog('One or both leaves invalid (and path(s)) => revert')
            self._undo_clone(volume_names, vdi_uuid, base_uuid, snap_uuid)
            return

        util.SMlog('Leaves valid but => revert')
        self._undo_clone(volume_names, vdi_uuid, base_uuid, snap_uuid)

    def _undo_clone(self, volume_names, vdi_uuid, base_uuid, snap_uuid):
        base_path = self._linstor.build_device_path(volume_names[base_uuid])
        base_metadata = self._linstor.get_volume_metadata(base_uuid)
        base_type = base_metadata[VDI_TYPE_TAG]

        if not util.pathexists(base_path):
            util.SMlog('Base not found! Exit...')
            util.SMlog('*** INTERRUPTED CLONE OP: rollback fail')
            return

        # Un-hide the parent.
        self._linstor.update_volume_metadata(base_uuid, {READ_ONLY_TAG: False})
        if base_type == vhdutil.VDI_TYPE_VHD:
            vhd_info = self._vhdutil.get_vhd_info(base_uuid, False)
            if vhd_info.hidden:
                vhdutil.setHidden(base_path, False)
        elif base_type == vhdutil.VDI_TYPE_RAW and \
                base_metadata.get(HIDDEN_TAG):
            self._linstor.update_volume_metadata(
                base_uuid, {HIDDEN_TAG: False}
            )

        # Remove the child nodes.
        if snap_uuid and snap_uuid in volume_names:
            util.SMlog('Destroying snap {}...'.format(snap_uuid))
            snap_metadata = self._linstor.get_volume_metadata(snap_uuid)

            if snap_metadata.get(VDI_TYPE_TAG) != vhdutil.VDI_TYPE_VHD:
                raise util.SMException('Clone {} not VHD'.format(snap_uuid))

            try:
                self._linstor.destroy_volume(snap_uuid)
            except Exception as e:
                util.SMlog(
                    'Cannot destroy snap {} during undo clone: {}'
                    .format(snap_uuid, e)
                )

        if vdi_uuid in volume_names:
            try:
                util.SMlog('Destroying {}...'.format(vdi_uuid))
                self._linstor.destroy_volume(vdi_uuid)
            except Exception as e:
                util.SMlog(
                    'Cannot destroy VDI {} during undo clone: {}'
                    .format(vdi_uuid, e)
                )
                # We can get an exception like this:
                # "Shutdown of the DRBD resource 'XXX failed", so the
                # volume info remains... The problem is we can't rename
                # properly the base VDI below this line, so we must change the
                # UUID of this bad VDI before.
                self._linstor.update_volume_uuid(
                    vdi_uuid, 'DELETED_' + vdi_uuid, force=True
                )

        # Rename!
        self._linstor.update_volume_uuid(base_uuid, vdi_uuid)

        # Inflate to the right size.
        if base_type == vhdutil.VDI_TYPE_VHD:
            vdi = self.vdi(vdi_uuid)
            volume_size = compute_volume_size(vdi.size, vdi.vdi_type)
            inflate(
                self._journaler, self._linstor, vdi_uuid, vdi.path,
                volume_size, vdi.capacity
            )
            self.vdis[vdi_uuid] = vdi

        # At this stage, tapdisk and SM vdi will be in paused state. Remove
        # flag to facilitate vm deactivate.
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'paused')

        util.SMlog('*** INTERRUPTED CLONE OP: rollback success')

    # --------------------------------------------------------------------------
    # Misc.
    # --------------------------------------------------------------------------

    def _ensure_space_available(self, amount_needed):
        space_available = self._linstor.max_volume_size_allowed
        if (space_available < amount_needed):
            util.SMlog(
                'Not enough space! Free space: {}, need: {}'.format(
                    space_available, amount_needed
                )
            )
            raise xs_errors.XenError('SRNoSpace')

    def _kick_gc(self):
        # Don't bother if an instance already running. This is just an
        # optimization to reduce the overhead of forking a new process if we
        # don't have to, but the process will check the lock anyways.
        lock = Lock(cleanup.LOCK_TYPE_RUNNING, self.uuid)
        if not lock.acquireNoblock():
            if not cleanup.should_preempt(self.session, self.uuid):
                util.SMlog('A GC instance already running, not kicking')
                return

            util.SMlog('Aborting currently-running coalesce of garbage VDI')
            try:
                if not cleanup.abort(self.uuid, soft=True):
                    util.SMlog('The GC has already been scheduled to re-start')
            except util.CommandException as e:
                if e.code != errno.ETIMEDOUT:
                    raise
                util.SMlog('Failed to abort the GC')
        else:
            lock.release()

        util.SMlog('Kicking GC')
        cleanup.gc(self.session, self.uuid, True)

# ==============================================================================
# LinstorSr VDI
# ==============================================================================


class LinstorVDI(VDI.VDI):
    # Warning: Not the same values than vhdutil.VDI_TYPE_*.
    # These values represents the types given on the command line.
    TYPE_RAW = 'raw'
    TYPE_VHD = 'vhd'

    MAX_SIZE = 2 * 1024 * 1024 * 1024 * 1024  # Max VHD size.

    # Metadata size given to the "S" param of vhd-util create.
    # "-S size (MB) for metadata preallocation".
    # Increase the performance when resize is called.
    MAX_METADATA_VIRT_SIZE = 2 * 1024 * 1024

    # --------------------------------------------------------------------------
    # VDI methods.
    # --------------------------------------------------------------------------

    def load(self, vdi_uuid):
        self._lock = self.sr.lock
        self._exists = True
        self._linstor = self.sr._linstor

        # Update hidden parent property.
        self.hidden = False

        def raise_bad_load(e):
            util.SMlog(
                'Got exception in LinstorVDI.load: {}'.format(e)
            )
            util.SMlog(traceback.format_exc())
            raise xs_errors.XenError(
                'VDIUnavailable',
                opterr='Could not load {} because: {}'.format(self.uuid, e)
            )

        #  Try to load VDI.
        try:
            if (
                self.sr.srcmd.cmd == 'vdi_attach_from_config' or
                self.sr.srcmd.cmd == 'vdi_detach_from_config'
            ) and self.sr.srcmd.params['vdi_uuid'] == self.uuid:
                self.vdi_type = vhdutil.VDI_TYPE_RAW
                self.path = self.sr.srcmd.params['vdi_path']
            else:
                self._determine_type_and_path()
                self._load_this()

            util.SMlog('VDI {} loaded! (path={}, hidden={})'.format(
                self.uuid, self.path, self.hidden
            ))
        except LinstorVolumeManagerError as e:
            # 1. It may be a VDI deletion.
            if e.code == LinstorVolumeManagerError.ERR_VOLUME_NOT_EXISTS:
                if self.sr.srcmd.cmd == 'vdi_delete':
                    self.deleted = True
                    return

            # 2. Or maybe a creation.
            if self.sr.srcmd.cmd == 'vdi_create':
                # Set type attribute of VDI parent class.
                # We use VHD by default.
                self.vdi_type = vhdutil.VDI_TYPE_VHD
                self._key_hash = None  # Only used in create.

                self._exists = False
                vdi_sm_config = self.sr.srcmd.params.get('vdi_sm_config')
                if vdi_sm_config is not None:
                    type = vdi_sm_config.get('type')
                    if type is not None:
                        if type == self.TYPE_RAW:
                            self.vdi_type = vhdutil.VDI_TYPE_RAW
                        elif type == self.TYPE_VHD:
                            self.vdi_type = vhdutil.VDI_TYPE_VHD
                        else:
                            raise xs_errors.XenError(
                                'VDICreate',
                                opterr='Invalid VDI type {}'.format(type)
                            )
                    if self.vdi_type == vhdutil.VDI_TYPE_VHD:
                        self._key_hash = vdi_sm_config.get('key_hash')

                # For the moment we don't have a path.
                self._update_device_name(None)
                return
            raise_bad_load(e)
        except Exception as e:
            raise_bad_load(e)

    def create(self, sr_uuid, vdi_uuid, size):
        # Usage example:
        # xe vdi-create sr-uuid=39a5826b-5a90-73eb-dd09-51e3a116f937
        # name-label="linstor-vdi-1" virtual-size=4096MiB sm-config:type=vhd

        # 1. Check if we are on the master and if the VDI doesn't exist.
        util.SMlog('LinstorVDI.create for {}'.format(self.uuid))
        if self._exists:
            raise xs_errors.XenError('VDIExists')

        assert self.uuid
        assert self.ty
        assert self.vdi_type

        # 2. Compute size and check space available.
        size = vhdutil.validate_and_round_vhd_size(long(size))
        util.SMlog('LinstorVDI.create: type={}, size={}'.format(
            self.vdi_type, size
        ))

        volume_size = compute_volume_size(size, self.vdi_type)
        self.sr._ensure_space_available(volume_size)

        # 3. Set sm_config attribute of VDI parent class.
        self.sm_config = self.sr.srcmd.params['vdi_sm_config']

        # 4. Create!
        failed = False
        try:
            self._linstor.create_volume(
                self.uuid, volume_size, persistent=False
            )
            volume_info = self._linstor.get_volume_info(self.uuid)

            self._update_device_name(volume_info.name)

            if self.vdi_type == vhdutil.VDI_TYPE_RAW:
                self.size = volume_info.virtual_size
            else:
                vhdutil.create(
                    self.path, size, False, self.MAX_METADATA_VIRT_SIZE
                )
                self.size = self.sr._vhdutil.get_size_virt(self.uuid)

            if self._key_hash:
                vhdutil.setKey(self.path, self._key_hash)

            # Because vhdutil commands modify the volume data,
            # we must retrieve a new time the utilisation size.
            volume_info = self._linstor.get_volume_info(self.uuid)

            volume_metadata = {
                NAME_LABEL_TAG: util.to_plain_string(self.label),
                NAME_DESCRIPTION_TAG: util.to_plain_string(self.description),
                IS_A_SNAPSHOT_TAG: False,
                SNAPSHOT_OF_TAG: '',
                SNAPSHOT_TIME_TAG: '',
                TYPE_TAG: self.ty,
                VDI_TYPE_TAG: self.vdi_type,
                READ_ONLY_TAG: bool(self.read_only),
                METADATA_OF_POOL_TAG: ''
            }
            self._linstor.set_volume_metadata(self.uuid, volume_metadata)
            self._linstor.mark_volume_as_persistent(self.uuid)
        except util.CommandException as e:
            failed = True
            raise xs_errors.XenError(
                'VDICreate', opterr='error {}'.format(e.code)
            )
        except Exception as e:
            failed = True
            raise xs_errors.XenError('VDICreate', opterr='error {}'.format(e))
        finally:
            if failed:
                util.SMlog('Unable to create VDI {}'.format(self.uuid))
                try:
                    self._linstor.destroy_volume(self.uuid)
                except Exception as e:
                    util.SMlog(
                        'Ignoring exception after fail in LinstorVDI.create: '
                        '{}'.format(e)
                    )

        self.utilisation = volume_info.physical_size
        self.sm_config['vdi_type'] = self.vdi_type

        self.ref = self._db_introduce()
        self.sr._update_stats(volume_info.virtual_size)

        return VDI.VDI.get_params(self)

    def delete(self, sr_uuid, vdi_uuid, data_only=False):
        util.SMlog('LinstorVDI.delete for {}'.format(self.uuid))
        if self.attached:
            raise xs_errors.XenError('VDIInUse')

        if self.deleted:
            return super(LinstorVDI, self).delete(
                sr_uuid, vdi_uuid, data_only
            )

        vdi_ref = self.sr.srcmd.params['vdi_ref']
        if not self.session.xenapi.VDI.get_managed(vdi_ref):
            raise xs_errors.XenError(
                'VDIDelete',
                opterr='Deleting non-leaf node not permitted'
            )

        try:
            # Remove from XAPI and delete from LINSTOR.
            self._linstor.destroy_volume(self.uuid)
            if not data_only:
                self._db_forget()

            self.sr.lock.cleanupAll(vdi_uuid)
        except Exception as e:
            util.SMlog(
                'Failed to remove the volume (maybe is leaf coalescing) '
                'for {} err: {}'.format(self.uuid, e)
            )
            raise xs_errors.XenError('VDIDelete', opterr=str(e))

        if self.uuid in self.sr.vdis:
            del self.sr.vdis[self.uuid]

        # TODO: Check size after delete.
        self.sr._update_stats(-self.capacity)
        self.sr._kick_gc()
        return super(LinstorVDI, self).delete(sr_uuid, vdi_uuid, data_only)

    def attach(self, sr_uuid, vdi_uuid):
        util.SMlog('LinstorVDI.attach for {}'.format(self.uuid))
        if (
            self.sr.srcmd.cmd != 'vdi_attach_from_config' or
            self.sr.srcmd.params['vdi_uuid'] != self.uuid
        ) and self.sr._journaler.has_entries(self.uuid):
            raise xs_errors.XenError(
                'VDIUnavailable',
                opterr='Interrupted operation detected on this VDI, '
                'scan SR first to trigger auto-repair'
            )

        writable = 'args' not in self.sr.srcmd.params or \
            self.sr.srcmd.params['args'][0] == 'true'

        # We need to inflate the volume if we don't have enough place
        # to mount the VHD image. I.e. the volume capacity must be greater
        # than the VHD size + bitmap size.
        need_inflate = True
        if self.vdi_type == vhdutil.VDI_TYPE_RAW or not writable or \
                self.capacity >= compute_volume_size(self.size, self.vdi_type):
            need_inflate = False

        if need_inflate:
            try:
                self._prepare_thin(True)
            except Exception as e:
                raise xs_errors.XenError(
                    'VDIUnavailable',
                    opterr='Failed to attach VDI during "prepare thin": {}'
                    .format(e)
                )

        if not util.pathexists(self.path):
            raise xs_errors.XenError(
                'VDIUnavailable', opterr='Could not find: {}'.format(self.path)
            )

        if not hasattr(self, 'xenstore_data'):
            self.xenstore_data = {}

        # TODO: Is it useful?
        self.xenstore_data.update(scsiutil.update_XS_SCSIdata(
            self.uuid, scsiutil.gen_synthetic_page_data(self.uuid)
        ))

        self.xenstore_data['storage-type'] = LinstorSR.DRIVER_TYPE

        self.attached = True

        return VDI.VDI.attach(self, self.sr.uuid, self.uuid)

    def detach(self, sr_uuid, vdi_uuid):
        util.SMlog('LinstorVDI.detach for {}'.format(self.uuid))
        self.attached = False

        if self.vdi_type == vhdutil.VDI_TYPE_RAW:
            return

        # The VDI is already deflated if the VHD image size + metadata is
        # equal to the LINSTOR volume size.
        volume_size = compute_volume_size(self.size, self.vdi_type)
        already_deflated = self.capacity <= volume_size

        if already_deflated:
            util.SMlog(
                'VDI {} already deflated (old volume size={}, volume size={})'
                .format(self.uuid, self.capacity, volume_size)
            )

        need_deflate = True
        if already_deflated:
            need_deflate = False
        elif self.sr._provisioning == 'thick':
            need_deflate = False

            vdi_ref = self.sr.srcmd.params['vdi_ref']
            if self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref):
                need_deflate = True

        if need_deflate:
            try:
                self._prepare_thin(False)
            except Exception as e:
                raise xs_errors.XenError(
                    'VDIUnavailable',
                    opterr='Failed to detach VDI during "prepare thin": {}'
                    .format(e)
                )

    def resize(self, sr_uuid, vdi_uuid, size):
        util.SMlog('LinstorVDI.resize for {}'.format(self.uuid))
        if self.hidden:
            raise xs_errors.XenError('VDIUnavailable', opterr='hidden VDI')

        if size < self.size:
            util.SMlog(
                'vdi_resize: shrinking not supported: '
                '(current size: {}, new size: {})'.format(self.size, size)
            )
            raise xs_errors.XenError('VDISize', opterr='shrinking not allowed')

        # Compute the virtual VHD size.
        size = vhdutil.validate_and_round_vhd_size(long(size))

        if size == self.size:
            return VDI.VDI.get_params(self)

        # Compute the LINSTOR volume size.
        new_volume_size = compute_volume_size(size, self.vdi_type)
        if self.vdi_type == vhdutil.VDI_TYPE_RAW:
            old_volume_size = self.size
        else:
            old_volume_size = self.capacity
            if self.sr._provisioning == 'thin':
                # VDI is currently deflated, so keep it deflated.
                new_volume_size = old_volume_size
        assert new_volume_size >= old_volume_size

        space_needed = new_volume_size - old_volume_size
        self.sr._ensure_space_available(space_needed)

        old_capacity = self.capacity
        if self.vdi_type == vhdutil.VDI_TYPE_RAW:
            self._linstor.resize(self.uuid, new_volume_size)
        else:
            if new_volume_size != old_volume_size:
                inflate(
                    self.sr._journaler, self._linstor, self.uuid, self.path,
                    new_volume_size, old_volume_size
                )
            vhdutil.setSizeVirtFast(self.path, size)

        # Reload size attributes.
        self._load_this()

        vdi_ref = self.sr.srcmd.params['vdi_ref']
        self.session.xenapi.VDI.set_virtual_size(vdi_ref, str(self.size))
        self.session.xenapi.VDI.set_physical_utilisation(
            vdi_ref, str(self.utilisation)
        )
        self.sr._update_stats(self.capacity - old_capacity)
        return VDI.VDI.get_params(self)

    def clone(self, sr_uuid, vdi_uuid):
        return self._do_snapshot(sr_uuid, vdi_uuid, VDI.SNAPSHOT_DOUBLE)

    def compose(self, sr_uuid, vdi1, vdi2):
        util.SMlog('VDI.compose for {} -> {}'.format(vdi2, vdi1))
        if self.vdi_type != vhdutil.VDI_TYPE_VHD:
            raise xs_errors.XenError('Unimplemented')

        parent_uuid = vdi1
        parent_path = self._linstor.get_device_path(parent_uuid)

        # We must pause tapdisk to correctly change the parent. Otherwise we
        # have a readonly error.
        # See: https://github.com/xapi-project/xen-api/blob/b3169a16d36dae0654881b336801910811a399d9/ocaml/xapi/storage_migrate.ml#L928-L929
        # and: https://github.com/xapi-project/xen-api/blob/b3169a16d36dae0654881b336801910811a399d9/ocaml/xapi/storage_migrate.ml#L775

        if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, self.uuid):
            raise util.SMException('Failed to pause VDI {}'.format(self.uuid))
        try:
            vhdutil.setParent(self.path, parent_path, False)
            vhdutil.setHidden(parent_path)
            self.sr.session.xenapi.VDI.set_managed(
                self.sr.srcmd.params['args'][0], False
            )
        finally:
            blktap2.VDI.tap_unpause(self.session, self.sr.uuid, self.uuid)

        if not blktap2.VDI.tap_refresh(self.session, self.sr.uuid, self.uuid):
            raise util.SMException(
                'Failed to refresh VDI {}'.format(self.uuid)
            )

        util.SMlog('Compose done')

    def generate_config(self, sr_uuid, vdi_uuid):
        """
        Generate the XML config required to attach and activate
        a VDI for use when XAPI is not running. Attach and
        activation is handled by vdi_attach_from_config below.
        """

        util.SMlog('LinstorVDI.generate_config for {}'.format(self.uuid))

        if not self.path or not util.pathexists(self.path):
            available = False
            # Try to refresh symlink path...
            try:
                self.path = self._linstor.get_device_path(vdi_uuid)
                available = util.pathexists(self.path)
            except Exception:
                pass
            if not available:
                raise xs_errors.XenError('VDIUnavailable')

        resp = {}
        resp['device_config'] = self.sr.dconf
        resp['sr_uuid'] = sr_uuid
        resp['vdi_uuid'] = self.uuid
        resp['sr_sm_config'] = self.sr.sm_config
        resp['vdi_path'] = self.path
        resp['command'] = 'vdi_attach_from_config'

        config = xmlrpclib.dumps(tuple([resp]), 'vdi_attach_from_config')
        return xmlrpclib.dumps((config,), "", True)

    def attach_from_config(self, sr_uuid, vdi_uuid):
        """
        Attach and activate a VDI using config generated by
        vdi_generate_config above. This is used for cases such as
        the HA state-file and the redo-log.
        """

        util.SMlog('LinstorVDI.attach_from_config for {}'.format(vdi_uuid))

        try:
            if not util.pathexists(self.sr.path):
                self.sr.attach(sr_uuid)

            if not DRIVER_CONFIG['ATTACH_FROM_CONFIG_WITH_TAPDISK']:
                return self.attach(sr_uuid, vdi_uuid)
        except Exception:
            util.logException('LinstorVDI.attach_from_config')
            raise xs_errors.XenError(
                'SRUnavailable',
                opterr='Unable to attach from config'
            )

    def reset_leaf(self, sr_uuid, vdi_uuid):
        if self.vdi_type != vhdutil.VDI_TYPE_VHD:
            raise xs_errors.XenError('Unimplemented')

        if not self.sr._vhdutil.has_parent(self.uuid):
            raise util.SMException(
                'ERROR: VDI {} has no parent, will not reset contents'
                .format(self.uuid)
            )

        vhdutil.killData(self.path)

    def _load_this(self):
        volume_metadata = self._linstor.get_volume_metadata(self.uuid)
        volume_info = self._linstor.get_volume_info(self.uuid)

        # Contains the physical size used on all disks.
        # When LINSTOR LVM driver is used, the size should be similar to
        # virtual size (i.e. the LINSTOR max volume size).
        # When LINSTOR Thin LVM driver is used, the used physical size should
        # be lower than virtual size at creation.
        # The physical size increases after each write in a new block.
        self.utilisation = volume_info.physical_size
        self.capacity = volume_info.virtual_size

        if self.vdi_type == vhdutil.VDI_TYPE_RAW:
            self.hidden = int(volume_metadata.get(HIDDEN_TAG) or 0)
            self.size = volume_info.virtual_size
            self.parent = ''
        else:
            vhd_info = self.sr._vhdutil.get_vhd_info(self.uuid)
            self.hidden = vhd_info.hidden
            self.size = vhd_info.sizeVirt
            self.parent = vhd_info.parentUuid

        if self.hidden:
            self.managed = False

        self.label = volume_metadata.get(NAME_LABEL_TAG) or ''
        self.description = volume_metadata.get(NAME_DESCRIPTION_TAG) or ''

        # Update sm_config_override of VDI parent class.
        self.sm_config_override = {'vhd-parent': self.parent or None}

    def _mark_hidden(self, hidden=True):
        if self.hidden == hidden:
            return

        if self.vdi_type == vhdutil.VDI_TYPE_VHD:
            vhdutil.setHidden(self.path, hidden)
        else:
            self._linstor.update_volume_metadata(self.uuid, {
                HIDDEN_TAG: hidden
            })
        self.hidden = hidden

    def update(self, sr_uuid, vdi_uuid):
        xenapi = self.session.xenapi
        vdi_ref = xenapi.VDI.get_by_uuid(self.uuid)

        volume_metadata = {
            NAME_LABEL_TAG: util.to_plain_string(
                xenapi.VDI.get_name_label(vdi_ref)
            ),
            NAME_DESCRIPTION_TAG: util.to_plain_string(
                xenapi.VDI.get_name_description(vdi_ref)
            )
        }

        try:
            self._linstor.update_volume_metadata(self.uuid, volume_metadata)
        except LinstorVolumeManagerError as e:
            if e.code == LinstorVolumeManagerError.ERR_VOLUME_NOT_EXISTS:
                raise xs_errors.XenError(
                    'VDIUnavailable',
                    opterr='LINSTOR volume {} not found'.format(self.uuid)
                )
            raise xs_errors.XenError('VDIUnavailable', opterr=str(e))

    # --------------------------------------------------------------------------
    # Thin provisioning.
    # --------------------------------------------------------------------------

    def _prepare_thin(self, attach):
        if self.sr._is_master:
            if attach:
                attach_thin(
                    self.session, self.sr._journaler, self._linstor,
                    self.sr.uuid, self.uuid
                )
            else:
                detach_thin(
                    self.session, self._linstor, self.sr.uuid, self.uuid
                )
        else:
            fn = 'attach' if attach else 'detach'

            # We assume the first pool is always the one currently in use.
            pools = self.session.xenapi.pool.get_all()
            master = self.session.xenapi.pool.get_master(pools[0])
            args = {
                'groupName': self.sr._group_name,
                'srUuid': self.sr.uuid,
                'vdiUuid': self.uuid
            }
            ret = self.session.xenapi.host.call_plugin(
                    master, self.sr.MANAGER_PLUGIN, fn, args
            )
            util.SMlog(
                'call-plugin ({} with {}) returned: {}'.format(fn, args, ret)
            )
            if ret == 'False':
                raise xs_errors.XenError(
                    'VDIUnavailable',
                    opterr='Plugin {} failed'.format(self.sr.MANAGER_PLUGIN)
                )

        # Reload size attrs after inflate or deflate!
        self._load_this()
        self.sr._update_physical_size()

        vdi_ref = self.sr.srcmd.params['vdi_ref']
        self.session.xenapi.VDI.set_physical_utilisation(
            vdi_ref, str(self.utilisation)
        )

        self.session.xenapi.SR.set_physical_utilisation(
            self.sr.sr_ref, str(self.sr.physical_utilisation)
        )

    # --------------------------------------------------------------------------
    # Generic helpers.
    # --------------------------------------------------------------------------

    def _determine_type_and_path(self):
        """
        Determine whether this is a RAW or a VHD VDI.
        """

        # 1. Check vdi_ref and vdi_type in config.
        try:
            vdi_ref = self.session.xenapi.VDI.get_by_uuid(self.uuid)
            if vdi_ref:
                sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
                vdi_type = sm_config.get('vdi_type')
                if vdi_type:
                    # Update parent fields.
                    self.vdi_type = vdi_type
                    self.sm_config_override = sm_config
                    self._update_device_name(
                        self._linstor.get_volume_name(self.uuid)
                    )
                    return
        except Exception:
            pass

        # 2. Otherwise use the LINSTOR volume manager directly.
        # It's probably a new VDI created via snapshot.
        volume_metadata = self._linstor.get_volume_metadata(self.uuid)
        self.vdi_type = volume_metadata.get(VDI_TYPE_TAG)
        if not self.vdi_type:
            raise xs_errors.XenError(
                'VDIUnavailable',
                opterr='failed to get vdi_type in metadata'
            )
        self._update_device_name(
            self._linstor.get_volume_name(self.uuid)
        )

    def _update_device_name(self, device_name):
        self._device_name = device_name

        # Mark path of VDI parent class.
        if device_name:
            self.path = self._linstor.build_device_path(self._device_name)
        else:
            self.path = None

    def _create_snapshot(self, snap_uuid, snap_of_uuid=None):
        """
        Snapshot self and return the snapshot VDI object.
        """

        # 1. Create a new LINSTOR volume with the same size than self.
        snap_path = self._linstor.shallow_clone_volume(
            self.uuid, snap_uuid, persistent=False
        )

        # 2. Write the snapshot content.
        is_raw = (self.vdi_type == vhdutil.VDI_TYPE_RAW)
        vhdutil.snapshot(
            snap_path, self.path, is_raw, self.MAX_METADATA_VIRT_SIZE
        )

        # 3. Get snapshot parent.
        snap_parent = self.sr._vhdutil.get_parent(snap_uuid)

        # 4. Update metadata.
        util.SMlog('Set VDI {} metadata of snapshot'.format(snap_uuid))
        volume_metadata = {
            NAME_LABEL_TAG: util.to_plain_string(self.label),
            NAME_DESCRIPTION_TAG: util.to_plain_string(self.description),
            IS_A_SNAPSHOT_TAG: bool(snap_of_uuid),
            SNAPSHOT_OF_TAG: snap_of_uuid,
            SNAPSHOT_TIME_TAG: '',
            TYPE_TAG: self.ty,
            VDI_TYPE_TAG: vhdutil.VDI_TYPE_VHD,
            READ_ONLY_TAG: False,
            METADATA_OF_POOL_TAG: ''
        }
        self._linstor.set_volume_metadata(snap_uuid, volume_metadata)

        # 5. Set size.
        snap_vdi = LinstorVDI(self.sr, snap_uuid)
        if not snap_vdi._exists:
            raise xs_errors.XenError('VDISnapshot')

        volume_info = self._linstor.get_volume_info(snap_uuid)

        snap_vdi.size = self.sr._vhdutil.get_size_virt(snap_uuid)
        snap_vdi.utilisation = volume_info.physical_size

        # 6. Update sm config.
        snap_vdi.sm_config = {}
        snap_vdi.sm_config['vdi_type'] = snap_vdi.vdi_type
        if snap_parent:
            snap_vdi.sm_config['vhd-parent'] = snap_parent
            snap_vdi.parent = snap_parent

        snap_vdi.label = self.label
        snap_vdi.description = self.description

        self._linstor.mark_volume_as_persistent(snap_uuid)

        return snap_vdi

    # --------------------------------------------------------------------------
    # Implement specific SR methods.
    # --------------------------------------------------------------------------

    def _rename(self, oldpath, newpath):
        # TODO: I'm not sure... Used by CBT.
        volume_uuid = self._linstor.get_volume_uuid_from_device_path(oldpath)
        self._linstor.update_volume_name(volume_uuid, newpath)

    def _do_snapshot(
        self, sr_uuid, vdi_uuid, snap_type, secondary=None, cbtlog=None
    ):
        # If cbt enabled, save file consistency state.
        if cbtlog is not None:
            if blktap2.VDI.tap_status(self.session, vdi_uuid):
                consistency_state = False
            else:
                consistency_state = True
            util.SMlog(
                'Saving log consistency state of {} for vdi: {}'
                .format(consistency_state, vdi_uuid)
            )
        else:
            consistency_state = None

        if self.vdi_type != vhdutil.VDI_TYPE_VHD:
            raise xs_errors.XenError('Unimplemented')

        if not blktap2.VDI.tap_pause(self.session, sr_uuid, vdi_uuid):
            raise util.SMException('Failed to pause VDI {}'.format(vdi_uuid))
        try:
            return self._snapshot(snap_type, cbtlog, consistency_state)
        finally:
            blktap2.VDI.tap_unpause(self.session, sr_uuid, vdi_uuid, secondary)

    def _snapshot(self, snap_type, cbtlog=None, cbt_consistency=None):
        util.SMlog(
            'LinstorVDI._snapshot for {} (type {})'
            .format(self.uuid, snap_type)
        )

        # 1. Checks...
        if self.hidden:
            raise xs_errors.XenError('VDIClone', opterr='hidden VDI')

        depth = self.sr._vhdutil.get_depth(self.uuid)
        if depth == -1:
            raise xs_errors.XenError(
                'VDIUnavailable',
                opterr='failed to get VHD depth'
            )
        elif depth >= vhdutil.MAX_CHAIN_SIZE:
            raise xs_errors.XenError('SnapshotChainTooLong')

        volume_path = self.path
        if not util.pathexists(volume_path):
            raise xs_errors.XenError(
                'EIO',
                opterr='IO error checking path {}'.format(volume_path)
            )

        # 2. Create base and snap uuid (if required) and a journal entry.
        base_uuid = util.gen_uuid()
        snap_uuid = None

        if snap_type == VDI.SNAPSHOT_DOUBLE:
            snap_uuid = util.gen_uuid()

        clone_info = '{}_{}'.format(base_uuid, snap_uuid)

        active_uuid = self.uuid
        self.sr._journaler.create(
            LinstorJournaler.CLONE, active_uuid, clone_info
        )

        try:
            # 3. Self becomes the new base.
            # The device path remains the same.
            self._linstor.update_volume_uuid(self.uuid, base_uuid)
            self.uuid = base_uuid
            self.location = self.uuid
            self.read_only = True
            self.managed = False

            # 4. Create snapshots (new active and snap).
            active_vdi = self._create_snapshot(active_uuid)

            snap_vdi = None
            if snap_type == VDI.SNAPSHOT_DOUBLE:
                snap_vdi = self._create_snapshot(snap_uuid, active_uuid)

            self.label = 'base copy'
            self.description = ''

            # 5. Mark the base VDI as hidden so that it does not show up
            # in subsequent scans.
            self._mark_hidden()
            self._linstor.update_volume_metadata(
                self.uuid, {READ_ONLY_TAG: True}
            )

            # 6. We must update the new active VDI with the "paused" and
            # "host_" properties. Why? Because the original VDI has been
            # paused and we we must unpause it after the snapshot.
            # See: `tap_unpause` in `blktap2.py`.
            vdi_ref = self.session.xenapi.VDI.get_by_uuid(active_uuid)
            sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
            for key in filter(
                lambda x: x == 'paused' or x.startswith('host_'),
                sm_config.keys()
            ):
                active_vdi.sm_config[key] = sm_config[key]

            # 7. Verify parent locator field of both children and
            # delete base if unused.
            introduce_parent = True
            try:
                snap_parent = None
                if snap_vdi:
                    snap_parent = snap_vdi.parent

                if active_vdi.parent != self.uuid and (
                    snap_type == VDI.SNAPSHOT_SINGLE or
                    snap_type == VDI.SNAPSHOT_INTERNAL or
                    snap_parent != self.uuid
                ):
                    util.SMlog(
                        'Destroy unused base volume: {} (path={})'
                        .format(self.uuid, self.path)
                    )
                    introduce_parent = False
                    self._linstor.destroy_volume(self.uuid)
            except Exception as e:
                util.SMlog('Ignoring exception: {}'.format(e))
                pass

            # 8. Introduce the new VDI records.
            if snap_vdi:
                # If the parent is encrypted set the key_hash for the
                # new snapshot disk.
                vdi_ref = self.sr.srcmd.params['vdi_ref']
                sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
                # TODO: Maybe remove key_hash support.
                if 'key_hash' in sm_config:
                    snap_vdi.sm_config['key_hash'] = sm_config['key_hash']
                # If we have CBT enabled on the VDI,
                # set CBT status for the new snapshot disk.
                if cbtlog:
                    snap_vdi.cbt_enabled = True

            if snap_vdi:
                snap_vdi_ref = snap_vdi._db_introduce()
                util.SMlog(
                    'vdi_clone: introduced VDI: {} ({})'
                    .format(snap_vdi_ref, snap_vdi.uuid)
                )
            if introduce_parent:
                base_vdi_ref = self._db_introduce()
                self.session.xenapi.VDI.set_managed(base_vdi_ref, False)
                util.SMlog(
                    'vdi_clone: introduced VDI: {} ({})'
                    .format(base_vdi_ref, self.uuid)
                )
                self._linstor.update_volume_metadata(self.uuid, {
                    NAME_LABEL_TAG: util.to_plain_string(self.label),
                    NAME_DESCRIPTION_TAG: util.to_plain_string(
                        self.description
                    ),
                    READ_ONLY_TAG: True,
                    METADATA_OF_POOL_TAG: ''
                })

            # 9. Update cbt files if user created snapshot (SNAPSHOT_DOUBLE)
            if snap_type == VDI.SNAPSHOT_DOUBLE and cbtlog:
                try:
                    self._cbt_snapshot(snap_uuid, cbt_consistency)
                except Exception:
                    # CBT operation failed.
                    # TODO: Implement me.
                    raise

            if snap_type != VDI.SNAPSHOT_INTERNAL:
                self.sr._update_stats(self.capacity)

            # 10. Return info on the new user-visible leaf VDI.
            ret_vdi = snap_vdi
            if not ret_vdi:
                ret_vdi = self
            if not ret_vdi:
                ret_vdi = active_vdi

            vdi_ref = self.sr.srcmd.params['vdi_ref']
            self.session.xenapi.VDI.set_sm_config(
                vdi_ref, active_vdi.sm_config
            )
        except Exception as e:
            util.logException('Failed to snapshot!')
            try:
                self.sr._handle_interrupted_clone(
                    active_uuid, clone_info, force_undo=True
                )
                self.sr._journaler.remove(LinstorJournaler.CLONE, active_uuid)
            except Exception as e:
                util.SMlog(
                    'WARNING: Failed to clean up failed snapshot: {}'
                    .format(e)
                )
            raise xs_errors.XenError('VDIClone', opterr=str(e))

        self.sr._journaler.remove(LinstorJournaler.CLONE, active_uuid)

        return ret_vdi.get_params()

# ------------------------------------------------------------------------------


if __name__ == '__main__':
    SRCommand.run(LinstorSR, DRIVER_INFO)
else:
    SR.registerSR(LinstorSR)
