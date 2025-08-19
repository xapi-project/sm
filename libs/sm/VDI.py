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
# VDI: Base class for virtual disk instances
#

from sm import cleanup
from sm import SR
import xmlrpc.client
from sm.core import xs_errors
from sm.core import util
from sm.core.lock import Lock
from sm import vhdutil
from sm import cbtutil
import os
import base64
from sm.constants import CBTLOG_TAG
from bitarray import bitarray
import uuid


SM_CONFIG_PASS_THROUGH_FIELDS = ["base_mirror", "key_hash"]

SNAPSHOT_SINGLE = 1  # true snapshot: 1 leaf, 1 read-only parent
SNAPSHOT_DOUBLE = 2  # regular snapshot/clone that creates 2 leaves
SNAPSHOT_INTERNAL = 3  # SNAPSHOT_SINGLE but don't update SR's virtual allocation
CBT_BLOCK_SIZE = (64 * 1024)


def VDIMetadataSize(type, virtualsize):
    size = 0
    if type == 'vhd':
        size_mb = virtualsize // (1024 * 1024)
        #Footer + footer copy + header + possible CoW parent locator fields
        size = 3 * 1024

        # BAT 4 Bytes per block segment
        size += (size_mb // 2) * 4
        size = util.roundup(512, size)

        # BATMAP 1 bit per block segment
        size += (size_mb // 2) // 8
        size = util.roundup(4096, size)

        # Segment bitmaps + Page align offsets
        size += (size_mb // 2) * 4096

    return size


class VDI(object):
    """Virtual Disk Instance descriptor.

    Attributes:
      uuid: string, globally unique VDI identifier conforming to OSF DEC 1.1
      label: string, user-generated tag string for identifyng the VDI
      description: string, longer user generated description string
      size: int, virtual size in bytes of this VDI
      utilisation: int, actual size in Bytes of data on disk that is 
        utilised. For non-sparse disks, utilisation == size
      vdi_type: string, disk type, e.g. raw file, partition
      parent: VDI object, parent backing VDI if this disk is a 
     CoW instance
      shareable: boolean, does this disk support multiple writer instances?
        e.g. shared OCFS disk
      attached: boolean, whether VDI is attached
      read_only: boolean, whether disk is read-only.
    """

    def __init__(self, sr, uuid):
        self.sr = sr
        # Don't set either the UUID or location to None- no good can
        # ever come of this.
        if uuid is not None:
            self.uuid = uuid
            self.location = uuid
            self.path = None
        else:
            # We assume that children class initializors calling without
            # uuid will set these attributes themselves somewhere. They
            # are VDIs whose physical paths/locations have no direct
            # connections with their UUID strings (e.g. ISOSR, udevSR,
            # SHMSR). So we avoid overwriting these attributes here.
            pass
        # deliberately not initialised self.sm_config so that it is
        # ommitted from the XML output

        self.label = ''
        self.description = ''
        self.vbds = []
        self.size = 0
        self._block_size = -1
        self.utilisation = 0
        self.vdi_type = ''
        self.has_child = 0
        self.parent = None
        self.shareable = False
        self.attached = False
        self.status = 0
        self.read_only = False
        self.xenstore_data = {}
        self.deleted = False
        self.session = sr.session
        self.managed = True
        self.sm_config_override = {}
        self.sm_config_keep = ["key_hash"]
        self.ty = "user"
        self.cbt_enabled = False

        self.load(uuid)

    @property
    def block_size(self):
        if self._block_size < 0:
            self._block_size = vhdutil.getBlockSize(self.path)
        return self._block_size

    @staticmethod
    def from_uuid(session, vdi_uuid):

        _VDI = session.xenapi.VDI
        vdi_ref = _VDI.get_by_uuid(vdi_uuid)
        sr_ref = _VDI.get_SR(vdi_ref)

        _SR = session.xenapi.SR
        sr_uuid = _SR.get_uuid(sr_ref)

        sr = SR.SR.from_uuid(session, sr_uuid)

        sr.srcmd.params['vdi_ref'] = vdi_ref
        return sr.vdi(vdi_uuid)

    def create(self, sr_uuid, vdi_uuid, size):
        """Create a VDI of size <Size> MB on the given SR. 

        This operation IS NOT idempotent and will fail if the UUID
        already exists or if there is insufficient space. The vdi must
        be explicitly attached via the attach() command following
        creation. The actual disk size created may be larger than the
        requested size if the substrate requires a size in multiples
        of a certain extent size. The SR must be queried for the exact
        size.
        """
        raise xs_errors.XenError('Unimplemented')

    def update(self, sr_uuid, vdi_uuid):
        """Query and update the configuration of a particular VDI.

        Given an SR and VDI UUID, this operation returns summary statistics
        on the named VDI. Note the XenAPI VDI object will exist when
        this call is made.
        """
        # no-op unless individual backends implement it
        return

    def introduce(self, sr_uuid, vdi_uuid):
        """Explicitly introduce a particular VDI.

        Given an SR and VDI UUID and a disk location (passed in via the <conf>
        XML), this operation verifies the existence of the underylying disk
        object and then creates the XenAPI VDI object.
        """
        raise xs_errors.XenError('Unimplemented')

    def attach(self, sr_uuid, vdi_uuid):
        """Initiate local access to the VDI. Initialises any device
        state required to access the VDI.

        This operation IS idempotent and should succeed if the VDI can be
        attached or if the VDI is already attached.

        Returns:
          string, local device path.
        """
        struct = {'params': self.path,
                   'xenstore_data': (self.xenstore_data or {})}
        return xmlrpc.client.dumps((struct, ), "", True)

    def detach(self, sr_uuid, vdi_uuid):
        """Remove local access to the VDI. Destroys any device 
        state initialised via the vdi.attach() command.

        This operation is idempotent.
        """
        raise xs_errors.XenError('Unimplemented')

    def clone(self, sr_uuid, vdi_uuid):
        """Create a mutable instance of the referenced VDI.

        This operation is not idempotent and will fail if the UUID
        already exists or if there is insufficient space. The SRC VDI
        must be in a detached state and deactivated. Upon successful
        creation of the clone, the clone VDI must be explicitly
        attached via vdi.attach(). If the driver does not support
        cloning this operation should raise SRUnsupportedOperation.

        Arguments:
        Raises:
          SRUnsupportedOperation
        """
        raise xs_errors.XenError('Unimplemented')

    def resize_online(self, sr_uuid, vdi_uuid, size):
        """Resize the given VDI which may have active VBDs, which have
        been paused for the duration of this call."""
        raise xs_errors.XenError('Unimplemented')

    def generate_config(self, sr_uuid, vdi_uuid):
        """Generate the XML config required to activate a VDI for use
        when XAPI is not running. Activation is handled by the
        vdi_attach_from_config() SMAPI call.
        """
        raise xs_errors.XenError('Unimplemented')

    def compose(self, sr_uuid, vdi1, vdi2):
        """Layer the updates from [vdi2] onto [vdi1], calling the result
        [vdi2].

        Raises:
          SRUnsupportedOperation
        """
        raise xs_errors.XenError('Unimplemented')

    def attach_from_config(self, sr_uuid, vdi_uuid):
        """Activate a VDI based on the config passed in on the CLI. For
        use when XAPI is not running. The config is generated by the
        Activation is handled by the vdi_generate_config() SMAPI call.
        """
        raise xs_errors.XenError('Unimplemented')

    def _do_snapshot(self, sr_uuid, vdi_uuid, snapType,
                     cloneOp=False, secondary=None, cbtlog=None):
        raise xs_errors.XenError('Unimplemented')

    def _delete_cbt_log(self):
        raise xs_errors.XenError('Unimplemented')

    def _rename(self, old, new):
        raise xs_errors.XenError('Unimplemented')

    def _cbt_log_exists(self, logpath):
        """Check if CBT log file exists

        Must be implemented by all classes inheriting from base VDI class
        """
        raise xs_errors.XenError('Unimplemented')

    def resize(self, sr_uuid, vdi_uuid, size):
        """Resize the given VDI to size <size> MB. Size can
        be any valid disk size greater than [or smaller than]
        the current value.

        This operation IS idempotent and should succeed if the VDI can
        be resized to the specified value or if the VDI is already the
        specified size. The actual disk size created may be larger
        than the requested size if the substrate requires a size in
        multiples of a certain extent size. The SR must be queried for
        the exact size. This operation does not modify the contents on
        the disk such as the filesystem.  Responsibility for resizing
        the FS is left to the VM administrator. [Reducing the size of
        the disk is a very dangerous operation and should be conducted
        very carefully.] Disk contents should always be backed up in
        advance.
        """
        raise xs_errors.XenError('Unimplemented')

    def resize_cbt(self, sr_uuid, vdi_uuid, size):
        """Resize the given VDI to size <size> MB. Size can
                be any valid disk size greater than [or smaller than]
                the current value.

                This operation IS idempotent and should succeed if the VDI can
                be resized to the specified value or if the VDI is already the
                specified size. The actual disk size created may be larger
                than the requested size if the substrate requires a size in
                multiples of a certain extent size. The SR must be queried for
                the exact size. This operation does not modify the contents on
                the disk such as the filesystem.  Responsibility for resizing
                the FS is left to the VM administrator. [Reducing the size of
                the disk is a very dangerous operation and should be conducted
                very carefully.] Disk contents should always be backed up in
                advance.
                """
        try:
            if self._get_blocktracking_status():
                logpath = self._get_cbt_logpath(vdi_uuid)
                self._cbt_op(vdi_uuid, cbtutil.set_cbt_size, logpath, size)
        except util.CommandException as ex:
            alert_name = "VDI_CBT_RESIZE_FAILED"
            alert_str = ("Resizing of CBT metadata for disk %s failed."
                         % vdi_uuid)
            self._disable_cbt_on_error(alert_name, alert_str)

    def delete(self, sr_uuid, vdi_uuid, data_only=False):
        """Delete this VDI.

        This operation IS idempotent and should succeed if the VDI
        exists and can be deleted or if the VDI does not exist. It is
        the responsibility of the higher-level management tool to
        ensure that the detach() operation has been explicitly called
        prior to deletion, otherwise the delete() will fail if the
        disk is still attached.
        """
        from sm import blktap2

        if data_only == False and self._get_blocktracking_status():
            logpath = self._get_cbt_logpath(vdi_uuid)
            parent_uuid = self._cbt_op(vdi_uuid, cbtutil.get_cbt_parent,
                                       logpath)
            parent_path = self._get_cbt_logpath(parent_uuid)
            child_uuid = self._cbt_op(vdi_uuid, cbtutil.get_cbt_child, logpath)
            child_path = self._get_cbt_logpath(child_uuid)

            lock = Lock("cbtlog", str(vdi_uuid))

            if self._cbt_log_exists(parent_path):
                self._cbt_op(parent_uuid, cbtutil.set_cbt_child,
                             parent_path, child_uuid)

            if self._cbt_log_exists(child_path):
                self._cbt_op(child_uuid, cbtutil.set_cbt_parent,
                             child_path, parent_uuid)
                lock.acquire()
                paused_for_coalesce = False
                try:
                    # Coalesce contents of bitmap with child's bitmap
                    # Check if child bitmap is currently attached
                    consistent = self._cbt_op(child_uuid,
                                              cbtutil.get_cbt_consistency,
                                              child_path)
                    if not consistent:
                        if not blktap2.VDI.tap_pause(self.session,
                                                     sr_uuid, child_uuid):
                            raise util.SMException("failed to pause VDI %s")
                        paused_for_coalesce = True
                    self._activate_cbt_log(self._get_cbt_logname(vdi_uuid))
                    self._cbt_op(child_uuid, cbtutil.coalesce_bitmap,
                                 logpath, child_path)
                    lock.release()
                except util.CommandException:
                    # If there is an exception in coalescing,
                    # CBT log file is not deleted and pointers are reset
                    # to what they were
                    util.SMlog("Exception in coalescing bitmaps on VDI delete,"
                               " restoring to previous state")
                    try:
                        if self._cbt_log_exists(parent_path):
                            self._cbt_op(parent_uuid, cbtutil.set_cbt_child,
                                         parent_path, vdi_uuid)
                        if self._cbt_log_exists(child_path):
                            self._cbt_op(child_uuid, cbtutil.set_cbt_parent,
                                         child_path, vdi_uuid)
                    finally:
                        lock.release()
                        lock.cleanup("cbtlog", str(vdi_uuid))
                    return
                finally:
                    # Unpause tapdisk if it wasn't originally paused
                    if paused_for_coalesce:
                        blktap2.VDI.tap_unpause(self.session, sr_uuid,
                                                child_uuid)
            lock.acquire()
            try:
                self._delete_cbt_log()
            finally:
                lock.release()
                lock.cleanup("cbtlog", str(vdi_uuid))

    def snapshot(self, sr_uuid, vdi_uuid):
        """Save an immutable copy of the referenced VDI.

        This operation IS NOT idempotent and will fail if the UUID
        already exists or if there is insufficient space. The vdi must
        be explicitly attached via the vdi_attach() command following
        creation. If the driver does not support snapshotting this
        operation should raise SRUnsupportedOperation

        Arguments:
        Raises:
          SRUnsupportedOperation
        """
        # logically, "snapshot" should mean SNAPSHOT_SINGLE and "clone" should
        # mean "SNAPSHOT_DOUBLE", but in practice we have to do SNAPSHOT_DOUBLE
        # in both cases, unless driver_params overrides it
        snapType = SNAPSHOT_DOUBLE
        if self.sr.srcmd.params['driver_params'].get("type"):
            if self.sr.srcmd.params['driver_params']["type"] == "single":
                snapType = SNAPSHOT_SINGLE
            elif self.sr.srcmd.params['driver_params']["type"] == "internal":
                snapType = SNAPSHOT_INTERNAL

        secondary = None
        if self.sr.srcmd.params['driver_params'].get("mirror"):
            secondary = self.sr.srcmd.params['driver_params']["mirror"]

        if self._get_blocktracking_status():
            cbtlog = self._get_cbt_logpath(self.uuid)
        else:
            cbtlog = None
        return  self._do_snapshot(sr_uuid, vdi_uuid, snapType,
                                  secondary=secondary, cbtlog=cbtlog)

    def activate(self, sr_uuid, vdi_uuid):
        """Activate VDI - called pre tapdisk open"""
        if self._get_blocktracking_status():
            if 'args' in self.sr.srcmd.params:
                read_write = self.sr.srcmd.params['args'][0]
                if read_write == "false":
                    # Disk is being attached in RO mode,
                    # don't attach metadata log file
                    return None

            lock = Lock("cbtlog", str(vdi_uuid))
            lock.acquire()

            try:
                logpath = self._get_cbt_logpath(vdi_uuid)
                logname = self._get_cbt_logname(vdi_uuid)

                # Activate CBT log file, if required
                self._activate_cbt_log(logname)
            finally:
                lock.release()

            # Check and update consistency
            consistent = self._cbt_op(vdi_uuid, cbtutil.get_cbt_consistency,
                                      logpath)
            if not consistent:
                alert_name = "VDI_CBT_METADATA_INCONSISTENT"
                alert_str = ("Changed Block Tracking metadata is inconsistent"
                             " for disk %s." % vdi_uuid)
                self._disable_cbt_on_error(alert_name, alert_str)
                return None

            self._cbt_op(self.uuid, cbtutil.set_cbt_consistency,
                         logpath, False)
            return {'cbtlog': logpath}
        return None

    def deactivate(self, sr_uuid, vdi_uuid):
        """Deactivate VDI - called post tapdisk close"""
        if self._get_blocktracking_status():
            lock = Lock("cbtlog", str(vdi_uuid))
            lock.acquire()

            try:
                logpath = self._get_cbt_logpath(vdi_uuid)
                logname = self._get_cbt_logname(vdi_uuid)
                self._cbt_op(vdi_uuid, cbtutil.set_cbt_consistency, logpath, True)
                # Finally deactivate log file
                self._deactivate_cbt_log(logname)
            finally:
                lock.release()

    def get_params(self):
        """
        Returns:
          XMLRPC response containing a single struct with fields
          'location' and 'uuid'
        """
        struct = {'location': self.location,
                   'uuid': self.uuid}
        return xmlrpc.client.dumps((struct, ), "", True)

    def load(self, vdi_uuid):
        """Post-init hook"""
        pass

    def _db_introduce(self):
        uuid = util.default(self, "uuid", lambda: util.gen_uuid())
        sm_config = util.default(self, "sm_config", lambda: {})
        if "vdi_sm_config" in self.sr.srcmd.params:
            for key in SM_CONFIG_PASS_THROUGH_FIELDS:
                val = self.sr.srcmd.params["vdi_sm_config"].get(key)
                if val:
                    sm_config[key] = val
        ty = util.default(self, "ty", lambda: "user")
        is_a_snapshot = util.default(self, "is_a_snapshot", lambda: False)
        metadata_of_pool = util.default(self, "metadata_of_pool", lambda: "OpaqueRef:NULL")
        snapshot_time = util.default(self, "snapshot_time", lambda: "19700101T00:00:00Z")
        snapshot_of = util.default(self, "snapshot_of", lambda: "OpaqueRef:NULL")
        cbt_enabled = util.default(self, "cbt_enabled", lambda: False)
        vdi = self.sr.session.xenapi.VDI.db_introduce(uuid, self.label, self.description, self.sr.sr_ref, ty, self.shareable, self.read_only, {}, self.location, {}, sm_config, self.managed, str(self.size), str(self.utilisation), metadata_of_pool, is_a_snapshot, xmlrpc.client.DateTime(snapshot_time), snapshot_of, cbt_enabled)
        return vdi

    def _db_forget(self):
        self.sr.forget_vdi(self.uuid)

    def _override_sm_config(self, sm_config):
        for key, val in self.sm_config_override.items():
            if val == sm_config.get(key):
                continue
            if val:
                util.SMlog("_override_sm_config: %s: %s -> %s" % \
                        (key, sm_config.get(key), val))
                sm_config[key] = val
            elif key in sm_config:
                util.SMlog("_override_sm_config: del %s" % key)
                del sm_config[key]

    def _db_update_sm_config(self, ref, sm_config):
        from sm import cleanup
        # List of sm-config keys that should not be modifed by db_update
        smconfig_protected_keys = [
            cleanup.VDI.DB_VDI_PAUSED,
            cleanup.VDI.DB_VHD_BLOCKS,
            cleanup.VDI.DB_VDI_RELINKING,
            cleanup.VDI.DB_VDI_ACTIVATING]

        current_sm_config = self.sr.session.xenapi.VDI.get_sm_config(ref)
        for key, val in sm_config.items():
            if (key.startswith("host_") or
                key in smconfig_protected_keys):
                continue
            if sm_config.get(key) != current_sm_config.get(key):
                util.SMlog("_db_update_sm_config: %s sm-config:%s %s->%s" % \
                        (self.uuid, key, current_sm_config.get(key), val))
                self.sr.session.xenapi.VDI.remove_from_sm_config(ref, key)
                self.sr.session.xenapi.VDI.add_to_sm_config(ref, key, val)

        for key in current_sm_config.keys():
            if (key.startswith("host_") or
                key in smconfig_protected_keys or
                key in self.sm_config_keep):
                continue
            if not sm_config.get(key):
                util.SMlog("_db_update_sm_config: %s del sm-config:%s" % \
                        (self.uuid, key))
                self.sr.session.xenapi.VDI.remove_from_sm_config(ref, key)

    def _db_update(self):
        vdi = self.sr.session.xenapi.VDI.get_by_uuid(self.uuid)
        self.sr.session.xenapi.VDI.set_virtual_size(vdi, str(self.size))
        self.sr.session.xenapi.VDI.set_physical_utilisation(vdi, str(self.utilisation))
        self.sr.session.xenapi.VDI.set_read_only(vdi, self.read_only)
        sm_config = util.default(self, "sm_config", lambda: {})
        self._override_sm_config(sm_config)
        self._db_update_sm_config(vdi, sm_config)
        self.sr.session.xenapi.VDI.set_cbt_enabled(vdi,
            self._get_blocktracking_status())

    def in_sync_with_xenapi_record(self, x):
        """Returns true if this VDI is in sync with the supplied XenAPI record"""
        if self.location != util.to_plain_string(x['location']):
            util.SMlog("location %s <> %s" % (self.location, x['location']))
            return False
        if self.read_only != x['read_only']:
            util.SMlog("read_only %s <> %s" % (self.read_only, x['read_only']))
            return False
        if str(self.size) != x['virtual_size']:
            util.SMlog("virtual_size %s <> %s" % (self.size, x['virtual_size']))
            return False
        if str(self.utilisation) != x['physical_utilisation']:
            util.SMlog("utilisation %s <> %s" % (self.utilisation, x['physical_utilisation']))
            return False
        sm_config = util.default(self, "sm_config", lambda: {})
        if set(sm_config.keys()) != set(x['sm_config'].keys()):
            util.SMlog("sm_config %s <> %s" % (repr(sm_config), repr(x['sm_config'])))
            return False
        for k in sm_config.keys():
            if sm_config[k] != x['sm_config'][k]:
                util.SMlog("sm_config %s <> %s" % (repr(sm_config), repr(x['sm_config'])))
                return False
        if self.cbt_enabled != x['cbt_enabled']:
            util.SMlog("cbt_enabled %s <> %s" % (
                self.cbt_enabled, x['cbt_enabled']))
            return False
        return True

    def configure_blocktracking(self, sr_uuid, vdi_uuid, enable):
        """Function for configuring blocktracking"""
        from sm import blktap2
        vdi_ref = self.sr.srcmd.params['vdi_ref']

        # Check if raw VDI or snapshot
        if self.vdi_type == vhdutil.VDI_TYPE_RAW or \
            self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref):
            raise xs_errors.XenError('VDIType',
                                     opterr='Raw VDI or snapshot not permitted')

        # Check if already enabled
        if self._get_blocktracking_status() == enable:
            return

        # Save disk state before pause
        disk_state = blktap2.VDI.tap_status(self.session, vdi_uuid)

        if not blktap2.VDI.tap_pause(self.session, sr_uuid, vdi_uuid):
            error = "Failed to pause VDI %s" % vdi_uuid
            raise xs_errors.XenError('CBTActivateFailed', opterr=error)
        logfile = None

        try:
            if enable:
                try:
                    # Check available space
                    self._ensure_cbt_space()
                    logfile = self._create_cbt_log()
                    # Set consistency
                    if disk_state:
                        util.SMlog("Setting consistency of cbtlog file to False for VDI: %s"
                                   % self.uuid)
                        logpath = self._get_cbt_logpath(self.uuid)
                        self._cbt_op(self.uuid, cbtutil.set_cbt_consistency,
                                     logpath, False)
                except Exception as error:
                    self._delete_cbt_log()
                    raise xs_errors.XenError('CBTActivateFailed',
                                             opterr=str(error))
            else:
                lock = Lock("cbtlog", str(vdi_uuid))
                lock.acquire()
                try:
                    # Find parent of leaf metadata file, if any,
                    # and nullify its successor
                    logpath = self._get_cbt_logpath(self.uuid)
                    parent = self._cbt_op(self.uuid,
                                          cbtutil.get_cbt_parent, logpath)
                    self._delete_cbt_log()
                    parent_path = self._get_cbt_logpath(parent)
                    if self._cbt_log_exists(parent_path):
                        self._cbt_op(parent, cbtutil.set_cbt_child,
                                     parent_path, uuid.UUID(int=0))
                except Exception as error:
                    raise xs_errors.XenError('CBTDeactivateFailed', str(error))
                finally:
                    lock.release()
                    lock.cleanup("cbtlog", str(vdi_uuid))
        finally:
            blktap2.VDI.tap_unpause(self.session, sr_uuid, vdi_uuid)

    def data_destroy(self, sr_uuid, vdi_uuid):
        """Delete the data associated with a CBT enabled snapshot

        Can only be called for a snapshot VDI on a VHD chain that has
        had CBT enabled on it at some point. The latter is enforced
        by upper layers
        """

        vdi_ref = self.sr.srcmd.params['vdi_ref']
        if not self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref):
            raise xs_errors.XenError('VDIType',
                                     opterr='Only allowed for snapshot VDIs')

        self.delete(sr_uuid, vdi_uuid, data_only=True)

    def list_changed_blocks(self):
        """ List all changed blocks """
        vdi_from = self.uuid
        params = self.sr.srcmd.params
        _VDI = self.session.xenapi.VDI
        vdi_to = _VDI.get_uuid(params['args'][0])
        sr_uuid = params['sr_uuid']

        if vdi_from == vdi_to:
            raise xs_errors.XenError('CBTChangedBlocksError',
                                     "Source and target VDI are same")

        # Check 1: Check if CBT is enabled on VDIs and they are related
        if (self._get_blocktracking_status(vdi_from) and
                self._get_blocktracking_status(vdi_to)):
            merged_bitmap = None
            curr_vdi = vdi_from
            vdi_size = 0
            logpath = self._get_cbt_logpath(curr_vdi)

            # Starting at log file after "vdi_from", traverse the CBT chain
            # through child pointers until one of the following is true
            #   * We've reached destination VDI
            #   * We've reached end of CBT chain originating at "vdi_from"
            while True:
                # Check if we have reached end of CBT chain
                next_vdi = self._cbt_op(curr_vdi, cbtutil.get_cbt_child,
                                        logpath)
                if not self._cbt_log_exists(self._get_cbt_logpath(next_vdi)):
                    # VDIs are not part of the same metadata chain
                    break
                else:
                    curr_vdi = next_vdi

                logpath = self._get_cbt_logpath(curr_vdi)
                curr_vdi_size = self._cbt_op(curr_vdi,
                                             cbtutil.get_cbt_size, logpath)
                util.SMlog("DEBUG: Processing VDI %s of size %d"
                           % (curr_vdi, curr_vdi_size))
                curr_bitmap = bitarray()
                curr_bitmap.frombytes(self._cbt_op(curr_vdi,
                                                   cbtutil.get_cbt_bitmap,
                                                   logpath))
                curr_bitmap.bytereverse()
                util.SMlog("Size of bitmap: %d" % len(curr_bitmap))

                expected_bitmap_len = curr_vdi_size // CBT_BLOCK_SIZE
                # This should ideally never happen but fail call to calculate
                # changed blocks instead of returning corrupt data
                if len(curr_bitmap) < expected_bitmap_len:
                    util.SMlog("Size of bitmap %d is less than expected size %d"
                               % (len(curr_bitmap), expected_bitmap_len))
                    raise xs_errors.XenError('CBTMetadataInconsistent',
                                              "Inconsistent bitmaps")

                if merged_bitmap:
                    # Rule out error conditions
                    # 1) New VDI size < original VDI size
                    # 2) New bitmap size < original bitmap size
                    # 3) new VDI size > original VDI size but new bitmap
                    # is not bigger
                    if (curr_vdi_size < vdi_size or
                        len(curr_bitmap) < len(merged_bitmap) or
                        (curr_vdi_size > vdi_size and
                        len(curr_bitmap) <= len(merged_bitmap))):
                        # Return error: Failure to calculate changed blocks
                        util.SMlog("Cannot calculate changed blocks with"
                        "inconsistent bitmap sizes")
                        raise xs_errors.XenError('CBTMetadataInconsistent',
                                                 "Inconsistent bitmaps")

                    # Check if disk has been resized
                    if curr_vdi_size > vdi_size:
                        vdi_size = curr_vdi_size
                        extended_size = len(curr_bitmap) - len(merged_bitmap)
                        # Extend merged_bitmap to match size of curr_bitmap
                        extended_bitmap = extended_size * bitarray('0')
                        merged_bitmap += extended_bitmap

                    # At this point bitmap sizes should be same
                    if (len(curr_bitmap) > len(merged_bitmap) and
                        curr_vdi_size == vdi_size):
                        # This is unusual. Log it but calculate merged
                        # bitmap by truncating new bitmap
                        util.SMlog("Bitmap for %s bigger than other bitmaps"
                        "in chain without change in size" % curr_vdi)
                        curr_bitmap = curr_bitmap[:len(merged_bitmap)]

                    merged_bitmap = merged_bitmap | curr_bitmap
                else:
                    merged_bitmap = curr_bitmap
                    vdi_size = curr_vdi_size

                # Check if we have reached "vdi_to"
                if curr_vdi == vdi_to:
                    encoded_string = base64.b64encode(merged_bitmap.tobytes()).decode()
                    return xmlrpc.client.dumps((encoded_string, ), "", True)
        # TODO: Check 2: If both VDIs still exist,
        # find common ancestor and find difference

        # TODO: VDIs are unrelated
        # return fully populated bitmap size of to VDI

        raise xs_errors.XenError('CBTChangedBlocksError',
                                 "Source and target VDI are unrelated")

    def _cbt_snapshot(self, snapshot_uuid, consistency_state):
        """ CBT snapshot"""
        snap_logpath = self._get_cbt_logpath(snapshot_uuid)
        vdi_logpath = self._get_cbt_logpath(self.uuid)

        # Rename vdi vdi.cbtlog to snapshot.cbtlog
        # and mark it consistent
        self._rename(vdi_logpath, snap_logpath)
        self._cbt_op(snapshot_uuid, cbtutil.set_cbt_consistency,
                     snap_logpath, True)

        #TODO: Make parent detection logic better. Ideally, get_cbt_parent
        # should return None if the parent is set to a UUID made of all 0s.
        # In this case, we don't know the difference between whether it is a
        # NULL UUID or the parent file is missing. See cbtutil for why we can't
        # do this
        parent = self._cbt_op(snapshot_uuid,
                              cbtutil.get_cbt_parent, snap_logpath)
        parent_path = self._get_cbt_logpath(parent)
        if self._cbt_log_exists(parent_path):
            self._cbt_op(parent, cbtutil.set_cbt_child,
                         parent_path, snapshot_uuid)
        try:
            # Ensure enough space for metadata file
            self._ensure_cbt_space()
            # Create new vdi.cbtlog
            self._create_cbt_log()
            # Set previous vdi node consistency status
            if not consistency_state:
                self._cbt_op(self.uuid, cbtutil.set_cbt_consistency,
                             vdi_logpath, consistency_state)
            # Set relationship pointers
            # Save the child of the VDI just snapshotted
            curr_child_uuid = self._cbt_op(snapshot_uuid, cbtutil.get_cbt_child,
                                           snap_logpath)
            self._cbt_op(self.uuid, cbtutil.set_cbt_parent,
                         vdi_logpath, snapshot_uuid)
            # Set child of new vdi to existing child of snapshotted VDI
            self._cbt_op(self.uuid, cbtutil.set_cbt_child,
                         vdi_logpath, curr_child_uuid)
            self._cbt_op(snapshot_uuid, cbtutil.set_cbt_child,
                         snap_logpath, self.uuid)
        except Exception as ex:
            alert_name = "VDI_CBT_SNAPSHOT_FAILED"
            alert_str = ("Creating CBT metadata log for disk %s failed."
                         % self.uuid)
            self._disable_cbt_on_error(alert_name, alert_str)

    def _get_blocktracking_status(self, uuid=None):
        """ Get blocktracking status """
        if not uuid:
            uuid = self.uuid
        if self.vdi_type == vhdutil.VDI_TYPE_RAW:
            return False
        elif 'VDI_CONFIG_CBT' not in util.sr_get_capability(
                self.sr.uuid, session=self.sr.session):
            return False
        logpath = self._get_cbt_logpath(uuid)
        return self._cbt_log_exists(logpath)

    def _set_blocktracking_status(self, vdi_ref, enable):
        """ Set blocktracking status"""
        vdi_config = self.session.xenapi.VDI.get_other_config(vdi_ref)
        if "cbt_enabled" in vdi_config:
            self.session.xenapi.VDI.remove_from_other_config(
                vdi_ref, "cbt_enabled")

        self.session.xenapi.VDI.add_to_other_config(
            vdi_ref, "cbt_enabled", enable)

    def _ensure_cbt_space(self):
        """ Ensure enough CBT space """
        pass

    def _get_cbt_logname(self, uuid):
        """ Get CBT logname """
        logName = "%s.%s" % (uuid, CBTLOG_TAG)
        return logName

    def _get_cbt_logpath(self, uuid):
        """ Get CBT logpath """
        logName = self._get_cbt_logname(uuid)
        return os.path.join(self.sr.path, logName)

    def _create_cbt_log(self):
        """ Create CBT log """
        try:
            logpath = self._get_cbt_logpath(self.uuid)
            vdi_ref = self.sr.srcmd.params['vdi_ref']
            size = self.session.xenapi.VDI.get_virtual_size(vdi_ref)
            #cbtutil.create_cbt_log(logpath, size)
            self._cbt_op(self.uuid, cbtutil.create_cbt_log, logpath, size)
            self._cbt_op(self.uuid, cbtutil.set_cbt_consistency, logpath, True)
        except Exception as e:
            try:
                self._delete_cbt_log()
            except:
                pass
            finally:
                raise e

        return logpath

    def _activate_cbt_log(self, logname):
        """Activate CBT log file

        SR specific Implementation required for VDIs on block-based SRs.
        No-op otherwise
        """
        return False

    def _deactivate_cbt_log(self, logname):
        """Deactivate CBT log file

        SR specific Implementation required for VDIs on block-based SRs.
        No-op otherwise
        """
        pass

    def _cbt_op(self, uuid, func, *args):
        # Lock cbtlog operations
        lock = Lock("cbtlog", str(uuid))
        lock.acquire()

        try:
            logname = self._get_cbt_logname(uuid)
            activated = self._activate_cbt_log(logname)
            ret = func( * args)
            if activated:
                self._deactivate_cbt_log(logname)
            return ret
        finally:
            lock.release()

    def _disable_cbt_on_error(self, alert_name, alert_str):
        util.SMlog(alert_str)
        self._delete_cbt_log()
        vdi_ref = self.sr.srcmd.params['vdi_ref']
        self.sr.session.xenapi.VDI.set_cbt_enabled(vdi_ref, False)
        alert_prio_warning = "3"
        alert_obj = "VDI"
        alert_uuid = str(self.uuid)
        self.sr.session.xenapi.message.create(alert_name,
                                              alert_prio_warning,
                                              alert_obj, alert_uuid,
                                              alert_str)

    def disable_leaf_on_secondary(self, vdi_uuid, secondary=None):
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        self.session.xenapi.VDI.remove_from_other_config(
            vdi_ref, cleanup.VDI.DB_LEAFCLSC)
        if secondary is not None:
            util.SMlog(f"We have secondary for {vdi_uuid}, "
                       "blocking leaf coalesce")
            self.session.xenapi.VDI.add_to_other_config(
                vdi_ref, cleanup.VDI.DB_LEAFCLSC,
                cleanup.VDI.LEAFCLSC_DISABLED)
