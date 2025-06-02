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
# Clear the attach status for all VDIs in the given SR on this host.
# Additionally, reset the paused state if this host is the master.

import XenAPI # pylint: disable=import-error
from sm import cleanup
from sm.core import util
from sm.core import lock


def reset_sr(session, host_uuid, sr_uuid, is_sr_master):
    from sm.vhdutil import LOCK_TYPE_SR

    cleanup.abort(sr_uuid)

    gc_lock = lock.Lock(lock.LOCK_TYPE_GC_RUNNING, sr_uuid)
    sr_lock = lock.Lock(LOCK_TYPE_SR, sr_uuid)
    gc_lock.acquire()
    sr_lock.acquire()

    sr_ref = session.xenapi.SR.get_by_uuid(sr_uuid)

    host_ref = session.xenapi.host.get_by_uuid(host_uuid)
    host_key = "host_%s" % host_ref

    util.SMlog("RESET for SR %s (master: %s)" % (sr_uuid, is_sr_master))

    vdi_recs = session.xenapi.VDI.get_all_records_where( \
            "field \"SR\" = \"%s\"" % sr_ref)

    for vdi_ref, vdi_rec in vdi_recs.items():
        vdi_uuid = vdi_rec["uuid"]
        sm_config = vdi_rec["sm_config"]
        if sm_config.get(host_key):
            util.SMlog("Clearing attached status for VDI %s" % vdi_uuid)
            session.xenapi.VDI.remove_from_sm_config(vdi_ref, host_key)
        if is_sr_master and sm_config.get("paused"):
            util.SMlog("Clearing paused status for VDI %s" % vdi_uuid)
            session.xenapi.VDI.remove_from_sm_config(vdi_ref, "paused")

    sr_lock.release()
    gc_lock.release()


def reset_vdi(session, vdi_uuid, force, term_output=True, writable=True):
    vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
    vdi_rec = session.xenapi.VDI.get_record(vdi_ref)
    sm_config = vdi_rec["sm_config"]
    host_ref = None
    clean = True
    for key, val in sm_config.items():
        if key.startswith("host_"):
            host_ref = key[len("host_"):]
            host_uuid = None
            host_invalid = False
            host_str = host_ref
            try:
                host_rec = session.xenapi.host.get_record(host_ref)
                host_uuid = host_rec["uuid"]
                host_str = "%s (%s)" % (host_uuid, host_rec["name_label"])
            except XenAPI.Failure as e:
                msg = "Invalid host: %s (%s)" % (host_ref, e)
                util.SMlog(msg)
                if term_output:
                    print(msg)
                host_invalid = True

            if host_invalid:
                session.xenapi.VDI.remove_from_sm_config(vdi_ref, key)
                msg = "Invalid host: Force-cleared %s for %s on host %s" % \
                        (val, vdi_uuid, host_str)
                util.SMlog(msg)
                if term_output:
                    print(msg)
                continue

            if force:
                session.xenapi.VDI.remove_from_sm_config(vdi_ref, key)
                msg = "Force-cleared %s for %s on host %s" % \
                        (val, vdi_uuid, host_str)
                util.SMlog(msg)
                if term_output:
                    print(msg)
                continue

            ret = session.xenapi.host.call_plugin(
                    host_ref, "on-slave", "is_open",
                    {"vdiUuid": vdi_uuid, "srRef": vdi_rec["SR"]})
            if ret != "False":
                util.SMlog("VDI %s is still open on host %s, not resetting" % \
                        (vdi_uuid, host_str))
                if term_output:
                    print("ERROR: VDI %s is still open on host %s" % \
                            (vdi_uuid, host_str))
                if writable:
                    return False
                else:
                    clean = False
            else:
                session.xenapi.VDI.remove_from_sm_config(vdi_ref, key)
                msg = "Cleared %s for %s on host %s" % \
                        (val, vdi_uuid, host_str)
                util.SMlog(msg)
                if term_output:
                    print(msg)

    if not host_ref:
        msg = "VDI %s is not marked as attached anywhere, nothing to do" \
            % vdi_uuid
        util.SMlog(msg)
        if term_output:
            print(msg)
    return clean
