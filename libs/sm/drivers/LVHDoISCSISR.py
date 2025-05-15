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
# LVHDoISCSISR: LVHD over ISCSI software initiator SR driver
#               matches with drivers/LVHDoISCSISR
#

from sm import SR
from sm.drivers import LVHDSR
from sm import BaseISCSI
from sm.core import util
from sm.core import scsiutil
from sm import lvutil
import time
import os
import sys
from sm.core import xs_errors
import xmlrpc.client
from sm.core import mpath_cli
from sm.core import iscsi
import glob
import copy
import xml.dom.minidom

CAPABILITIES = ["SR_PROBE", "SR_UPDATE", "SR_METADATA", "SR_TRIM", "SR_CACHING",
                "VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH",
                "VDI_GENERATE_CONFIG", "VDI_CLONE", "VDI_SNAPSHOT",
                "VDI_RESIZE", "ATOMIC_PAUSE", "VDI_RESET_ON_BOOT/2",
                "VDI_UPDATE", "VDI_MIRROR", "VDI_CONFIG_CBT",
                "VDI_ACTIVATE", "VDI_DEACTIVATE"]

CONFIGURATION = [['SCSIid', 'The scsi_id of the destination LUN'], \
                  ['target', 'IP address or hostname of the iSCSI target'], \
                  ['targetIQN', 'The IQN of the target LUN group to be attached'], \
                  ['chapuser', 'The username to be used during CHAP authentication'], \
                  ['chappassword', 'The password to be used during CHAP authentication'], \
                  ['incoming_chapuser', 'The incoming username to be used during bi-directional CHAP authentication (optional)'], \
                  ['incoming_chappassword', 'The incoming password to be used during bi-directional CHAP authentication (optional)'], \
                  ['port', 'The network port number on which to query the target'], \
                  ['multihomed', 'Enable multi-homing to this target, true or false (optional, defaults to same value as host.other_config:multipathing)'], \
                  ['usediscoverynumber', 'The specific iscsi record index to use. (optional)'], \
                  ['allocation', 'Valid values are thick or thin (optional, defaults to thick)']]

DRIVER_INFO = {
    'name': 'LVHD over iSCSI',
    'description': 'SR plugin which represents disks as Logical Volumes within a Volume Group created on an iSCSI LUN',
    'vendor': 'Citrix Systems Inc',
    'copyright': '(C) 2008 Citrix Systems Inc',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
    }


class LVHDoISCSISR(LVHDSR.LVHDSR):
    """LVHD over ISCSI storage repository"""

    @staticmethod
    def handles(type):
        if type == "lvmoiscsi":
            return True
        if type == "lvhdoiscsi":
            return True
        return False

    def load(self, sr_uuid):
        if not sr_uuid:
            # This is a probe call, generate a temp sr_uuid
            sr_uuid = util.gen_uuid()

        # If this is a vdi command, don't initialise SR
        if util.isVDICommand(self.original_srcmd.cmd):
            self.SCSIid = self.dconf['SCSIid']
        else:
            self.create_iscsi_sessions(sr_uuid)

        LVHDSR.LVHDSR.load(self, sr_uuid)

    def create_iscsi_sessions(self, sr_uuid):
        if 'target' in self.original_srcmd.dconf:
            self.original_srcmd.dconf['targetlist'] = self.original_srcmd.dconf['target']
        baseiscsi = BaseISCSI.BaseISCSISR(self.original_srcmd, sr_uuid)
        self.iscsiSRs = []
        self.iscsiSRs.append(baseiscsi)
        saved_exc = None
        targets = self.dconf['target'].split(',')
        if len(targets) > 1 or self.dconf['targetIQN'] == "*":
            # Instantiate multiple sessions
            self.iscsiSRs = []
            if self.dconf['targetIQN'] == "*":
                IQN = "any"
            else:
                IQN = self.dconf['targetIQN']
            dict = {}
            IQNstring = ""
            IQNs = []
            try:
                if 'multiSession' in self.dconf:
                    IQNs = self.dconf['multiSession'].split("|")
                    for IQN in IQNs:
                        if IQN:
                            dict[IQN] = ""
                        else:
                            try:
                                IQNs.remove(IQN)
                            except:
                                # Exceptions are not expected but just in case
                                pass
                    # Order in multiSession must be preserved. It is important for dual-controllers.
                    # IQNstring cannot be built with a dictionary iteration because of this
                    IQNstring = self.dconf['multiSession']
                else:
                    for tgt in targets:
                        try:
                            tgt_ip = util._convertDNS(tgt)
                        except:
                            raise xs_errors.XenError('DNSError')
                        iscsi.ensure_daemon_running_ok(baseiscsi.localIQN)
                        map = iscsi.discovery(tgt_ip, baseiscsi.port, baseiscsi.chapuser, baseiscsi.chappassword, targetIQN=IQN)
                        util.SMlog("Discovery for IP %s returned %s" % (tgt, map))
                        for i in range(0, len(map)):
                            (portal, tpgt, iqn) = map[i]
                            (ipaddr, port) = iscsi.parse_IP_port(portal)
                            try:
                                util._testHost(ipaddr, int(port), 'ISCSITarget')
                            except:
                                util.SMlog("Target Not reachable: (%s:%s)" % (ipaddr, port))
                                continue
                            key = "%s,%s,%s" % (ipaddr, port, iqn)
                            dict[key] = ""
                # Again, do not mess up with IQNs order. Dual controllers will benefit from that
                if IQNstring == "":
                    # Compose the IQNstring first
                    for key in dict.keys():
                        IQNstring += "%s|" % key
                    # Reinitialize and store iterator
                    key_iterator = iter(dict.keys())
                else:
                    key_iterator = IQNs

                # Now load the individual iSCSI base classes
                for key in key_iterator:
                    (ipaddr, port, iqn) = key.split(',')
                    srcmd_copy = copy.deepcopy(self.original_srcmd)
                    srcmd_copy.dconf['target'] = ipaddr
                    srcmd_copy.dconf['targetIQN'] = iqn
                    srcmd_copy.dconf['multiSession'] = IQNstring
                    util.SMlog("Setting targetlist: %s" % srcmd_copy.dconf['targetlist'])
                    self.iscsiSRs.append(BaseISCSI.BaseISCSISR(srcmd_copy, sr_uuid))
                pbd = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
                if pbd is not None and 'multiSession' not in self.dconf:
                    dconf = self.session.xenapi.PBD.get_device_config(pbd)
                    dconf['multiSession'] = IQNstring
                    self.session.xenapi.PBD.set_device_config(pbd, dconf)
            except Exception as exc:
                util.logException("LVHDoISCSISR.load")
                saved_exc = exc
        try:
            self.iscsi = self.iscsiSRs[0]
        except IndexError as exc:
            if isinstance(saved_exc, xs_errors.SROSError):
                raise saved_exc  # pylint: disable-msg=E0702
            elif isinstance(saved_exc, Exception):
                raise xs_errors.XenError('SMGeneral', str(saved_exc))
            else:
                raise xs_errors.XenError('SMGeneral', str(exc))
        # Be extremely careful not to throw exceptions here since this function
        # is the main one used by all operations including probing and creating
        pbd = None
        try:
            pbd = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
        except:
            pass
        # Apart from the upgrade case, user must specify a SCSIid
        if 'SCSIid' not in self.dconf:
            # Dual controller issue
            self.LUNs = {}  # Dict for LUNs from all the iscsi objects
            for ii in range(0, len(self.iscsiSRs)):
                self.iscsi = self.iscsiSRs[ii]
                self._LUNprint(sr_uuid)
                for key in self.iscsi.LUNs:
                    self.LUNs[key] = self.iscsi.LUNs[key]
            self.print_LUNs_XML()
            self.iscsi = self.iscsiSRs[0]  # back to original value
            raise xs_errors.XenError('ConfigSCSIid')
        self.SCSIid = self.dconf['SCSIid']
        # This block checks if the first iscsi target contains the right SCSIid.
        # If not it scans the other iscsi targets because chances are that more
        # than one controller is present
        dev_match = False
        forced_login = False
        # No need to check if only one iscsi target is present
        if len(self.iscsiSRs) == 1:
            pass
        else:
            target_success = False
            attempt_discovery = False
            for iii in range(0, len(self.iscsiSRs)):
                # Check we didn't leave any iscsi session open
                # If exceptions happened before, the cleanup function has worked on the right target.
                if forced_login == True:
                    try:
                        iscsi.ensure_daemon_running_ok(self.iscsi.localIQN)
                        iscsi.logout(self.iscsi.target, self.iscsi.targetIQN)
                        forced_login = False
                    except:
                        raise xs_errors.XenError('ISCSILogout')
                self.iscsi = self.iscsiSRs[iii]
                util.SMlog("path %s" % self.iscsi.path)
                util.SMlog("iscsci data: targetIQN %s, portal %s" % (self.iscsi.targetIQN, self.iscsi.target))
                iscsi.ensure_daemon_running_ok(self.iscsi.localIQN)
                if not iscsi._checkTGT(self.iscsi.targetIQN, self.iscsi.target):
                    attempt_discovery = True
                    try:
                        # Ensure iscsi db has been populated
                        map = iscsi.discovery(
                            self.iscsi.target,
                            self.iscsi.port,
                            self.iscsi.chapuser,
                            self.iscsi.chappassword,
                            targetIQN=self.iscsi.targetIQN)
                        if len(map) == 0:
                            util.SMlog("Discovery for iscsi data targetIQN %s,"
                                       " portal %s returned empty list"
                                       " Trying another path if available" %
                                       (self.iscsi.targetIQN,
                                        self.iscsi.target))
                            continue
                    except:
                        util.SMlog("Discovery failed for iscsi data targetIQN"
                                   " %s, portal %s. Trying another path if"
                                   " available" %
                                   (self.iscsi.targetIQN, self.iscsi.target))
                        continue
                    try:
                        iscsi.login(self.iscsi.target,
                                       self.iscsi.targetIQN,
                                       self.iscsi.chapuser,
                                       self.iscsi.chappassword,
                                       self.iscsi.incoming_chapuser,
                                       self.iscsi.incoming_chappassword,
                                       self.mpath == "true")
                    except:
                        util.SMlog("Login failed for iscsi data targetIQN %s,"
                                   " portal %s. Trying another path"
                                   " if available" %
                                   (self.iscsi.targetIQN, self.iscsi.target))
                        continue
                    target_success = True
                    forced_login = True
                # A session should be active.
                if not util.wait_for_path(self.iscsi.path, BaseISCSI.MAX_TIMEOUT):
                    util.SMlog("%s has no associated LUNs" % self.iscsi.targetIQN)
                    continue
                scsiid_path = "/dev/disk/by-id/scsi-" + self.SCSIid
                if not util.wait_for_path(scsiid_path, BaseISCSI.MAX_TIMEOUT):
                    util.SMlog("%s not found" % scsiid_path)
                    continue
                for file in filter(self.iscsi.match_lun, util.listdir(self.iscsi.path)):
                    lun_path = os.path.join(self.iscsi.path, file)
                    lun_dev = scsiutil.getdev(lun_path)
                    try:
                        lun_scsiid = scsiutil.getSCSIid(lun_dev)
                    except:
                        util.SMlog("getSCSIid failed on %s in iscsi %s: LUN"
                                   " offline or iscsi path down" %
                                   (lun_dev, self.iscsi.path))
                        continue
                    util.SMlog("dev from lun %s %s" % (lun_dev, lun_scsiid))
                    if lun_scsiid == self.SCSIid:
                        util.SMlog("lun match in %s" % self.iscsi.path)
                        dev_match = True
                        # No more need to raise ISCSITarget exception.
                        # Resetting attempt_discovery
                        attempt_discovery = False
                        break
                if dev_match:
                    if iii == 0:
                        break
                    util.SMlog("IQN reordering needed")
                    new_iscsiSRs = []
                    IQNs = {}
                    IQNstring = ""
                    # iscsiSRs can be seen as a circular buffer: the head now is the matching one
                    for kkk in list(range(iii, len(self.iscsiSRs))) + list(range(0, iii)):
                        new_iscsiSRs.append(self.iscsiSRs[kkk])
                        ipaddr = self.iscsiSRs[kkk].target
                        port = self.iscsiSRs[kkk].port
                        iqn = self.iscsiSRs[kkk].targetIQN
                        key = "%s,%s,%s" % (ipaddr, port, iqn)
                        # The final string must preserve the order without repetition
                        if key not in IQNs:
                            IQNs[key] = ""
                            IQNstring += "%s|" % key
                    util.SMlog("IQNstring is now %s" % IQNstring)
                    self.iscsiSRs = new_iscsiSRs
                    util.SMlog("iqn %s is leading now" % self.iscsiSRs[0].targetIQN)
                    # Updating pbd entry, if any
                    try:
                        pbd = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
                        if pbd is not None and 'multiSession' in self.dconf:
                            util.SMlog("Updating multiSession in PBD")
                            dconf = self.session.xenapi.PBD.get_device_config(pbd)
                            dconf['multiSession'] = IQNstring
                            self.session.xenapi.PBD.set_device_config(pbd, dconf)
                    except:
                        pass
                    break
            if not target_success and attempt_discovery:
                raise xs_errors.XenError('ISCSITarget')

            # Check for any unneeded open iscsi sessions
            if forced_login == True:
                try:
                    iscsi.ensure_daemon_running_ok(self.iscsi.localIQN)
                    iscsi.logout(self.iscsi.target, self.iscsi.targetIQN)
                    forced_login = False
                except:
                    raise xs_errors.XenError('ISCSILogout')

    def print_LUNs_XML(self):
        dom = xml.dom.minidom.Document()
        element = dom.createElement("iscsi-target")
        dom.appendChild(element)
        for uuid in self.LUNs:
            val = self.LUNs[uuid]
            entry = dom.createElement('LUN')
            element.appendChild(entry)

            for attr in ('vendor', 'serial', 'LUNid', \
                         'size', 'SCSIid'):
                try:
                    aval = getattr(val, attr)
                except AttributeError:
                    continue

                if aval:
                    subentry = dom.createElement(attr)
                    entry.appendChild(subentry)
                    textnode = dom.createTextNode(str(aval))
                    subentry.appendChild(textnode)

        print(dom.toprettyxml(), file=sys.stderr)

    def _getSCSIid_from_LUN(self, sr_uuid):
        was_attached = True
        self.iscsi.attach(sr_uuid)
        dev = self.dconf['LUNid'].split(',')
        if len(dev) > 1:
            raise xs_errors.XenError('LVMOneLUN')
        path = os.path.join(self.iscsi.path, "LUN%s" % dev[0])
        if not util.wait_for_path(path, BaseISCSI.MAX_TIMEOUT):
            util.SMlog("Unable to detect LUN attached to host [%s]" % path)
        try:
            SCSIid = scsiutil.getSCSIid(path)
        except:
            raise xs_errors.XenError('InvalidDev')
        self.iscsi.detach(sr_uuid)
        return SCSIid

    def _LUNprint(self, sr_uuid):
        if self.iscsi.attached:
            # Force a rescan on the bus.
            self.iscsi.refresh()
#            time.sleep(5)
# Now call attach (handles the refcounting + session activa)
        self.iscsi.attach(sr_uuid)

        util.SMlog("LUNprint: waiting for path: %s" % self.iscsi.path)
        if util.wait_for_path("%s/LUN*" % self.iscsi.path, BaseISCSI.MAX_TIMEOUT):
            try:
                adapter = self.iscsi.adapter[self.iscsi.address]
                util.SMlog("adapter=%s" % adapter)

                # find a scsi device on which to issue a report luns command:
                devs = glob.glob("%s/LUN*" % self.iscsi.path)
                sgdevs = []
                for i in devs:
                    sgdevs.append(int(i.split("LUN")[1]))
                sgdevs.sort()
                sgdev = "%s/LUN%d" % (self.iscsi.path, sgdevs[0])

                # issue a report luns:
                luns = util.pread2(["/usr/bin/sg_luns", "-q", sgdev]).split('\n')
                nluns = len(luns) - 1  # remove the line relating to the final \n

                # make sure we've got that many sg devices present
                for i in range(0, 30):
                    luns = scsiutil._dosgscan()
                    sgdevs = [r for r in luns if r[1] == adapter]
                    if len(sgdevs) >= nluns:
                        util.SMlog("Got all %d sg devices" % nluns)
                        break
                    else:
                        util.SMlog("Got %d sg devices - expecting %d" % (len(sgdevs), nluns))
                        time.sleep(1)

                if os.path.exists("/sbin/udevsettle"):
                    util.pread2(["/sbin/udevsettle"])
                else:
                    util.pread2(["/sbin/udevadm", "settle"])
            except:
                util.SMlog("Generic exception caught. Pass")
                pass  # Make sure we don't break the probe...

        self.iscsi.print_LUNs()
        self.iscsi.detach(sr_uuid)

    def create(self, sr_uuid, size):
        # Check SCSIid not already in use by other PBDs
        if util.test_SCSIid(self.session, sr_uuid, self.SCSIid):
            raise xs_errors.XenError('SRInUse')

        self.iscsi.attach(sr_uuid)
        try:
            self.iscsi._attach_LUN_bySCSIid(self.SCSIid)
            self._pathrefresh(LVHDoISCSISR)
            LVHDSR.LVHDSR.create(self, sr_uuid, size)
        except Exception as inst:
            self.iscsi.detach(sr_uuid)
            raise xs_errors.XenError("SRUnavailable", opterr=inst)
        self.iscsi.detach(sr_uuid)

    def delete(self, sr_uuid):
        self._pathrefresh(LVHDoISCSISR)
        LVHDSR.LVHDSR.delete(self, sr_uuid)
        for i in self.iscsiSRs:
            i.detach(sr_uuid)

    def attach(self, sr_uuid):
        try:
            connected = False
            stored_exception = None
            for i in self.iscsiSRs:
                try:
                    i.attach(sr_uuid)
                except xs_errors.SROSError as inst:
                    # Some iscsi objects can fail login/discovery but not all. Storing exception
                    if inst.errno in [141, 83]:
                        util.SMlog("Connection failed for target %s, continuing.." % i.target)
                        stored_exception = inst
                        continue
                    else:
                        raise
                else:
                    connected = True

                i._attach_LUN_bySCSIid(self.SCSIid)

            # Check if at least one iscsi succeeded
            if not connected and stored_exception:
                # pylint: disable=raising-bad-type
                raise stored_exception

            if 'multiSession' in self.dconf:
                # Force a manual bus refresh
                for a in self.iscsi.adapter:
                    scsiutil.rescan([self.iscsi.adapter[a]])

            self._pathrefresh(LVHDoISCSISR)

            # Check that we only have PVs for the volume group with the expected SCSI ID
            lvutil.checkPVScsiIds(self.vgname, self.SCSIid)

            LVHDSR.LVHDSR.attach(self, sr_uuid)
        except Exception as inst:
            for i in self.iscsiSRs:
                i.detach(sr_uuid)

            # If we already have a proper error just raise it
            if isinstance(inst, xs_errors.SROSError):
                raise

            raise xs_errors.XenError("SRUnavailable", opterr=inst)

        self._setMultipathableFlag(SCSIid=self.SCSIid)

    def detach(self, sr_uuid):
        LVHDSR.LVHDSR.detach(self, sr_uuid)
        for i in self.iscsiSRs:
            util.SMlog(f'Detaching {i}')
            i.detach(sr_uuid)

    def scan(self, sr_uuid):
        self._pathrefresh(LVHDoISCSISR)
        if self.mpath == "true":
            for i in self.iscsiSRs:
                try:
                    i.attach(sr_uuid)
                except xs_errors.SROSError:
                    util.SMlog("Connection failed for target %s, continuing.." % i.target)
        LVHDSR.LVHDSR.scan(self, sr_uuid)

    def probe(self):
        self.uuid = util.gen_uuid()

        # When multipathing is enabled, since we don't refcount the multipath maps,
        # we should not attempt to do the iscsi.attach/detach when the map is already present,
        # as this will remove it (which may well be in use).
        if self.mpath == 'true' and 'SCSIid' in self.dconf:
            maps = []
            try:
                maps = mpath_cli.list_maps()
            except:
                pass

            if self.dconf['SCSIid'] in maps:
                raise xs_errors.XenError('SRInUse')

        self.iscsi.attach(self.uuid)
        self.iscsi._attach_LUN_bySCSIid(self.SCSIid)
        self._pathrefresh(LVHDoISCSISR)
        out = LVHDSR.LVHDSR.probe(self)
        self.iscsi.detach(self.uuid)
        return out

    def check_sr(self, sr_uuid):
        """Hook to check SR health"""
        pbdref = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
        if pbdref:
                other_config = self.session.xenapi.PBD.get_other_config(pbdref)
                if util.sessions_less_than_targets(other_config, self.dconf):
                    self.create_iscsi_sessions(sr_uuid)
                    for iscsi in self.iscsiSRs:
                        try:
                            iscsi.attach(sr_uuid)
                        except xs_errors.SROSError:
                            util.SMlog("Failed to attach iSCSI target")

    def vdi(self, uuid):
        return LVHDoISCSIVDI(self, uuid)


class LVHDoISCSIVDI(LVHDSR.LVHDVDI):
    def generate_config(self, sr_uuid, vdi_uuid):
        util.SMlog("LVHDoISCSIVDI.generate_config")
        if not lvutil._checkLV(self.path):
            raise xs_errors.XenError('VDIUnavailable')
        dict = {}
        self.sr.dconf['localIQN'] = self.sr.iscsi.localIQN
        self.sr.dconf['multipathing'] = self.sr.mpath
        self.sr.dconf['multipathhandle'] = self.sr.mpathhandle
        dict['device_config'] = self.sr.dconf
        if 'chappassword_secret' in dict['device_config']:
            s = util.get_secret(self.session, dict['device_config']['chappassword_secret'])
            del dict['device_config']['chappassword_secret']
            dict['device_config']['chappassword'] = s
        dict['sr_uuid'] = sr_uuid
        dict['vdi_uuid'] = vdi_uuid
        dict['command'] = 'vdi_attach_from_config'
        # Return the 'config' encoded within a normal XMLRPC response so that
        # we can use the regular response/error parsing code.
        config = xmlrpc.client.dumps(tuple([dict]), "vdi_attach_from_config")
        return xmlrpc.client.dumps((config, ), "", True)

    def attach_from_config(self, sr_uuid, vdi_uuid):
        util.SMlog("LVHDoISCSIVDI.attach_from_config")
        try:
            self.sr.iscsi.attach(sr_uuid)
            self.sr.iscsi._attach_LUN_bySCSIid(self.sr.SCSIid)
            return LVHDSR.LVHDVDI.attach(self, sr_uuid, vdi_uuid)
        except:
            util.logException("LVHDoISCSIVDI.attach_from_config")
            raise xs_errors.XenError('SRUnavailable', \
                        opterr='Unable to attach the heartbeat disk')

# SR registration at import
SR.registerSR(LVHDoISCSISR)
