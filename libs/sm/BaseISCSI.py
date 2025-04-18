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
# ISCSISR: ISCSI software initiator SR driver
#

from sm import SR
from sm.core import util
import time
from sm import LUNperVDI
import os
import sys
import re
import glob
import xml.dom.minidom
from sm.core import scsiutil
from sm.core import iscsi
from sm.core import xs_errors

INITIATORNAME_FILE = '/etc/iscsi/initiatorname.iscsi'
SECTOR_SHIFT = 9
DEFAULT_PORT = 3260
# 2^16 Max port number value
MAXPORT = 65535
MAX_TIMEOUT = 15
MAX_LUNID_TIMEOUT = 60
ISCSI_PROCNAME = "iscsi_tcp"


class BaseISCSISR(SR.SR):
    """ISCSI storage repository"""

    @property
    def force_tapdisk(self):
        return self.dconf.get('force_tapdisk', 'false') == 'true'

    @property
    def attached(self):
        if not self._attached:
            self._attached = False
            self._attached = iscsi._checkTGT(self.targetIQN, self.target)
        return self._attached

    @attached.setter
    def attached(self, value):
        self._attached = value

    @property
    def pathdict(self):
        if not self._pathdict:
            self._initPaths()
        return self._pathdict

    @property
    def adapter(self):
        if not self._adapter:
            self._initPaths()
        return self._adapter

    @adapter.setter
    def adapter(self, value):
        self._adapter = value

    @property
    def devs(self):
        if not self._devs:
            self._initPaths()
        return self._devs

    @property
    def tgtidx(self):
        """This appears to only be referenced by a unit test. Do we really need it? """
        if not self._tgtidx:
            self._initPaths()
        return self._tgtidx

    @property
    def path(self):
        if not self._path:
            self._initPaths()
        return self._path

    @property
    def address(self):
        if not self._address:
            self._initPaths()
        return self._address

    def handles(type):
        return False
    handles = staticmethod(handles)

    def _synchroniseAddrList(self, addrlist):
        if not self.multihomed:
            return
        change = False
        if 'multihomelist' not in self.dconf:
            change = True
            self.mlist = []
            mstr = ""
        else:
            self.mlist = self.dconf['multihomelist'].split(',')
            mstr = self.dconf['multihomelist']
        for val in addrlist:
            if not val in self.mlist:
                self.mlist.append(val)
                if len(mstr):
                    mstr += ","
                mstr += val
                change = True
        if change:
            pbd = None
            try:
                pbd = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
                if pbd is not None:
                    device_config = self.session.xenapi.PBD.get_device_config(pbd)
                    device_config['multihomelist'] = mstr
                    self.session.xenapi.PBD.set_device_config(pbd, device_config)
            except:
                pass

    def load(self, sr_uuid):
        if self.force_tapdisk:
            self.sr_vditype = 'aio'
        else:
            self.sr_vditype = 'phy'
        self.discoverentry = 0
        self.default_vdi_visibility = False

        # Required parameters
        if 'target' not in self.dconf or not self.dconf['target']:
            raise xs_errors.XenError('ConfigTargetMissing')

        # we are no longer putting hconf in the xml.
        # Instead we pass a session and host ref and let the SM backend query XAPI itself
        try:
            if 'localIQN' not in self.dconf:
                self.localIQN = self.session.xenapi.host.get_other_config(self.host_ref)['iscsi_iqn']
            else:
                self.localIQN = self.dconf['localIQN']
        except:
            raise xs_errors.XenError('ConfigISCSIIQNMissing')

        # Check for empty string
        if not self.localIQN:
            raise xs_errors.XenError('ConfigISCSIIQNMissing')

        try:
            self.target = util._convertDNS(self.dconf['target'].split(',')[0])
        except:
            raise xs_errors.XenError('DNSError')

        self.targetlist = self.target
        if 'targetlist' in self.dconf:
            self.targetlist = self.dconf['targetlist']

        # Optional parameters
        self.chapuser = ""
        self.chappassword = ""
        if 'chapuser' in self.dconf \
                and ('chappassword' in self.dconf or 'chappassword_secret' in self.dconf):
            self.chapuser = self.dconf['chapuser'].encode('utf-8')
            if 'chappassword_secret' in self.dconf:
                self.chappassword = util.get_secret(self.session, self.dconf['chappassword_secret'])
            else:
                self.chappassword = self.dconf['chappassword']

            self.chappassword = self.chappassword.encode('utf-8')

        self.incoming_chapuser = ""
        self.incoming_chappassword = ""
        if 'incoming_chapuser' in self.dconf \
                and ('incoming_chappassword' in self.dconf or 'incoming_chappassword_secret' in self.dconf):
            self.incoming_chapuser = self.dconf['incoming_chapuser'].encode('utf-8')
            if 'incoming_chappassword_secret' in self.dconf:
                self.incoming_chappassword = util.get_secret(self.session, self.dconf['incoming_chappassword_secret'])
            else:
                self.incoming_chappassword = self.dconf['incoming_chappassword']

            self.incoming_chappassword = self.incoming_chappassword.encode('utf-8')

        self.port = DEFAULT_PORT
        if 'port' in self.dconf and self.dconf['port']:
            try:
                self.port = int(self.dconf['port'])
            except:
                raise xs_errors.XenError('ISCSIPort')
        if self.port > MAXPORT or self.port < 1:
            raise xs_errors.XenError('ISCSIPort')

        # For backwards compatibility
        if 'usediscoverynumber' in self.dconf:
            self.discoverentry = self.dconf['usediscoverynumber']

        self.multihomed = False
        if 'multihomed' in self.dconf:
            if self.dconf['multihomed'] == "true":
                self.multihomed = True
        elif self.mpath == 'true':
            self.multihomed = True

        if 'targetIQN' not in self.dconf or  not self.dconf['targetIQN']:
            self._scan_IQNs()
            raise xs_errors.XenError('ConfigTargetIQNMissing')

        self.targetIQN = self.dconf['targetIQN']

        self._attached = None
        self._pathdict = None
        self._adapter = None
        self._devs = None
        self._tgtidx = None
        self._path = None
        self._address = None

    def _initPaths(self):
        self._init_adapters()
        # Generate a list of all possible paths
        self._pathdict = {}
        addrlist = []
        rec = {}
        key = "%s:%d" % (self.target, self.port)
        rec['ipaddr'] = self.target
        rec['port'] = self.port
        rec['path'] = os.path.join("/dev/iscsi", self.targetIQN, \
                                   key)
        self._pathdict[key] = rec
        util.SMlog("PATHDICT: key %s: %s" % (key, rec))
        self._tgtidx = key
        addrlist.append(key)

        self._path = rec['path']
        self._address = self.tgtidx
        if not self.attached:
            return

        if self.multihomed:
            map = iscsi.get_node_records(targetIQN=self.targetIQN)
            for i in range(0, len(map)):
                (portal, tpgt, iqn) = map[i]
                (ipaddr, port) = iscsi.parse_IP_port(portal)
                if self.target != ipaddr:
                    key = "%s:%s" % (ipaddr, port)
                    rec = {}
                    rec['ipaddr'] = ipaddr
                    rec['port'] = int(port)
                    rec['path'] = os.path.join("/dev/iscsi", self.targetIQN, \
                                   key)
                    self._pathdict[key] = rec
                    util.SMlog("PATHDICT: key %s: %s" % (key, rec))
                    addrlist.append(key)

        if not os.path.exists(self.path):
            # Try to detect an active path in order of priority
            for key in self.pathdict:
                if key in self.adapter:
                    self._tgtidx = key
                    self._path = self.pathdict[self.tgtidx]['path']
                    if os.path.exists(self.path):
                        util.SMlog("Path found: %s" % self.path)
                        break
            self._address = self.tgtidx
        self._synchroniseAddrList(addrlist)

    def _init_adapters(self):
        # Generate a list of active adapters
        ids = scsiutil._genHostList(ISCSI_PROCNAME)
        util.SMlog(ids)
        self._adapter = {}
        for host in ids:
            try:
                targetIQN = iscsi.get_targetIQN(host)
                if targetIQN != self.targetIQN:
                    continue
                (addr, port) = iscsi.get_targetIP_and_port(host)
                entry = "%s:%s" % (addr, port)
                self._adapter[entry] = host
            except:
                pass
        self._devs = scsiutil.cacheSCSIidentifiers()

    def attach(self, sr_uuid):
        self._mpathHandle()

        multiTargets = False

        npaths = 0
        try:
            pbdref = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
            if pbdref:
                other_config = self.session.xenapi.PBD.get_other_config(pbdref)
                multiTargets = util.sessions_less_than_targets(other_config, self.dconf)
        except:
            pass

        if not self.attached or multiTargets:
            # Verify iSCSI target and port
            if 'multihomelist' in self.dconf and 'multiSession' not in self.dconf:
                targetlist = self.dconf['multihomelist'].split(',')
            else:
                targetlist = ['%s:%d' % (self.target, self.port)]
            conn = False
            for val in targetlist:
                (target, port) = iscsi.parse_IP_port(val)
                try:
                    util._testHost(target, int(port), 'ISCSITarget')
                    self.target = target
                    self.port = int(port)
                    conn = True
                    break
                except:
                    pass
            if not conn:
                raise xs_errors.XenError('ISCSITarget')

            # Test and set the initiatorname file
            iscsi.ensure_daemon_running_ok(self.localIQN)

            # Check to see if auto attach was set
            if not iscsi._checkTGT(self.targetIQN, tgt=self.target) or multiTargets:
                try:
                    iqn_map = []
                    if 'any' != self.targetIQN:
                        try:
                            iqn_map = iscsi.get_node_records(self.targetIQN)
                        except:
                            # Pass the exception that is thrown, when there
                            # are no nodes
                            pass

                    # Check our current target is in the map
                    portal = '%s:%d' % (self.target, self.port)
                    if len(iqn_map) == 0 or not any([x[0] for x in iqn_map if x[0] == portal]):
                        iqn_map = iscsi.discovery(self.target, self.port,
                                                  self.chapuser, self.chappassword,
                                                  self.targetIQN,
                                                  iscsi.get_iscsi_interfaces())
                    if len(iqn_map) == 0:
                        self._scan_IQNs()
                        raise xs_errors.XenError('ISCSIDiscovery',
                                                 opterr='check target settings')
                    for i in range(0, len(iqn_map)):
                        (portal, tpgt, iqn) = iqn_map[i]
                        try:
                            (ipaddr, port) = iscsi.parse_IP_port(portal)
                            if not self.multihomed and ipaddr != self.target:
                                continue
                            util._testHost(ipaddr, int(port), 'ISCSITarget')
                            util.SMlog("Logging in to [%s:%s]" % (ipaddr, port))
                            iscsi.login(portal, iqn, self.chapuser,
                                           self.chappassword,
                                           self.incoming_chapuser,
                                           self.incoming_chappassword,
                                           self.mpath == "true")
                            npaths = npaths + 1
                        except Exception as e:
                            # Exceptions thrown in login are acknowledged,
                            # the rest of exceptions are ignored since some of the
                            # paths in multipath may not be reachable
                            if str(e).startswith('ISCSI login'):
                                raise
                            else:
                                pass

                    if not iscsi._checkTGT(self.targetIQN, tgt=self.target):
                        raise xs_errors.XenError('ISCSIDevice', \
                                                 opterr='during login')

                    # Allow the devices to settle
                    time.sleep(5)

                except util.CommandException as inst:
                    raise xs_errors.XenError('ISCSILogin', \
                                             opterr='code is %d' % inst.code)
            self.attached = True
        self._initPaths()
        util._incr_iscsiSR_refcount(self.targetIQN, sr_uuid)
        IQNs = []
        if "multiSession" in self.dconf:
            IQNs = ""
            for iqn in self.dconf['multiSession'].split("|"):
                if len(iqn):
                    IQNs += iqn.split(',')[2]
        else:
            IQNs.append(self.targetIQN)

        sessions = 0
        paths = iscsi.get_IQN_paths()
        for path in paths:
            try:
                if util.get_single_entry(os.path.join(path, 'targetname')) in IQNs:
                    sessions += 1
                    util.SMlog("IQN match. Incrementing sessions to %d" % sessions)
            except:
                util.SMlog("Failed to read targetname path," \
                           + "iscsi_sessions value may be incorrect")

        if pbdref:
            # Just to be safe in case of garbage left during crashes
            # we remove the key and add it
            self.session.xenapi.PBD.remove_from_other_config(
                pbdref, "iscsi_sessions")
            self.session.xenapi.PBD.add_to_other_config(
                pbdref, "iscsi_sessions", str(sessions))

        if 'SCSIid' in self.dconf:
            if self.mpath == 'true':
                self.mpathmodule.refresh(self.dconf['SCSIid'], 0)
            dev_path = os.path.join("/dev/disk/by-scsid", self.dconf['SCSIid'])
            if not os.path.exists(dev_path):
                # LUN may have been added to the SAN since the session was created
                iscsi.refresh_luns(self.targetIQN, self.target)

            if not os.path.exists(dev_path):
                raise xs_errors.XenError('ConfigSCSIid')

            devs = os.listdir(dev_path)
            for dev in devs:
                realdev = os.path.realpath(os.path.join(dev_path, dev))
                util.set_scheduler(os.path.basename(realdev))

    def detach(self, sr_uuid, delete=False):
        keys = []
        pbdref = None
        try:
            pbdref = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
        except:
            pass
        if 'SCSIid' in self.dconf:
            scsi_id = self.dconf['SCSIid']
            util.SMlog(f"Resetting mpath on {scsi_id}")
            self.mpathmodule.reset(scsi_id, explicit_unmap=True)
            keys.append("mpath-" + scsi_id)

        # Remove iscsi_sessions and multipathed keys
        if pbdref is not None:
            if self.cmd == 'sr_detach':
                keys += ["multipathed", "iscsi_sessions"]
            for key in keys:
                try:
                    self.session.xenapi.PBD.remove_from_other_config(pbdref, key)
                except:
                    pass

        if util._decr_iscsiSR_refcount(self.targetIQN, sr_uuid) != 0:
            return

        if self.direct and util._containsVDIinuse(self):
            return

        if iscsi._checkTGT(self.targetIQN):
            try:
                iscsi.logout(self.target, self.targetIQN, all=True)
                if delete:
                    iscsi.delete(self.targetIQN)
            except util.CommandException as inst:
                raise xs_errors.XenError('ISCSIQueryDaemon', \
                          opterr='error is %d' % inst.code)
            if iscsi._checkTGT(self.targetIQN):
                raise xs_errors.XenError('ISCSIQueryDaemon', \
                    opterr='Failed to logout from target')

        self.attached = False

    def create(self, sr_uuid, size):
        # Check whether an SR already exists
        SRs = self.session.xenapi.SR.get_all_records()
        for sr in SRs:
            record = SRs[sr]
            sm_config = record["sm_config"]
            if 'targetIQN' in sm_config and \
               sm_config['targetIQN'] == self.targetIQN:
                raise xs_errors.XenError('SRInUse')
        self.attach(sr_uuid)
        # Wait up to MAX_TIMEOUT for devices to appear
        util.wait_for_path(self.path, MAX_TIMEOUT)

        if self._loadvdis() > 0:
            scanrecord = SR.ScanRecord(self)
            scanrecord.synchronise()
        try:
            self.detach(sr_uuid)
        except:
            pass
        self.sm_config = self.session.xenapi.SR.get_sm_config(self.sr_ref)
        self.sm_config['disktype'] = 'Raw'
        self.sm_config['datatype'] = 'ISCSI'
        self.sm_config['target'] = self.target
        self.sm_config['targetIQN'] = self.targetIQN
        self.sm_config['multipathable'] = 'true'
        self.session.xenapi.SR.set_sm_config(self.sr_ref, self.sm_config)
        return

    def delete(self, sr_uuid):
        self.detach(sr_uuid)
        return

    def probe(self):
        SRs = self.session.xenapi.SR.get_all_records()
        Recs = {}
        for sr in SRs:
            record = SRs[sr]
            sm_config = record["sm_config"]
            if 'targetIQN' in sm_config and \
               sm_config['targetIQN'] == self.targetIQN:
                Recs[record["uuid"]] = sm_config
        return self.srlist_toxml(Recs)
    
    def scan(self, sr_uuid):
        if not self.passthrough:
            if not self.attached:
                raise xs_errors.XenError('SRUnavailable')
            self.refresh()
            time.sleep(2)  # it seems impossible to tell when a scan's finished
            self._loadvdis()
            self.physical_utilisation = self.physical_size
            for uuid, vdi in self.vdis.items():
                if vdi.managed:
                    self.physical_utilisation += vdi.size
            self.virtual_allocation = self.physical_utilisation
        return super(BaseISCSISR, self).scan(sr_uuid)

    def vdi(self, uuid):
        return LUNperVDI.RAWVDI(self, uuid)

    def _scan_IQNs(self):
        # Verify iSCSI target and port
        util._testHost(self.target, self.port, 'ISCSITarget')

        # Test and set the initiatorname file
        iscsi.ensure_daemon_running_ok(self.localIQN)

        map = iscsi.discovery(self.target, self.port, self.chapuser,
                                 self.chappassword,
                                 interface_array=iscsi.get_iscsi_interfaces())
        map.append(("%s:%d" % (self.targetlist, self.port), "0", "*"))
        self.print_entries(map)

    def _attach_LUN_bylunid(self, lunid):
        if not self.attached:
            raise xs_errors.XenError('SRUnavailable')
        connected = []
        for val in self.adapter:
            if val not in self.pathdict:
                continue
            rec = self.pathdict[val]
            path = os.path.join(rec['path'], "LUN%s" % lunid)
            realpath = os.path.realpath(path)
            host = self.adapter[val]
            l = [realpath, host, 0, 0, lunid]

            addDevice = True
            if realpath in self.devs:
                # if the device is stale remove it before adding again
                real_SCSIid = None
                try:
                    real_SCSIid = scsiutil.getSCSIid(realpath)
                except:
                    pass

                if real_SCSIid is not None:
                    # make sure this is the same scsiid, if not remove the device
                    cur_scsibuspath = glob.glob('/dev/disk/by-scsibus/*-%s:0:0:%s' % (host, lunid))
                    cur_SCSIid = os.path.basename(cur_scsibuspath[0]).split("-")[0]
                    if cur_SCSIid != real_SCSIid:
                        # looks stale, remove it
                        scsiutil.scsi_dev_ctrl(l, "remove")
                    else:
                        util.SMlog("Not attaching LUNID %s for adapter %s" \
                            " since the device exists and the scsi id %s seems" \
                            " to be valid. " % (lunid, val, real_SCSIid))
                        addDevice = False
                else:
                    # looks stale, remove it
                    scsiutil.scsi_dev_ctrl(l, "remove")

            if addDevice:
                # add the device
                scsiutil.scsi_dev_ctrl(l, "add")
                if not util.wait_for_path(path, MAX_LUNID_TIMEOUT):
                    util.SMlog("Unable to detect LUN attached to host on path [%s]" % path)
                    continue
            connected.append(path)
        return connected

    def _attach_LUN_byserialid(self, serialid):
        if not self.attached:
            raise xs_errors.XenError('SRUnavailable')
        connected = []
        for val in self.adapter:
            if val not in self.pathdict:
                continue
            rec = self.pathdict[val]
            path = os.path.join(rec['path'], "SERIAL-%s" % serialid)
            realpath = os.path.realpath(path)
            if realpath not in self.devs:
                if not util.wait_for_path(path, 5):
                    util.SMlog("Unable to detect LUN attached to host on serial path [%s]" % path)
                    continue
            connected.append(path)
        return connected

    def _detach_LUN_bylunid(self, lunid, SCSIid):
        if not self.attached:
            raise xs_errors.XenError('SRUnavailable')
        if self.mpath == 'true' and len(SCSIid):
            self.mpathmodule.reset(SCSIid, explicit_unmap=True)
            util.remove_mpathcount_field(self.session, self.host_ref, self.sr_ref, SCSIid)
        for val in self.adapter:
            if val not in self.pathdict:
                continue
            rec = self.pathdict[val]
            path = os.path.join(rec['path'], "LUN%s" % lunid)
            realpath = os.path.realpath(path)
            if realpath in self.devs:
                util.SMlog("Found key: %s" % realpath)
                scsiutil.scsi_dev_ctrl(self.devs[realpath], 'remove')
                # Wait for device to disappear
                if not util.wait_for_nopath(realpath, MAX_LUNID_TIMEOUT):
                    util.SMlog("Device has not disappeared after %d seconds" % \
                               MAX_LUNID_TIMEOUT)
                else:
                    util.SMlog("Device [%s,%s] disappeared" % (realpath, path))

    def _attach_LUN_bySCSIid(self, SCSIid):
        if not self.attached:
            raise xs_errors.XenError('SRUnavailable')

        path = self.mpathmodule.path(SCSIid)
        if not util.pathexists(path):
            self.refresh()
            if not util.wait_for_path(path, MAX_TIMEOUT):
                util.SMlog("Unable to detect LUN attached to host [%s]"
                           % path)
                raise xs_errors.XenError('ISCSIDevice')

    # This function queries the session for the attached LUNs
    def _loadvdis(self):
        count = 0
        if not os.path.exists(self.path):
            return 0
        for file in filter(self.match_lun, util.listdir(self.path)):
            vdi_path = os.path.join(self.path, file)
            LUNid = file.replace("LUN", "")
            uuid = scsiutil.gen_uuid_from_string(scsiutil.getuniqueserial(vdi_path))
            obj = self.vdi(uuid)
            obj._query(vdi_path, LUNid)
            self.vdis[uuid] = obj
            self.physical_size += obj.size
            count += 1
        return count

    def refresh(self):
        for val in self.adapter:
            util.SMlog("Rescanning host adapter %s" % self.adapter[val])
            scsiutil.rescan([self.adapter[val]])

    # Helper function for LUN-per-VDI VDI.introduce
    def _getLUNbySMconfig(self, sm_config):
        if 'LUNid' not in sm_config:
            raise xs_errors.XenError('VDIUnavailable')
        LUNid = int(sm_config['LUNid'])
        if not len(self._attach_LUN_bylunid(LUNid)):
            raise xs_errors.XenError('VDIUnavailable')
        return os.path.join(self.path, "LUN%d" % LUNid)

    # This function takes an ISCSI device and populate it with
    # a dictionary of available LUNs on that target.
    def print_LUNs(self):
        self.LUNs = {}
        if os.path.exists(self.path):
            dom0_disks = util.dom0_disks()
            for file in util.listdir(self.path):
                if file.find("LUN") != -1 and file.find("_") == -1:
                    vdi_path = os.path.join(self.path, file)
                    if os.path.realpath(vdi_path) in dom0_disks:
                        util.SMlog("Hide dom0 boot disk LUN")
                    else:
                        LUNid = file.replace("LUN", "")
                        obj = self.vdi(self.uuid)
                        obj._query(vdi_path, LUNid)
                        self.LUNs[obj.uuid] = obj

    def print_entries(self, map):
        dom = xml.dom.minidom.Document()
        element = dom.createElement("iscsi-target-iqns")
        dom.appendChild(element)
        count = 0
        for address, tpgt, iqn in map:
            entry = dom.createElement('TGT')
            element.appendChild(entry)
            subentry = dom.createElement('Index')
            entry.appendChild(subentry)
            textnode = dom.createTextNode(str(count))
            subentry.appendChild(textnode)

            try:
                # We always expect a port so this holds
                # regardless of IP version
                (addr, port) = address.rsplit(':', 1)
            except:
                addr = address
                port = DEFAULT_PORT
            subentry = dom.createElement('IPAddress')
            entry.appendChild(subentry)
            textnode = dom.createTextNode(str(addr))
            subentry.appendChild(textnode)

            if int(port) != DEFAULT_PORT:
                subentry = dom.createElement('Port')
                entry.appendChild(subentry)
                textnode = dom.createTextNode(str(port))
                subentry.appendChild(textnode)

            subentry = dom.createElement('TargetIQN')
            entry.appendChild(subentry)
            textnode = dom.createTextNode(str(iqn))
            subentry.appendChild(textnode)
            count += 1
        print(dom.toprettyxml(), file=sys.stderr)

    def srlist_toxml(self, SRs):
        dom = xml.dom.minidom.Document()
        element = dom.createElement("SRlist")
        dom.appendChild(element)

        for val in SRs:
            record = SRs[val]
            entry = dom.createElement('SR')
            element.appendChild(entry)

            subentry = dom.createElement("UUID")
            entry.appendChild(subentry)
            textnode = dom.createTextNode(val)
            subentry.appendChild(textnode)

            subentry = dom.createElement("Target")
            entry.appendChild(subentry)
            textnode = dom.createTextNode(record['target'])
            subentry.appendChild(textnode)

            subentry = dom.createElement("TargetIQN")
            entry.appendChild(subentry)
            textnode = dom.createTextNode(record['targetIQN'])
            subentry.appendChild(textnode)
        return dom.toprettyxml()

    def match_lun(self, s):
        regex = re.compile("_")
        if regex.search(s, 0):
            return False
        regex = re.compile("LUN")
        return regex.search(s, 0)
