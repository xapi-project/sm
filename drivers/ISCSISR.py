#!/usr/bin/python
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

import SR, VDI, SRCommand, util
import statvfs, time, LUNperVDI
import os, socket, sys, re, glob
import xml.dom.minidom
import shutil, xmlrpclib
import scsiutil, iscsilib
import xs_errors, errno

CAPABILITIES = ["SR_PROBE","VDI_CREATE","VDI_DELETE","VDI_ATTACH",
                "VDI_DETACH", "VDI_INTRODUCE"]

CONFIGURATION = [ [ 'target', 'IP address or hostname of the iSCSI target (required)' ], \
                  [ 'targetIQN', 'The IQN of the target LUN group to be attached (required)' ], \
                  [ 'chapuser', 'The username to be used during CHAP authentication (optional)' ], \
                  [ 'chappassword', 'The password to be used during CHAP authentication (optional)' ], \
                  [ 'incoming_chapuser', 'The incoming username to be used during bi-directional CHAP authentication (optional)' ], \
                  [ 'incoming_chappassword', 'The incoming password to be used during bi-directional CHAP authentication (optional)' ], \
                  [ 'port', 'The network port number on which to query the target (optional)' ], \
                  [ 'multihomed', 'Enable multi-homing to this target, true or false (optional, defaults to same value as host.other_config:multipathing)' ],
                  [ 'force_tapdisk', 'Force use of tapdisk, true or false (optional, defaults to false)'],
]

DRIVER_INFO = {
    'name': 'iSCSI',
    'description': 'Base ISCSI SR driver, provides a LUN-per-VDI. Does not support creation of VDIs but accesses existing LUNs on a target.',
    'vendor': 'Citrix Systems Inc',
    'copyright': '(C) 2008 Citrix Systems Inc',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
    }

INITIATORNAME_FILE = '/etc/iscsi/initiatorname.iscsi'
SECTOR_SHIFT = 9
DEFAULT_PORT = 3260
# 2^16 Max port number value
MAXPORT = 65535
MAX_TIMEOUT = 15
MAX_LUNID_TIMEOUT = 60
ISCSI_PROCNAME = "iscsi_tcp"

class ISCSISR(SR.SR):
    """ISCSI storage repository"""

    @property
    def force_tapdisk(self):
        return self.dconf.get('force_tapdisk', 'false') == 'true'

    def handles(type):
        if type == "iscsi":
            return True
        return False
    handles = staticmethod(handles)

    def _synchroniseAddrList(self, addrlist):
        if not self.multihomed:
            return
        change = False
        if not self.dconf.has_key('multihomelist'):
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
                if pbd <> None:
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
        if not self.dconf.has_key('target') or  not self.dconf['target']:
            raise xs_errors.XenError('ConfigTargetMissing')

        # we are no longer putting hconf in the xml.
        # Instead we pass a session and host ref and let the SM backend query XAPI itself
        try:
            if not self.dconf.has_key('localIQN'):
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
        if self.dconf.has_key('targetlist'):
            self.targetlist = self.dconf['targetlist']

        # Optional parameters
        self.chapuser = ""
        self.chappassword = ""
        if self.dconf.has_key('chapuser') \
                and (self.dconf.has_key('chappassword') or self.dconf.has_key('chappassword_secret')):
            self.chapuser = self.dconf['chapuser']
            if self.dconf.has_key('chappassword_secret'):
                self.chappassword = util.get_secret(self.session, self.dconf['chappassword_secret'])
            else:
                self.chappassword = self.dconf['chappassword']

        self.incoming_chapuser = ""
        self.incoming_chappassword = ""
        if self.dconf.has_key('incoming_chapuser') \
                and (self.dconf.has_key('incoming_chappassword') or self.dconf.has_key('incoming_chappassword_secret')):
            self.incoming_chapuser = self.dconf['incoming_chapuser']
            if self.dconf.has_key('incoming_chappassword_secret'):
                self.incoming_chappassword = util.get_secret(self.session, self.dconf['incoming_chappassword_secret'])
            else:
                self.incoming_chappassword = self.dconf['incoming_chappassword']

        self.port = DEFAULT_PORT
        if self.dconf.has_key('port') and self.dconf['port']:
            try:
                self.port = long(self.dconf['port'])
            except:
                raise xs_errors.XenError('ISCSIPort')
        if self.port > MAXPORT or self.port < 1:
            raise xs_errors.XenError('ISCSIPort')

        # For backwards compatibility
        if self.dconf.has_key('usediscoverynumber'):
            self.discoverentry = self.dconf['usediscoverynumber']

        self.multihomed = False
        if self.dconf.has_key('multihomed'):
            if self.dconf['multihomed'] == "true":
                self.multihomed = True
        elif self.mpath == 'true':
            self.multihomed = True

        if not self.dconf.has_key('targetIQN') or  not self.dconf['targetIQN']:
            self._scan_IQNs()
            raise xs_errors.XenError('ConfigTargetIQNMissing')

        self.targetIQN = unicode(self.dconf['targetIQN']).encode('utf-8')
        self.attached = False
        try:
            self.attached = iscsilib._checkTGT(self.targetIQN)
        except:
            pass
        self._initPaths()

    def _initPaths(self):
        self._init_adapters()
        # Generate a list of all possible paths
        self.pathdict = {}
        addrlist = []
        rec = {}
        key = "%s:%d" % (self.target,self.port)
        rec['ipaddr'] = self.target
        rec['port'] = self.port
        rec['path'] = os.path.join("/dev/iscsi",self.targetIQN,\
                                   key)
        self.pathdict[key] = rec
        util.SMlog("PATHDICT: key %s: %s" % (key,rec))
        self.tgtidx = key
        addrlist.append(key)

        self.path = rec['path']
        self.address = self.tgtidx
        if not self.attached:
            return
        
        if self.multihomed:
            map = iscsilib.get_node_records(targetIQN=self.targetIQN)
            for i in range(0,len(map)):
                (portal,tpgt,iqn) = map[i]
                (ipaddr, port) = iscsilib.parse_IP_port(portal)
                if self.target != ipaddr:
                    key = "%s:%s" % (ipaddr,port)
                    rec = {}
                    rec['ipaddr'] = ipaddr
                    rec['port'] = long(port)
                    rec['path'] = os.path.join("/dev/iscsi",self.targetIQN,\
                                   key)
                    self.pathdict[key] = rec
                    util.SMlog("PATHDICT: key %s: %s" % (key,rec))
                    addrlist.append(key)

        if not os.path.exists(self.path):
            # Try to detect an active path in order of priority
            for key in self.pathdict:
                if self.adapter.has_key(key):
                    self.tgtidx = key
                    self.path = self.pathdict[self.tgtidx]['path']
                    if os.path.exists(self.path):
                        util.SMlog("Path found: %s" % self.path)
                        break
            self.address = self.tgtidx
        self._synchroniseAddrList(addrlist)

    def _init_adapters(self):
        # Generate a list of active adapters
        ids = scsiutil._genHostList(ISCSI_PROCNAME)
        util.SMlog(ids)
        self.adapter = {}
        for host in ids:
            try:
                targetIQN = iscsilib.get_targetIQN(host)
                if targetIQN != self.targetIQN:
                    continue
                (addr, port) = iscsilib.get_targetIP_and_port(host)
                entry = "%s:%s" % (addr,port)
                self.adapter[entry] = host
            except:
                pass
        self.devs = scsiutil.cacheSCSIidentifiers()

    def attach(self, sr_uuid):
        self._mpathHandle()

        npaths=0
        if not self.attached:
            # Verify iSCSI target and port
            if self.dconf.has_key('multihomelist') and not self.dconf.has_key('multiSession'):
                targetlist = self.dconf['multihomelist'].split(',')
            else:
                targetlist = ['%s:%d' % (self.target,self.port)]
            conn = False
            for val in targetlist:
                (target, port) = iscsilib.parse_IP_port(val)
                try:
                    util._testHost(target, long(port), 'ISCSITarget')
                    self.target = target
                    self.port = long(port)
                    conn = True
                    break
                except:
                    pass
            if not conn:
                raise xs_errors.XenError('ISCSITarget')

            # Test and set the initiatorname file
            iscsilib.ensure_daemon_running_ok(self.localIQN)

            # Check to see if auto attach was set
            if not iscsilib._checkTGT(self.targetIQN):
                try:
                    map = []
                    if 'any' != self.targetIQN:
                        try:
                            map = iscsilib.get_node_records(self.targetIQN)
                        except:
                            # Pass the exception that is thrown, when there
                            # are no nodes
                            pass
                    if len(map) == 0:
                        map = iscsilib.discovery(self.target, self.port,
                                                  self.chapuser, self.chappassword,
                                                  self.targetIQN,
                                                  self._get_iscsi_interfaces())
                    if len(map) == 0:
                        self._scan_IQNs()
                        raise xs_errors.XenError('ISCSIDiscovery', 
                                                 opterr='check target settings')
                    for i in range(0,len(map)):
                        (portal,tpgt,iqn) = map[i]
                        try:
                            (ipaddr, port) = iscsilib.parse_IP_port(portal)
                            if not self.multihomed and ipaddr != self.target:
                                continue
                            util._testHost(ipaddr, long(port), 'ISCSITarget')
                            util.SMlog("Logging in to [%s:%s]" % (ipaddr,port))
                            iscsilib.login(portal, iqn, self.chapuser,
                                           self.chappassword,
                                           self.incoming_chapuser,
                                           self.incoming_chappassword,
                                           self.mpath == "true")
                            npaths = npaths + 1
                        except Exception, e:
                            # Exceptions thrown in login are acknowledged, 
                            # the rest of exceptions are ignored since some of the
                            # paths in multipath may not be reachable
                            if str(e) == 'ISCSI login failed, verify CHAP credentials':
                                raise
                            else:
                                pass

                    if not iscsilib._checkTGT(self.targetIQN):
                        raise xs_errors.XenError('ISCSIDevice', \
                                                 opterr='during login')
                
                    # Allow the devices to settle
                    time.sleep(5)
                
                except util.CommandException, inst:
                    raise xs_errors.XenError('ISCSILogin', \
                                             opterr='code is %d' % inst.code)
            self.attached = True
        self._initPaths()
        util._incr_iscsiSR_refcount(self.targetIQN, sr_uuid)
        IQNs = []
        if self.dconf.has_key("multiSession"):
            IQNs = ""
            for iqn in self.dconf['multiSession'].split("|"):
                if len(iqn): IQNs += iqn.split(',')[2]
        else:
            IQNs.append(self.targetIQN)
        sessions = 0
        paths = iscsilib.get_IQN_paths()
        for path in paths:
            try:
                if util.get_single_entry(os.path.join(path, 'targetname')) in IQNs:
                    sessions += 1
                    util.SMlog("IQN match. Incrementing sessions to %d" % sessions)
            except:
                util.SMlog("Failed to read targetname path," \
                           + "iscsi_sessions value may be incorrect")

        try:
            pbdref = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
            if pbdref <> None:
                # Just to be safe in case of garbage left during crashes
                # we remove the key and add it
                self.session.xenapi.PBD.remove_from_other_config(
                    pbdref, "iscsi_sessions")
                self.session.xenapi.PBD.add_to_other_config(
                    pbdref, "iscsi_sessions", str(sessions))
        except:
            pass

        if self.mpath == 'true' and self.dconf.has_key('SCSIid'):
            self.mpathmodule.refresh(self.dconf['SCSIid'],npaths)
            # set the device mapper's I/O scheduler
            self.block_setscheduler('/dev/disk/by-scsid/%s/mapper'
                    % self.dconf['SCSIid'])


    def detach(self, sr_uuid):
        keys = []
        pbdref = None
        try:
            pbdref = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
        except:
            pass
        if self.dconf.has_key('SCSIid'):
            self.mpathmodule.reset(self.dconf['SCSIid'], True) # explicitly unmap
            keys.append("mpath-" + self.dconf['SCSIid'])

        # Remove iscsi_sessions and multipathed keys
        if pbdref <> None:
            if self.cmd == 'sr_detach':
                keys += ["multipathed", "iscsi_sessions", "MPPEnabled"]
            for key in keys:
                try:
                    self.session.xenapi.PBD.remove_from_other_config(pbdref, key)
                except:
                    pass

        if util._decr_iscsiSR_refcount(self.targetIQN, sr_uuid) != 0:
            return

        if self.direct and util._containsVDIinuse(self):
            return

        if iscsilib._checkTGT(self.targetIQN):
            try:
                iscsilib.logout(self.target, self.targetIQN, all=True)
            except util.CommandException, inst:
                    raise xs_errors.XenError('ISCSIQueryDaemon', \
                          opterr='error is %d' % inst.code)
            if iscsilib._checkTGT(self.targetIQN):
                raise xs_errors.XenError('ISCSIQueryDaemon', \
                    opterr='Failed to logout from target')

        self.attached = False

    def create(self, sr_uuid, size):
        # Check whether an SR already exists
        SRs = self.session.xenapi.SR.get_all_records()
        for sr in SRs:
            record = SRs[sr]
            sm_config = record["sm_config"]
            if sm_config.has_key('targetIQN') and \
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
            if sm_config.has_key('targetIQN') and \
               sm_config['targetIQN'] == self.targetIQN:
                Recs[record["uuid"]] = sm_config
        return self.srlist_toxml(Recs)
                
    
    def scan(self, sr_uuid):
        if not self.passthrough:
            if not self.attached:
                raise xs_errors.XenError('SRUnavailable')
            self.refresh()
            time.sleep(2) # it seems impossible to tell when a scan's finished
            self._loadvdis()
            self.physical_utilisation = self.physical_size
            for uuid, vdi in self.vdis.iteritems():
                if vdi.managed:
                    self.physical_utilisation += vdi.size
            self.virtual_allocation = self.physical_utilisation
        return super(ISCSISR, self).scan(sr_uuid)
                
    def vdi(self, uuid):
        return LUNperVDI.RAWVDI(self, uuid)

    def _scan_IQNs(self):
        # Verify iSCSI target and port
        util._testHost(self.target, self.port, 'ISCSITarget')

        # Test and set the initiatorname file
        iscsilib.ensure_daemon_running_ok(self.localIQN)

        map = iscsilib.discovery(self.target, self.port, self.chapuser,
                                 self.chappassword,
                                 interfaceArray=self._get_iscsi_interfaces())
        map.append(("%s:%d" % (self.targetlist,self.port),"0","*"))
        self.print_entries(map)

    def _attach_LUN_bylunid(self, lunid):
        if not self.attached:
            raise xs_errors.XenError('SRUnavailable')
        connected = []
        for val in self.adapter:
            if not self.pathdict.has_key(val):
                continue
            rec = self.pathdict[val]
            path = os.path.join(rec['path'],"LUN%s" % lunid)
            realpath = os.path.realpath(path)
            host = self.adapter[val]
            l = [realpath, host, 0, 0, lunid]
            
            addDevice = True            
            if self.devs.has_key(realpath):
                # if the device is stale remove it before adding again
                real_SCSIid = None
                try:
                    real_SCSIid = scsiutil.getSCSIid(realpath)
                except:
                    pass
                
                if real_SCSIid != None:
                    # make sure this is the same scsiid, if not remove the device                    
                    cur_scsibuspath = glob.glob('/dev/disk/by-scsibus/*-%s:0:0:%s' % (host,lunid))
                    cur_SCSIid = os.path.basename(cur_scsibuspath[0]).split("-")[0]                    
                    if cur_SCSIid != real_SCSIid:
                        # looks stale, remove it
                        scsiutil.scsi_dev_ctrl(l,"remove")
                    else:
                        util.SMlog("Not attaching LUNID %s for adapter %s"\
                            " since the device exists and the scsi id %s seems"\
                            " to be valid. " % (lunid, val, real_SCSIid))
                        addDevice = False
                else:
                    # looks stale, remove it
                    scsiutil.scsi_dev_ctrl(l,"remove")

            if addDevice:
                # add the device
                scsiutil.scsi_dev_ctrl(l,"add")
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
            if not self.pathdict.has_key(val):
                continue
            rec = self.pathdict[val]
            path = os.path.join(rec['path'],"SERIAL-%s" % serialid)
            realpath = os.path.realpath(path)
            if not self.devs.has_key(realpath):
                if not util.wait_for_path(path, 5):
                    util.SMlog("Unable to detect LUN attached to host on serial path [%s]" % path)
                    continue
            connected.append(path)
        return connected

    def _detach_LUN_bylunid(self, lunid, SCSIid):
        if not self.attached:
            raise xs_errors.XenError('SRUnavailable')
        if self.mpath == 'true' and len(SCSIid):
            self.mpathmodule.reset(SCSIid, True)
            util.remove_mpathcount_field(self.session, self.host_ref, self.sr_ref, SCSIid)
        for val in self.adapter:
            if not self.pathdict.has_key(val):
                continue
            rec = self.pathdict[val]
            path = os.path.join(rec['path'],"LUN%s" % lunid)            
            realpath = os.path.realpath(path)
            if self.devs.has_key(realpath):
		util.SMlog("Found key: %s" % realpath)
                scsiutil.scsi_dev_ctrl(self.devs[realpath], 'remove')
                # Wait for device to disappear
                if not util.wait_for_nopath(realpath, MAX_LUNID_TIMEOUT):
                    util.SMlog("Device has not disappeared after %d seconds" % \
                               MAX_LUNID_TIMEOUT)
                else:
                    util.SMlog("Device [%s,%s] disappeared" % (realpath,path))

    def _attach_LUN_bySCSIid(self, SCSIid):
        if not self.attached:
            raise xs_errors.XenError('SRUnavailable')

        path = self.mpathmodule.path(SCSIid)
        if not util.pathexists(path):
            self.refresh()
            if not util.wait_for_path(path, MAX_TIMEOUT):
                util.SMlog("Unable to detect LUN attached to host [%s]" \
                           % path)
                return False
        return True


    # This function queries the session for the attached LUNs
    def _loadvdis(self):
        count = 0
        if not os.path.exists(self.path):
            return 0
        for file in filter(self.match_lun, util.listdir(self.path)):
            vdi_path = os.path.join(self.path,file)
            LUNid = file.replace("LUN","")
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
        if not sm_config.has_key('LUNid'):
            raise xs_errors.XenError('VDIUnavailable')
        LUNid = long(sm_config['LUNid'])
        if not len(self._attach_LUN_bylunid(LUNid)):
            raise xs_errors.XenError('VDIUnavailable')
        return os.path.join(self.path,"LUN%d" % LUNid)

    # This function takes an ISCSI device and populate it with
    # a dictionary of available LUNs on that target.
    def print_LUNs(self):
        self.LUNs = {}
        if os.path.exists(self.path):
            dom0_disks = util.dom0_disks()
            for file in util.listdir(self.path):
                if file.find("LUN") != -1 and file.find("_") == -1:
                    vdi_path = os.path.join(self.path,file)
                    if os.path.realpath(vdi_path) in dom0_disks:
                        util.SMlog("Hide dom0 boot disk LUN")
                    else:
                        LUNid = file.replace("LUN","")
                        obj = self.vdi(self.uuid)
                        obj._query(vdi_path, LUNid)
                        self.LUNs[obj.uuid] = obj

    def print_entries(self, map):
        dom = xml.dom.minidom.Document()
        element = dom.createElement("iscsi-target-iqns")
        dom.appendChild(element)
        count = 0
        for address,tpgt,iqn in map:
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
        print >>sys.stderr,dom.toprettyxml()

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
        if regex.search(s,0):
            return False
        regex = re.compile("LUN")
        return regex.search(s, 0)    
        

    def _get_iscsi_interfaces(self):
        result = []
        try:
            # Get all configured iscsiadm interfaces
            cmd = ["iscsiadm", "-m", "iface"]
            (stdout,stderr)= iscsilib.exn_on_failure(cmd,
                                                    "Failure occured querying iscsi daemon");
            # Get the interface (first column) from a line such as default
            # tcp,<empty>,<empty>,<empty>,<empty>
            for line in stdout.split("\n"):
                line_element = line.split(" ")
                interface_name = line_element[0];
                # ignore interfaces which aren't marked as starting with
                # c_.
                if len(line_element)==2 and interface_name[:2]=="c_":
                    result.append(interface_name)
        except:
            # Ignore exception from exn on failure, still return the default
            # interface
            pass
        # In case there are no configured interfaces, still add the default
        # interface
        if len(result) == 0:
            result.append("default")
        return result

if __name__ == '__main__':
    SRCommand.run(ISCSISR, DRIVER_INFO)
else:
    SR.registerSR(ISCSISR)
