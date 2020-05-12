#!/usr/bin/python

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

import util
import time, os, sys, re
import xs_errors
import lock
import mpath_cli
import mpp_mpathutil
import glob
import json

def get_dm_major():
    global cached_DM_maj
    if not cached_DM_maj:
        try:
            line = filter(lambda x: x.endswith('device-mapper\n'), open('/proc/devices').readlines())
            cached_DM_maj = int(line[0].split()[0])
        except:
            pass
    return cached_DM_maj

def mpc_exit(session, code):
    if session is not None:
        try:
            session.xenapi.logout()
        except:
            pass
    sys.exit(code)

def match_host_id(s):
    regex = re.compile("^INSTALLATION_UUID")
    return regex.search(s, 0)

def get_localhost_uuid():
    filename = '/etc/xensource-inventory'
    try:
        f = open(filename, 'r')
    except:
        raise xs_errors.XenError('EIO', \
              opterr="Unable to open inventory file [%s]" % filename)
    domid = ''
    for line in filter(match_host_id, f.readlines()):
        domid = line.split("'")[1]
    return domid

def match_dmpLUN(s):
    regex = re.compile("[0-9]*:[0-9]*:[0-9]*:[0-9]*")
    return regex.search(s, 0)

def match_pathup(s):
    match = re.match(r'.*\d+:\d+:\d+:\d+\s+\S+\s+\S+\s+\S+\s+(\S+)', s)
    if match:
        path_status = match.group(1)
    if path_status in ['faulty', 'shaky', 'failed']:
        return False
    return True

def _tostring(l):
    return str(l)

def get_path_count(SCSIid):
    count = 0
    total = 0
    lines = mpath_cli.get_topology(SCSIid)
    for line in filter(match_dmpLUN,lines):
        total += 1
        if match_pathup(line):
            count += 1
    return (count, total)

def get_root_dev_major():
    buf = os.stat('/')
    devno = buf.st_dev
    return os.major(devno)

# @key:     key to update
# @SCSIid:  SCSI id of multipath map
# @entry:   string representing previous value
# @remove:  callback to remove key
# @add:     callback to add key/value pair
def update_config(key, SCSIid, entry, remove, add, mpp_path_update = False):
    if mpp_path_update:
        remove('multipathed')
        remove(key)
        remove('MPPEnabled')
        add('MPPEnabled','true')
        add('multipathed','true')
        add(key,str(entry))
        return

    path = os.path.join(MAPPER_DIR, SCSIid)
    util.SMlog("MPATH: Updating entry for [%s], current: %s" % (SCSIid,entry))
    if os.path.exists(path):
        count, total = get_path_count(SCSIid)
        max = 0
	if len(entry) != 0:
            try:
                p = entry.strip('[')
                p = p.strip(']')
                q = p.split(',')
                max = int(q[1])
            except:
                pass
        if total > max:
            max = total
        newentry = [count, max]
        if str(newentry) != entry:
            remove('multipathed')
            remove(key)
            add('multipathed','true')
            add(key,str(newentry))
            util.SMlog("MPATH: Set val: %s" % str(newentry))
    else:
        util.SMlog('MPATH: device %s gone' % (SCSIid))
        remove('multipathed')
        remove(key)

def get_SCSIidlist(devconfig, sm_config):
    SCSIidlist = []
    if sm_config.has_key('SCSIid'):
        SCSIidlist = sm_config['SCSIid'].split(',')
    elif devconfig.has_key('SCSIid'):
        SCSIidlist.append(devconfig['SCSIid'])
    elif devconfig.has_key('provider'):
        SCSIidlist.append(devconfig['ScsiId'])
    else:
        for key in sm_config:
            if util._isSCSIid(key):
                SCSIidlist.append(re.sub("^scsi-","",key))
    return SCSIidlist

if __name__ == '__main__':
    supported = ['iscsi','lvmoiscsi','rawhba','lvmohba', 'ocfsohba', 'ocfsoiscsi', 'netapp','lvmofcoe', 'gfs2']

    LOCK_TYPE_HOST = "host"
    LOCK_NS1 = "mpathcount1"
    LOCK_NS2 = "mpathcount2"

    MAPPER_DIR = "/dev/mapper"
    mpp_path_update = False
    match_bySCSIid = False
    mpath_enabled = True

    if len(sys.argv) == 3:
        match_bySCSIid = True
        SCSIid = sys.argv[1]
        mpp_path_update = True
        mpp_entry = sys.argv[2]

    cached_DM_maj = None

    try:
        session = util.get_localAPI_session()
    except:
        print "Unable to open local XAPI session"
        sys.exit(-1)

    localhost = session.xenapi.host.get_by_uuid(get_localhost_uuid())
    # Check whether multipathing is enabled (either for root dev or SRs)
    try:
        if get_root_dev_major() != get_dm_major():
            hconf = session.xenapi.host.get_other_config(localhost)
            assert(hconf['multipathing'] == 'true')
            mpath_enabled = True
    except:
        mpath_enabled = False

    # Check root disk if multipathed
    try:
        if get_root_dev_major() == get_dm_major():
            def _remove(key):
                session.xenapi.host.remove_from_other_config(localhost,key)
            def _add(key, val):
                session.xenapi.host.add_to_other_config(localhost,key,val)
            config = session.xenapi.host.get_other_config(localhost)
            maps = mpath_cli.list_maps()
            # Ensure output headers are not in the list
            if 'name' in maps:
                maps.remove('name')
            # first map will always correspond to the root dev, dm-0
            assert(len(maps) > 0)
            i = maps[0]
            if (not match_bySCSIid) or i == SCSIid:
                util.SMlog("Matched SCSIid %s, updating " \
                        " Host.other-config:mpath-boot " % i)
                key="mpath-boot"
                if not config.has_key(key):
                    update_config(key, i, "", _remove, _add)
                else:
                    update_config(key, i, config[key], _remove, _add)
    except:
        util.SMlog("MPATH: Failure updating Host.other-config:mpath-boot db")
        mpc_exit(session, -1)

    try:
        pbds = session.xenapi.PBD.get_all_records_where("field \"host\" = \"%s\"" % localhost)
    except:
        mpc_exit(session,-1)

    try:
        for pbd in pbds:
            def remove(key):
                session.xenapi.PBD.remove_from_other_config(pbd,key)
            def add(key, val):
                session.xenapi.PBD.add_to_other_config(pbd,key,val)
            record = pbds[pbd]
            config = record['other_config']
            SR = record['SR']
            srtype = session.xenapi.SR.get_type(SR)
            if srtype in supported:
                devconfig = record["device_config"]
                sm_config = session.xenapi.SR.get_sm_config(SR)
                SCSIidlist = get_SCSIidlist(devconfig, sm_config)
                if not len(SCSIidlist):
                    continue
                for i in SCSIidlist:
                    if match_bySCSIid and i != SCSIid:
                        continue
                    util.SMlog("Matched SCSIid, updating %s" % i)
                    key = "mpath-" + i
                    if not mpath_enabled:
                        remove(key)
                        remove('multipathed')
                    elif mpp_path_update:
                        util.SMlog("Matched SCSIid, updating entry %s" % str(mpp_entry))
                        update_config(key, i, mpp_entry, remove, add, mpp_path_update)
                    else:
                        if not config.has_key(key):
                            update_config(key, i, "", remove, add)
                        else:
                            update_config(key, i, config[key], remove, add)
    except:
        util.SMlog("MPATH: Failure updating db. %s" % sys.exc_info())
        mpc_exit(session, -1)

    util.SMlog("MPATH: Update done")

    mpc_exit(session,0)
