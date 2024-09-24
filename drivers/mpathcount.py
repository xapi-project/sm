#!/usr/bin/python3

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
import os
import sys
import re
import xs_errors
import mpath_cli
import json

supported = ['iscsi', 'lvmoiscsi', 'rawhba', 'lvmohba', 'ocfsohba', 'ocfsoiscsi', 'netapp', 'lvmofcoe', 'gfs2']

LOCK_TYPE_HOST = "host"
LOCK_NS1 = "mpathcount1"
LOCK_NS2 = "mpathcount2"

MAPPER_DIR = "/dev/mapper"
MPATHS_DIR = "/dev/shm"
MPATH_FILE_NAME = "/dev/shm/mpath_status"
match_bySCSIid = False
mpath_enabled = True
SCSIid = 'NOTSUPPLIED'

cached_DM_maj = None

def get_dm_major():
    global cached_DM_maj
    if not cached_DM_maj:
        try:
            line = [x for x in open('/proc/devices').readlines() if x.endswith('device-mapper\n')]
            cached_DM_maj = int(line[0].split()[0])
        except:
            pass
    return cached_DM_maj


def mpc_exit(session, code):
    if session is not None:
        try:
            session.xenapi.session.logout()
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
    for line in filter(match_dmpLUN, lines):
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
# @mpath_status:  map to record multipath status
def update_config(key, SCSIid, entry, remove, add, mpath_status=None):
    path = os.path.join(MAPPER_DIR, SCSIid)
    util.SMlog("MPATH: Updating entry for [%s], current: %s" % (SCSIid, entry))
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
            add('multipathed', 'true')
            add(key, str(newentry))
            util.SMlog("MPATH: Set val: %s" % str(newentry))
        if mpath_status != None:
            mpath_status.update({str(key): f"{count}/{max}"})
    else:
        util.SMlog('MPATH: device %s gone' % (SCSIid))
        remove('multipathed')
        remove(key)


def get_SCSIidlist(devconfig, sm_config):
    SCSIidlist = []
    if 'SCSIid' in sm_config:
        SCSIidlist = sm_config['SCSIid'].split(',')
    elif 'SCSIid' in devconfig:
        SCSIidlist.append(devconfig['SCSIid'])
    elif 'provider' in devconfig:
        SCSIidlist.append(devconfig['ScsiId'])
    else:
        for key in sm_config:
            if util._isSCSIid(key):
                SCSIidlist.append(re.sub("^scsi-", "", key))
    return SCSIidlist


def check_root_disk(config, maps, remove, add):
    if get_root_dev_major() == get_dm_major():
        # Ensure output headers are not in the list
        if 'name' in maps:
            maps.remove('name')
        # first map will always correspond to the root dev, dm-0
        assert(len(maps) > 0)
        i = maps[0]
        if (not match_bySCSIid) or i == SCSIid:
            util.SMlog("Matched SCSIid %s, updating " \
                    " Host.other-config:mpath-boot " % i)
            key = "mpath-boot"
            if key not in config:
                update_config(key, i, "", remove, add)
            else:
                update_config(key, i, config[key], remove, add)


def check_devconfig(devconfig, sm_config, config, remove, add, mpath_status=None):
    SCSIidlist = get_SCSIidlist(devconfig, sm_config)
    if not len(SCSIidlist):
        return
    for i in SCSIidlist:
        if match_bySCSIid and i != SCSIid:
            continue
        util.SMlog("Matched SCSIid, updating %s" % i)
        key = "mpath-" + i
        if not mpath_enabled:
            remove(key)
            remove('multipathed')
        else:
            if key not in config:
                update_config(key, i, "", remove, add, mpath_status)
            else:
                update_config(key, i, config[key], remove, add, mpath_status)


def check_xapi_is_enabled(session, hostref):
    host = session.xenapi.host.get_record(hostref)
    if not host['enabled']:
        util.SMlog("Xapi is not enabled, exiting")
        mpc_exit(session, 0)


if __name__ == '__main__':
    try:
        session = util.get_localAPI_session()
    except:
        print("Unable to open local XAPI session")
        sys.exit(-1)

    localhost = session.xenapi.host.get_by_uuid(get_localhost_uuid())
    check_xapi_is_enabled(session, localhost)
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
        def _remove(key):
            session.xenapi.host.remove_from_other_config(localhost, key)


        def _add(key, val):
            session.xenapi.host.add_to_other_config(localhost, key, val)
        config = session.xenapi.host.get_other_config(localhost)
        maps = mpath_cli.list_maps()
        check_root_disk(config, maps, _remove, _add)

    except:
        util.SMlog("MPATH: Failure updating Host.other-config:mpath-boot db")
        mpc_exit(session, -1)

    try:
        pbds = session.xenapi.PBD.get_all_records_where("field \"host\" = \"%s\"" % localhost)
    except:
        mpc_exit(session, -1)

    try:
        mpath_status = {}
        for pbd in pbds:
            def remove(key):
                session.xenapi.PBD.remove_from_other_config(pbd, key)


            def add(key, val):
                session.xenapi.PBD.add_to_other_config(pbd, key, val)
            record = pbds[pbd]
            config = record['other_config']
            SR = record['SR']
            srtype = session.xenapi.SR.get_type(SR)
            if srtype in supported:
                devconfig = record["device_config"]
                sm_config = session.xenapi.SR.get_sm_config(SR)
                check_devconfig(devconfig, sm_config, config, remove, add, mpath_status)
        mpath_status = mpath_status if mpath_enabled else {}
        util.atomicFileWrite(MPATH_FILE_NAME, MPATHS_DIR, json.dumps(mpath_status))
        os.chmod(MPATH_FILE_NAME, 0o0644)
    except:
        util.SMlog("MPATH: Failure updating db. %s" % sys.exc_info())
        mpc_exit(session, -1)

    util.SMlog("MPATH: Update done")

    mpc_exit(session, 0)
