#!/usr/bin/python3

# Copyright (C) Cloud Software Group, Inc.
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

"""
Health check for SR, to be triggered periodically by a systemd timer. What is checked is
SR implementation type dependent.
"""

from sm import SR
from sm.core import util
from sm.core import xs_errors

def check_xapi_is_enabled(session, hostref):
    host = session.xenapi.host.get_record(hostref)
    return host['enabled']


def main():
    """
    For all locally plugged SRs check that they are healthy
    """
    try:
        session = util.get_localAPI_session()
    except xs_errors.SROSError:
        util.SMlog("Unable to open local XAPI session", priority=util.LOG_ERR)
        return

    try:
        localhost = util.get_localhost_ref(session)
        if not check_xapi_is_enabled(session, localhost):
            # Xapi not enabled, skip and let the next timer trigger this
            return

        sm_types = [x['type'] for x in session.xenapi.SM.get_all_records_where(
            'field "required_api_version" = "1.0"').values()]
        for sm_type in sm_types:
            srs = session.xenapi.SR.get_all_records_where(
                f'field "type" = "{sm_type}"')
            for sr in srs:
                pbds = session.xenapi.PBD.get_all_records_where(
                    f'field "SR" = "{sr}" and field "host" = "{localhost}"')
                if not pbds:
                    continue

                pbd_ref, pbd = pbds.popitem()
                if not pbd['currently_attached']:
                    continue

                sr_uuid = srs[sr]['uuid']
                sr_obj = SR.SR.from_uuid(session, sr_uuid)
                sr_obj.check_sr(sr_uuid)
    finally:
        session.xenapi.session.logout()


if __name__ == "__main__":
    main()
