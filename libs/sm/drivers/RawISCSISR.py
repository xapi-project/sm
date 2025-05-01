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
#          matches with drivers/ISCSISR
#

from sm import SR
from sm import BaseISCSI
from sm import LUNperVDI
from sm.core import util

CAPABILITIES = ["SR_PROBE", "VDI_CREATE", "VDI_DELETE", "VDI_ATTACH",
                "VDI_DETACH", "VDI_INTRODUCE"]

CONFIGURATION = [['target', 'IP address or hostname of the iSCSI target (required)'], \
                  ['targetIQN', 'The IQN of the target LUN group to be attached (required)'], \
                  ['chapuser', 'The username to be used during CHAP authentication (optional)'], \
                  ['chappassword', 'The password to be used during CHAP authentication (optional)'], \
                  ['incoming_chapuser', 'The incoming username to be used during bi-directional CHAP authentication (optional)'], \
                  ['incoming_chappassword', 'The incoming password to be used during bi-directional CHAP authentication (optional)'], \
                  ['port', 'The network port number on which to query the target (optional)'], \
                  ['multihomed', 'Enable multi-homing to this target, true or false (optional, defaults to same value as host.other_config:multipathing)'],
                  ['force_tapdisk', 'Force use of tapdisk, true or false (optional, defaults to false)'],
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


class RawISCSISR(BaseISCSI.BaseISCSISR):
    """Raw ISCSI storage repository"""

    def handles(type):
        if type == "iscsi":
            return True
        return False
    handles = staticmethod(handles)

    def load(self, vdi_uuid):
        super(RawISCSISR, self).load(vdi_uuid)
        self.managed = True

    def detach(self, sr_uuid):
        super(RawISCSISR, self).detach(sr_uuid, True)

    def vdi(self, uuid):
        return ISCSIVDI(self, uuid)


class ISCSIVDI(LUNperVDI.RAWVDI):
    def load(self, vdi_uuid):
        super(ISCSIVDI, self).load(vdi_uuid)
        self.managed = True


# SR registration at import
SR.registerSR(RawISCSISR)
