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

import re
import util
import glob
import os

SYSFS_NET_PATH='/sys/class/net'

def parse_fcoe_eth_info():
    fcoe_eth_info = {}
    # create a dictionary of rport to eth
    try:
        cmd = ['fcoeadm', '-l']
        regex = re.compile("eth[0-9]")
        for line in util.doexec(cmd)[1].split('\n'):
            if line.find("Interface") != -1:
                searchObj = regex.search(line, 0)
                if searchObj:
                    eth = searchObj.group()
                    util.SMlog("eth: %s" % eth)
            if line.find("rport") != -1:
                str1, str2 = line.split(":", 1)
                fcoe_eth_info[str2.strip()] = eth
                eth = ""
    except:
        pass

    return fcoe_eth_info

def parse_fcoe_port_name_info():
    fcoe_port_info = []
    fcoe_ports = glob.glob(os.path.join(SYSFS_NET_PATH, "eth*"))
    for port in fcoe_ports:
        try:
            cmd= ['fcoeadm', '-i', os.path.basename(port)]
            for line in util.doexec(cmd)[1].split('\n'):
                if line.find("Port Name") != -1:
                    str1, str2 = line.split(":")
                    str2 = str2.strip()
                    port = int(str2,0)
                    util.SMlog(" port is %d" % port)
                    fcoe_port_info.append(port)
                    break
        except:
            pass

    return fcoe_port_info
