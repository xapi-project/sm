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
# Xensource error codes
#

import os
import xml.dom.minidom
import SR
import util

XML_DEFS = '/opt/xensource/sm/XE_SR_ERRORCODES.xml'


class XenError(Exception):
    def __new__(self, key, opterr=None):
        # Check the XML definition file exists
        if not os.path.exists(XML_DEFS):
            raise Exception("No XML def file found")

        # Read the definition list
        errorlist = self._fromxml('SM-errorcodes')

        ########DEBUG#######
        #for val in self.errorlist.keys():
        #    subdict = self.errorlist[val]
        #    print "KEY [%s]" % val
        #    for subval in subdict.keys():
        #        print "\tSUBKEY: %s, VALUE: %s" % (subval,subdict[subval])
        ########END#######

        # Now find the specific error
        if key in errorlist:
            subdict = errorlist[key]
            errorcode = int(subdict['value'])
            errormessage = subdict['description']
            if opterr is not None:
                errormessage += " [opterr=%s]" % opterr
            util.SMlog("Raising exception [%d, %s]" % (errorcode, errormessage))
            return SR.SROSError(errorcode, errormessage)

        # development error
        return SR.SROSError(1, "Error reporting error, unknown key %s" % key)

    @staticmethod
    def _fromxml(tag):
        dom = xml.dom.minidom.parse(XML_DEFS)
        objectlist = dom.getElementsByTagName(tag)[0]

        errorlist = {}
        for node in objectlist.childNodes:
            taglist = {}
            newval = False
            for n in node.childNodes:
                if n.nodeType == n.ELEMENT_NODE and node.nodeName == 'code':
                    taglist[n.nodeName] = ""
                    for e in n.childNodes:
                        if e.nodeType == e.TEXT_NODE:
                            newval = True
                            taglist[n.nodeName] += e.data
            if newval:
                name = taglist['name']
                errorlist[name] = taglist
        return errorlist
