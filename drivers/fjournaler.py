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
# File-based journaling

from __future__ import print_function
import os
import errno

import util
from journaler import JournalerException

SEPARATOR = "_"


class Journaler:
    """Simple file-based journaler. A journal is a id-value pair, and there
    can be only one journal for a given id."""

    def __init__(self, dir):
        self.dir = dir

    def create(self, type, id, val):
        """Create an entry of type "type" for "id" with the value "val".
        Error if such an entry already exists."""
        valExisting = self.get(type, id)
        if valExisting:
            raise JournalerException("Journal already exists for '%s:%s': %s" \
                    % (type, id, valExisting))
        path = self._getPath(type, id)
        f = open(path, "w")
        f.write(val)
        f.close()

    def remove(self, type, id):
        """Remove the entry of type "type" for "id". Error if the entry doesn't
        exist."""
        val = self.get(type, id)
        if not val:
            raise JournalerException("No journal for '%s:%s'" % (type, id))
        path = self._getPath(type, id)
        os.unlink(path)

    def get(self, type, id):
        """Get the value for the journal entry of type "type" for "id".
        Return None if no such entry exists"""
        path = self._getPath(type, id)
        if not util.pathexists(path):
            return None
        try:
            f = open(path, "r")
        except IOError as e:
            if e.errno == errno.ENOENT:
                # the file can disappear any time, since there is no locking
                return None
            raise
        val = f.readline()
        return val

    def getAll(self, type):
        """Get a mapping id->value for all entries of type "type" """
        fileList = os.listdir(self.dir)
        entries = dict()
        for fileName in fileList:
            if not fileName.startswith(type):
                continue
            parts = fileName.split(SEPARATOR, 2)
            if len(parts) != 2:
                raise JournalerException("Bad file name: %s" % fileName)
            t, id = parts
            if t != type:
                continue
            val = self.get(type, id)
            if val:
                entries[id] = val
        return entries

    def _getPath(self, type, id):
        name = "%s%s%s" % (type, SEPARATOR, id)
        path = os.path.join(self.dir, name)
        return path
