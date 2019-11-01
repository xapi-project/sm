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
# cifutils: Extract credentials from SR (e.g ISOSR, SMBSR) dconf

import util
import xs_errors


class CIFSException(Exception):
    def __init__(self, errstr):
        self.errstr = errstr


def getDconfPasswordKey(prefix=""):
    key_password = prefix + 'password'
    key_secret = prefix + 'password_secret'
    return key_password, key_secret


def containsPassword(dconf, prefix=""):
    key_password, key_secret = getDconfPasswordKey(prefix)
    return ((key_password in dconf) or (key_secret in dconf))


def containsCredentials(dconf, prefix=""):
    return ((('username' in dconf)) and (containsPassword(dconf, prefix)))


def splitDomainAndUsername(uname):

    username = None
    domain = None
    dom_username = uname.split('\\')

    if len(dom_username) == 1:
        domain = None
        username = dom_username[0]
    elif len(dom_username) == 2:
        domain = dom_username[0]
        username = dom_username[1]
    else:
        raise CIFSException("A maximum of 2 tokens are expected "
                            "(<domain>\<username>). {} were given."
                            .format(len(dom_username)))
    return username, domain


def getCIFCredentials(dconf, session, prefix=""):
    credentials = None
    domain = None
    if (containsCredentials(dconf, prefix)):

        username, domain = splitDomainAndUsername(dconf['username'])
        credentials = {}
        credentials["USER"] = util.to_plain_string(username)

        key_password, key_secret = getDconfPasswordKey(prefix)
        if key_secret in dconf:
            password = util.get_secret(session, dconf[key_secret])
        else:
            password = dconf[key_password]

        credentials["PASSWD"] = util.to_plain_string(password)

        domain = util.to_plain_string(domain)

    return credentials, domain
