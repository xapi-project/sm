#!/usr/bin/env python
#
# Copyright (C) 2020  Vates SAS - ronan.abhamon@vates.fr
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#


from linstorvolumemanager import LinstorVolumeManager
import linstor
import re
import util


class LinstorJournalerError(Exception):
    pass

# ==============================================================================


class LinstorJournaler:
    """
    Simple journaler that uses LINSTOR properties for persistent "storage".
    A journal is a id-value pair, and there can be only one journal for a
    given id. An identifier is juste a transaction name.
    """

    REG_TYPE = re.compile('^([^/]+)$')
    REG_TRANSACTION = re.compile('^[^/]+/([^/]+)$')

    """
    Types of transaction in the journal.
    """
    CLONE = 'clone'
    INFLATE = 'inflate'

    @staticmethod
    def default_logger(*args):
        print(args)

    def __init__(self, uri, group_name, logger=default_logger.__func__):
        self._namespace = '{}journal/'.format(
            LinstorVolumeManager._build_sr_namespace()
        )

        def connect():
            self._journal = linstor.KV(
                LinstorVolumeManager._build_group_name(group_name),
                uri=uri,
                namespace=self._namespace
            )

        util.retry(
            connect,
            maxretry=60,
            exceptions=[linstor.errors.LinstorNetworkError]
        )
        self._logger = logger

    def create(self, type, identifier, value):
        # TODO: Maybe rename to 'add' in the future (in Citrix code too).

        key = self._get_key(type, identifier)

        # 1. Ensure transaction doesn't exist.
        current_value = self.get(type, identifier)
        if current_value is not None:
            raise LinstorJournalerError(
                'Journal transaction already exists for \'{}:{}\': {}'
                .format(type, identifier, current_value)
            )

        # 2. Write!
        try:
            self._reset_namespace()
            self._logger(
                'Create journal transaction \'{}:{}\''.format(type, identifier)
            )
            self._journal[key] = str(value)
        except Exception as e:
            try:
                self._journal.pop(key, 'empty')
            except Exception as e2:
                self._logger(
                    'Failed to clean up failed journal write: {} (Ignored)'
                    .format(e2)
                )

            raise LinstorJournalerError(
                'Failed to write to journal: {}'.format(e)
            )

    def remove(self, type, identifier):
        key = self._get_key(type, identifier)
        try:
            self._reset_namespace()
            self._logger(
                'Destroy journal transaction \'{}:{}\''
                .format(type, identifier)
            )
            self._journal.pop(key)
        except Exception as e:
            raise LinstorJournalerError(
                'Failed to remove transaction \'{}:{}\': {}'
                .format(type, identifier, e)
            )

    def get(self, type, identifier):
        return self._journal.get(self._get_key(type, identifier))

    def get_all(self, type):
        entries = {}

        self._journal.namespace = self._namespace + '{}/'.format(type)
        for (key, value) in self._journal.items():
            res = self.REG_TYPE.match(key)
            if res:
                identifier = res.groups()[0]
                entries[identifier] = value
        return entries

    # Added to compatibility with Citrix API.
    def getAll(self, type):
        return self.get_all(type)

    def has_entries(self, identifier):
        self._reset_namespace()
        for (key, value) in self._journal.items():
            res = self.REG_TRANSACTION.match(key)
            if res:
                current_identifier = res.groups()[0]
                if current_identifier == identifier:
                    return True
        return False

    # Added to compatibility with Citrix API.
    def hasJournals(self, identifier):
        return self.has_entries(identifier)

    def _reset_namespace(self):
        self._journal.namespace = self._namespace

    @staticmethod
    def _get_key(type, identifier):
        return '{}/{}'.format(type, identifier)
