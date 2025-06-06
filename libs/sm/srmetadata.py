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
# Functions to read and write SR metadata
#
from io import SEEK_SET

from sm.core import util
from sm import metadata
import os
from sm.core import xs_errors
from sm import lvutil
import xml.sax.saxutils

# A metadata file is considered to be made up of 512 byte sectors.
# Most of the information in it is in the form of fragments of XML.
# The first four contain SR information - well, the first actually
# contains a header, and the next three contain bits of XML representing SR
# info, but the four are treated as a unit. Information in the header includes
# the length of the part of the file that's in use.
# Subsequent sectors, if they are in use, contain VDI information - in the LVM
# case they take two sectors each. VDI information might mark the VDI as
# having been deleted, in which case the sectors used to contain this info can
# potentially be reused when a new VDI is subsequently added.

# String data in this module takes the form of normal Python unicode `str`
# instances, or UTF-8 encoded `bytes`, depending on circumstance. In `dict`
# instances such as are used to represent SR and VDI info, `str` is used (as
# these may be returned to, or have been supplied by, this module's callers).
# Data going into or taken from a metadata file is `bytes`. XML and XML
# fragments come under this category, so are `bytes`. XML tag names are `str`
# instances, as these are also used as `dict` keys.


SECTOR_SIZE = 512
XML_HEADER = b"<?xml version=\"1.0\" ?>"
MAX_METADATA_LENGTH_SIZE = 10
OFFSET_TAG = 'offset'

# define xml tags for metadata
ALLOCATION_TAG = 'allocation'
NAME_LABEL_TAG = 'name_label'
NAME_DESCRIPTION_TAG = 'name_description'
VDI_TAG = 'vdi'
VDI_DELETED_TAG = 'deleted'
UUID_TAG = 'uuid'
IS_A_SNAPSHOT_TAG = 'is_a_snapshot'
SNAPSHOT_OF_TAG = 'snapshot_of'
TYPE_TAG = 'type'
VDI_TYPE_TAG = 'vdi_type'
READ_ONLY_TAG = 'read_only'
MANAGED_TAG = 'managed'
SNAPSHOT_TIME_TAG = 'snapshot_time'
METADATA_OF_POOL_TAG = 'metadata_of_pool'
SVID_TAG = 'svid'
LUN_LABEL_TAG = 'll'
MAX_VDI_NAME_LABEL_DESC_LENGTH = SECTOR_SIZE - 2 * len(NAME_LABEL_TAG) - \
    2 * len(NAME_DESCRIPTION_TAG) - len(VDI_TAG) - 12

ATOMIC_UPDATE_PARAMS_AND_OFFSET = {NAME_LABEL_TAG: 2,
                                        NAME_DESCRIPTION_TAG: 3}
SR_INFO_SIZE_IN_SECTORS = 4
HEADER_SEP = ':'
METADATA_UPDATE_OBJECT_TYPE_TAG = 'objtype'
METADATA_OBJECT_TYPE_SR = 'sr'
METADATA_OBJECT_TYPE_VDI = 'vdi'
METADATA_BLK_SIZE = 512


# ----------------- # General helper functions - begin # -----------------
def open_file(path, write=False):
    if write:
        try:
            file_p = open(path, 'wb+')
        except OSError as e:
            raise OSError(
                "Failed to open file %s for read-write. Error: %s" %
                (path, e.errno))
    else:
        try:
            file_p = open(path, 'rb')
        except OSError as e:
            raise OSError(
                "Failed to open file %s for read. Error: %s" %
                (path, e.errno))
    return file_p


def file_write_wrapper(fd, offset, data):
    """
    Writes data to a file at a given offset. Padding (consisting of spaces)
    may be written out after the given data to ensure that complete blocks are
    written.
    """
    try:
        blocksize = METADATA_BLK_SIZE
        length = len(data)
        newlength = length
        if length % blocksize:
            newlength = length + (blocksize - length % blocksize)
        fd.seek(offset, SEEK_SET)
        to_write = data + b' ' * (newlength - length)
        return fd.write(to_write)
    except OSError as e:
        raise OSError(
            "Failed to write file with params %s. Error: %s" %
            ([fd, offset, blocksize, data], e.errno))


def file_read_wrapper(fd, offset, bytesToRead=METADATA_BLK_SIZE):
    """
    Reads data from a file at a given offset. If not specified, the amount of
    data to read defaults to one block.
    """
    try:
        fd.seek(offset, SEEK_SET)
        return fd.read(bytesToRead)
    except OSError as e:
        raise OSError(
            "Failed to read file with params %s. Error: %s" %
            ([fd, offset, bytesToRead], e.errno))


def to_utf8(s):
    return s.encode("utf-8")


def from_utf8(bs):
    return bs.decode("utf-8")


# get a range which is block aligned, contains 'offset' and allows
# length bytes to be written
def getBlockAlignedRange(offset, length):
    # It looks like offsets and lengths are in reality always sector aligned,
    # and since a block and a sector are the same size we could probably do
    # without this code.
    # There methods elsewhere in this module (updateSR, getMetadataForWrite)
    # that appear try to cope with the possibility of the block-aligned range
    # for SR info also containing VDI info, or vice versa. On the face of it,
    # that's impossible, and so there's scope for simplification there too.
    block_size = METADATA_BLK_SIZE
    lower = 0
    if offset % block_size == 0:
        lower = offset
    else:
        lower = offset - offset % block_size

    upper = lower + block_size

    while upper < (lower + length):
        upper += block_size

    return (lower, upper)


def buildHeader(length, major=metadata.MD_MAJOR, minor=metadata.MD_MINOR):
    len_fmt = "%%-%ds" % MAX_METADATA_LENGTH_SIZE
    return to_utf8(metadata.HDR_STRING
                   + HEADER_SEP
                   + (len_fmt % length)
                   + HEADER_SEP
                   + str(major)
                   + HEADER_SEP
                   + str(minor))


def unpackHeader(header):
    vals = from_utf8(header).split(HEADER_SEP)
    if len(vals) != 4 or vals[0] != metadata.HDR_STRING:
        util.SMlog("Exception unpacking metadata header: "
                   "Error: Bad header '%s'" % (header))
        raise xs_errors.XenError('MetadataError', \
                        opterr='Bad header')
    return (vals[0], vals[1], vals[2], vals[3])


def getSector(s):
    sector_fmt = b"%%-%ds" % SECTOR_SIZE
    return sector_fmt % s


def buildXMLSector(tagName, value):
    # truncate data if we breach the 512 limit
    tag_bytes = to_utf8(tagName)
    value_bytes = to_utf8(value)

    elt = b"<%s>%s</%s>" % (tag_bytes, value_bytes, tag_bytes)
    if len(elt) > SECTOR_SIZE:
        length = util.unictrunc(value_bytes, SECTOR_SIZE - 2 * len(tag_bytes) - 5)
        util.SMlog('warning: SR %s truncated from %d to %d bytes'
                   % (tagName, len(value_bytes), length))
        elt = b"<%s>%s</%s>" % (tag_bytes, value_bytes[:length], tag_bytes)

    return getSector(elt)


def buildXMLElement(tag, value_dict):
    return to_utf8("<%s>%s</%s>" % (tag, value_dict[tag], tag))


def openingTag(tag):
    return b"<%s>" % to_utf8(tag)


def closingTag(tag):
    return b"</%s>" % to_utf8(tag)


def buildParsableMetadataXML(info):
    tag = to_utf8(metadata.XML_TAG)
    return b"%s<%s>%s</%s>" % (XML_HEADER, tag, info, tag)


def updateLengthInHeader(fd, length, major=metadata.MD_MAJOR, \
                         minor=metadata.MD_MINOR):
    try:
        md = file_read_wrapper(fd, 0)
        updated_md = buildHeader(length, major, minor)
        updated_md += md[SECTOR_SIZE:]

        # Now write the new length
        file_write_wrapper(fd, 0, updated_md)
    except Exception as e:
        util.SMlog("Exception updating metadata length with length: %d."
                   "Error: %s" % (length, str(e)))
        raise


def getMetadataLength(fd):
    try:
        sector1 = \
            file_read_wrapper(fd, 0, SECTOR_SIZE).strip()
        hdr = unpackHeader(sector1)
        return int(hdr[1])
    except Exception as e:
        util.SMlog("Exception getting metadata length: "
                   "Error: %s" % str(e))
        raise


# ----------------- # General helper functions - end # -----------------
class MetadataHandler:

    VDI_INFO_SIZE_IN_SECTORS = None

    # constructor
    def __init__(self, path=None, write=True):

        self.fd = None
        self.path = path
        if self.path is not None:
            self.fd = open_file(self.path, write)

    def __del__(self):
        if self.fd:
            self.fd.close()

    @property
    def vdi_info_size(self):
        return self.VDI_INFO_SIZE_IN_SECTORS * SECTOR_SIZE

    def spaceAvailableForVdis(self, count):
        raise NotImplementedError("spaceAvailableForVdis is undefined")

    # common utility functions
    def getMetadata(self, params={}):
        try:
            sr_info = {}
            vdi_info = {}
            try:
                md = self.getMetadataInternal(params)
                sr_info = md['sr_info']
                vdi_info = md['vdi_info']
            except:
                # Maybe there is no metadata yet
                pass

        except Exception as e:
            util.SMlog('Exception getting metadata. Error: %s' % str(e))
            raise xs_errors.XenError('MetadataError', \
                         opterr='%s' % str(e))

        return (sr_info, vdi_info)

    def writeMetadata(self, sr_info, vdi_info):
        try:
            self.writeMetadataInternal(sr_info, vdi_info)
        except Exception as e:
            util.SMlog('Exception writing metadata. Error: %s' % str(e))
            raise xs_errors.XenError('MetadataError', \
                         opterr='%s' % str(e))

    # read metadata for this SR and find if a metadata VDI exists
    def findMetadataVDI(self):
        try:
            vdi_info = self.getMetadata()[1]
            for offset in vdi_info.keys():
                if vdi_info[offset][TYPE_TAG] == 'metadata' and \
                    vdi_info[offset][IS_A_SNAPSHOT_TAG] == '0':
                    return vdi_info[offset][UUID_TAG]

            return None
        except Exception as e:
            util.SMlog('Exception checking if SR metadata a metadata VDI.' \
                       'Error: %s' % str(e))
            raise xs_errors.XenError('MetadataError', \
                         opterr='%s' % str(e))

    # update the SR information or one of the VDIs information
    # the passed in map would have a key 'objtype', either sr or vdi.
    # if the key is sr, the following might be passed in
    #   SR name-label
    #   SR name_description
    # if the key is vdi, the following information per VDI may be passed in
    #   uuid - mandatory
    #   name-label
    #   name_description
    #   is_a_snapshot
    #   snapshot_of, if snapshot status is true
    #   snapshot time
    #   type (system, user or metadata etc)
    #   vdi_type: raw or vhd
    #   read_only
    #   location
    #   managed
    #   metadata_of_pool
    def updateMetadata(self, update_map={}):
        util.SMlog("Updating metadata : %s" % update_map)

        try:
            objtype = update_map[METADATA_UPDATE_OBJECT_TYPE_TAG]
            del update_map[METADATA_UPDATE_OBJECT_TYPE_TAG]

            if objtype == METADATA_OBJECT_TYPE_SR:
                self.updateSR(update_map)
            elif objtype == METADATA_OBJECT_TYPE_VDI:
                self.updateVdi(update_map)
        except Exception as e:
            util.SMlog('Error updating Metadata Volume with update' \
                         'map: %s. Error: %s' % (update_map, str(e)))
            raise xs_errors.XenError('MetadataError', \
                         opterr='%s' % str(e))

    def deleteVdiFromMetadata(self, vdi_uuid):
        util.SMlog("Deleting vdi: %s" % vdi_uuid)
        try:
            self.deleteVdi(vdi_uuid)
        except Exception as e:
            util.SMlog('Error deleting vdi %s from the metadata. ' \
                'Error: %s' % (vdi_uuid, str(e)))
            raise xs_errors.XenError('MetadataError', \
                opterr='%s' % str(e))

    def addVdi(self, vdi_info={}):
        util.SMlog("Adding VDI with info: %s" % vdi_info)
        try:
            self.addVdiInternal(vdi_info)
        except Exception as e:
            util.SMlog('Error adding VDI to Metadata Volume with ' \
                'update map: %s. Error: %s' % (vdi_info, str(e)))
            raise xs_errors.XenError('MetadataError', \
                opterr='%s' % (str(e)))

    def ensureSpaceIsAvailableForVdis(self, count):
        util.SMlog("Checking if there is space in the metadata for %d VDI." % \
                   count)
        try:
            self.spaceAvailableForVdis(count)
        except Exception as e:
            raise xs_errors.XenError('MetadataError', \
                opterr='%s' % str(e))

    # common functions
    def deleteVdi(self, vdi_uuid, offset=0):
        util.SMlog("Entering deleteVdi")
        try:
            md = self.getMetadataInternal({'vdi_uuid': vdi_uuid})
            if 'offset' not in md:
                util.SMlog("Metadata for VDI %s not present, or already removed, " \
                    "no further deletion action required." % vdi_uuid)
                return

            md['vdi_info'][md['offset']][VDI_DELETED_TAG] = '1'
            self.updateVdi(md['vdi_info'][md['offset']])

            try:
                mdlength = getMetadataLength(self.fd)
                if (mdlength - md['offset']) == self.vdi_info_size:
                    updateLengthInHeader(self.fd,
                                         mdlength - self.vdi_info_size)
            except:
                raise
        except Exception as e:
            raise Exception("VDI delete operation failed for " \
                                "parameters: %s, %s. Error: %s" % \
                                (self.path, vdi_uuid, str(e)))

    # common functions with some details derived from the child class
    def generateVDIsForRange(self, vdi_info, lower, upper, update_map={}, \
                             offset=0):
        if not len(vdi_info.keys()) or offset not in vdi_info:
            return self.getVdiInfo(update_map)

        value = b""
        for vdi_offset in vdi_info.keys():
            if vdi_offset < lower:
                continue

            if len(value) >= (upper - lower):
                break

            vdi_map = vdi_info[vdi_offset]
            if vdi_offset == offset:
                # write passed in VDI info
                for key in update_map.keys():
                    vdi_map[key] = update_map[key]

            for i in range(1, self.VDI_INFO_SIZE_IN_SECTORS + 1):
                if len(value) < (upper - lower):
                    value += self.getVdiInfo(vdi_map, i)

        return value

    def addVdiInternal(self, Dict):
        util.SMlog("Entering addVdiInternal")
        try:
            Dict[VDI_DELETED_TAG] = '0'
            mdlength = getMetadataLength(self.fd)
            md = self.getMetadataInternal({'firstDeleted': 1, 'includeDeletedVdis': 1})
            if 'foundDeleted' not in md:
                md['offset'] = mdlength
                (md['lower'], md['upper']) = \
                    getBlockAlignedRange(mdlength, self.vdi_info_size)
            # If this has created a new VDI, update metadata length
            if 'foundDeleted' in md:
                value = self.getMetadataToWrite(md['sr_info'], md['vdi_info'], \
                        md['lower'], md['upper'], Dict, md['offset'])
            else:
                value = self.getMetadataToWrite(md['sr_info'], md['vdi_info'], \
                        md['lower'], md['upper'], Dict, mdlength)

            file_write_wrapper(self.fd, md['lower'], value)

            if 'foundDeleted' in md:
                updateLengthInHeader(self.fd, mdlength)
            else:
                updateLengthInHeader(self.fd, mdlength + self.vdi_info_size)
            return True
        except Exception as e:
            util.SMlog("Exception adding vdi with info: %s. Error: %s" % \
                       (Dict, str(e)))
            raise

    # Get metadata from the file name passed in
    # additional params:
    # includeDeletedVdis - include deleted VDIs in the returned metadata
    # vdi_uuid - only fetch metadata till a particular VDI
    # offset - only fetch metadata till a particular offset
    # firstDeleted - get the first deleted VDI
    # indexByUuid - index VDIs by uuid
    # the return value of this function is a dictionary having the following keys
    # sr_info: dictionary containing sr information
    # vdi_info: dictionary containing vdi information indexed by offset
    # offset: when passing in vdi_uuid/firstDeleted below
    # deleted - true if deleted VDI found to be replaced
    def getMetadataInternal(self, params={}):
        try:
            lower = 0
            upper = 0
            retmap = {}
            sr_info_map = {}
            ret_vdi_info = {}
            length = getMetadataLength(self.fd)

            # Read in the metadata fil
            metadataxml = file_read_wrapper(self.fd, 0, length)

            # At this point we have the complete metadata in metadataxml
            offset = SECTOR_SIZE + len(XML_HEADER)
            sr_info = metadataxml[offset: SECTOR_SIZE * 4]
            offset = SECTOR_SIZE * 4
            sr_info = sr_info.replace(b'\x00', b'')

            parsable_metadata = buildParsableMetadataXML(sr_info)
            retmap['sr_info'] = metadata._parseXML(parsable_metadata)

            # At this point we check if an offset has been passed in
            if 'offset' in params:
                upper = getBlockAlignedRange(params['offset'], 0)[1]
            else:
                upper = length

            # Now look at the VDI objects
            while offset < upper:
                vdi_info = metadataxml[offset:offset + self.vdi_info_size]
                vdi_info = vdi_info.replace(b'\x00', b'')
                parsable_metadata = buildParsableMetadataXML(vdi_info)
                vdi_info_map = metadata._parseXML(parsable_metadata)[VDI_TAG]
                vdi_info_map[OFFSET_TAG] = offset

                if 'includeDeletedVdis' not in params and \
                    vdi_info_map[VDI_DELETED_TAG] == '1':
                    offset += self.vdi_info_size
                    continue

                if 'indexByUuid' in params:
                    ret_vdi_info[vdi_info_map[UUID_TAG]] = vdi_info_map
                else:
                    ret_vdi_info[offset] = vdi_info_map

                if 'vdi_uuid' in params:
                    if vdi_info_map[UUID_TAG] == params['vdi_uuid']:
                        retmap['offset'] = offset
                        (lower, upper) = \
                            getBlockAlignedRange(offset, self.vdi_info_size)

                elif 'firstDeleted' in params:
                    if vdi_info_map[VDI_DELETED_TAG] == '1':
                        retmap['foundDeleted'] = 1
                        retmap['offset'] = offset
                        (lower, upper) = \
                            getBlockAlignedRange(offset, self.vdi_info_size)

                offset += self.vdi_info_size

            retmap['lower'] = lower
            retmap['upper'] = upper
            retmap['vdi_info'] = ret_vdi_info
            return retmap
        except Exception as e:
            util.SMlog("Exception getting metadata with params" \
                    "%s. Error: %s" % (params, str(e)))
            raise

    # This function expects both sr name_label and sr name_description to be
    # passed in
    def updateSR(self, Dict):
        util.SMlog('entering updateSR')

        value = b""

        # Find the offset depending on what we are updating
        diff = set(Dict.keys()) - set(ATOMIC_UPDATE_PARAMS_AND_OFFSET.keys())
        if diff == set([]):
            offset = SECTOR_SIZE * 2
            (lower, upper) = getBlockAlignedRange(offset, SECTOR_SIZE * 2)
            md = self.getMetadataInternal({'offset': \
                                SECTOR_SIZE * (SR_INFO_SIZE_IN_SECTORS - 1)})

            sr_info = md['sr_info']
            vdi_info_by_offset = md['vdi_info']

            # update SR info with Dict
            for key in Dict.keys():
                sr_info[key] = Dict[key]

            # if lower is less than SR header size
            if lower < SR_INFO_SIZE_IN_SECTORS * SECTOR_SIZE:
                # if upper is less than SR header size
                if upper <= SR_INFO_SIZE_IN_SECTORS * SECTOR_SIZE:
                    for i in range(lower // SECTOR_SIZE, upper // SECTOR_SIZE):
                        value += self.getSRInfoForSectors(sr_info, range(i, i + 1))
                else:
                    for i in range(lower // SECTOR_SIZE, SR_INFO_SIZE_IN_SECTORS):
                        value += self.getSRInfoForSectors(sr_info, range(i, i + 1))

                    # generate the remaining VDI
                    value += self.generateVDIsForRange(vdi_info_by_offset,
                                SR_INFO_SIZE_IN_SECTORS, upper)
            else:
                # generate the remaining VDI
                value += self.generateVDIsForRange(vdi_info_by_offset, lower, upper)

            file_write_wrapper(self.fd, lower, value)
        else:
            raise Exception("SR Update operation not supported for "
                            "parameters: %s" % diff)

    def updateVdi(self, Dict):
        util.SMlog('entering updateVdi')
        try:
            mdlength = getMetadataLength(self.fd)
            md = self.getMetadataInternal({'vdi_uuid': Dict[UUID_TAG]})
            value = self.getMetadataToWrite(md['sr_info'], md['vdi_info'], \
                        md['lower'], md['upper'], Dict, md['offset'])
            file_write_wrapper(self.fd, md['lower'], value)
            return True
        except Exception as e:
            util.SMlog("Exception updating vdi with info: %s. Error: %s" % \
                       (Dict, str(e)))
            raise

    # This should be called only in the cases where we are initially writing
    # metadata, the function would expect a dictionary which had all information
    # about the SRs and all its VDIs
    def writeMetadataInternal(self, sr_info, vdi_info):
        try:
            md = self.getSRInfoForSectors(sr_info, range(0, SR_INFO_SIZE_IN_SECTORS))

            # Go over the VDIs passed and for each
            for key in vdi_info.keys():
                md += self.getVdiInfo(vdi_info[key])

            # Now write the metadata on disk.
            file_write_wrapper(self.fd, 0, md)
            updateLengthInHeader(self.fd, len(md))

        except Exception as e:
            util.SMlog("Exception writing metadata with info: %s, %s. " \
                       "Error: %s" % (sr_info, vdi_info, str(e)))
            raise

    # generates metadata info to write taking the following parameters:
    # a range, lower - upper
    # sr and vdi information
    # VDI information to update
    # an optional offset to the VDI to update
    def getMetadataToWrite(self, sr_info, vdi_info, lower, upper, update_map, \
                           offset):
        util.SMlog("Entering getMetadataToWrite")
        try:
            value = b""
            vdi_map = {}

            # if lower is less than SR info
            if lower < SECTOR_SIZE * SR_INFO_SIZE_IN_SECTORS:
                # generate SR info
                for i in range(lower // SECTOR_SIZE, SR_INFO_SIZE_IN_SECTORS):
                    value += self.getSRInfoForSectors(sr_info, range(i, i + 1))

                # generate the rest of the VDIs till upper
                value += self.generateVDIsForRange(vdi_info, \
                   SECTOR_SIZE * SR_INFO_SIZE_IN_SECTORS, upper, update_map, offset)
            else:
                # skip till you get a VDI with lower as the offset, then generate
                value += self.generateVDIsForRange(vdi_info, lower, upper, \
                                              update_map, offset)
            return value
        except Exception as e:
            util.SMlog("Exception generating metadata to write with info: " \
                       "sr_info: %s, vdi_info: %s, lower: %d, upper: %d, " \
                       "update_map: %s, offset: %d. Error: %s" % \
                       (sr_info, vdi_info, lower, upper, update_map, offset, str(e)))
            raise

    # specific functions, to be implement by the child classes
    def getVdiInfo(self, Dict, generateSector=0):
        return b""

    def getSRInfoForSectors(self, sr_info, range):
        return b""


class LVMMetadataHandler(MetadataHandler):

    VDI_INFO_SIZE_IN_SECTORS = 2

    # constructor
    def __init__(self, path=None, write=True):
        lvutil.ensurePathExists(path)
        MetadataHandler.__init__(self, path, write)

    def spaceAvailableForVdis(self, count):
        created = False
        try:
            # The easiest way to do this, is to create a dummy vdi and write it
            uuid = util.gen_uuid()
            vdi_info = {UUID_TAG: uuid,
                        NAME_LABEL_TAG: 'dummy vdi for space check',
                        NAME_DESCRIPTION_TAG: 'dummy vdi for space check',
                        IS_A_SNAPSHOT_TAG: 0,
                        SNAPSHOT_OF_TAG: '',
                        SNAPSHOT_TIME_TAG: '',
                        TYPE_TAG: 'user',
                        VDI_TYPE_TAG: 'vhd',
                        READ_ONLY_TAG: 0,
                        MANAGED_TAG: 0,
                        'metadata_of_pool': ''
            }

            created = self.addVdiInternal(vdi_info)
        except IOError as e:
            raise
        finally:
            if created:
                # Now delete the dummy VDI created above
                self.deleteVdi(uuid)
                return

    # This function generates VDI info based on the passed in information
    # it also takes in a parameter to determine whether both the sector
    # or only one sector needs to be generated, and which one
    # generateSector - can be 1 or 2, defaults to 0 and generates both sectors
    def getVdiInfo(self, Dict, generateSector=0):
        util.SMlog("Entering VDI info")
        try:
            vdi_info = b""
            # HP split into 2 functions, 1 for generating the first 2 sectors,
            # which will be called by all classes
            # and one specific to this class
            if generateSector == 1 or generateSector == 0:
                label = xml.sax.saxutils.escape(Dict[NAME_LABEL_TAG])
                desc = xml.sax.saxutils.escape(Dict[NAME_DESCRIPTION_TAG])
                label_length = len(to_utf8(label))
                desc_length = len(to_utf8(desc))

                if label_length + desc_length > MAX_VDI_NAME_LABEL_DESC_LENGTH:
                    limit = MAX_VDI_NAME_LABEL_DESC_LENGTH // 2
                    if label_length > limit:
                        label = label[:util.unictrunc(label, limit)]
                        util.SMlog('warning: name-label truncated from '
                                   '%d to %d bytes'
                                   % (label_length, len(to_utf8(label))))

                    if desc_length > limit:
                        desc = desc[:util.unictrunc(desc, limit)]
                        util.SMlog('warning: description truncated from '
                                   '%d to %d bytes'
                                   % (desc_length, len(to_utf8(desc))))

                Dict[NAME_LABEL_TAG] = label
                Dict[NAME_DESCRIPTION_TAG] = desc

                # Fill the open struct and write it
                vdi_info += getSector(openingTag(VDI_TAG)
                                      + buildXMLElement(NAME_LABEL_TAG, Dict)
                                      + buildXMLElement(NAME_DESCRIPTION_TAG,
                                                        Dict))

            if generateSector == 2 or generateSector == 0:
                sector2 = b""

                if VDI_DELETED_TAG not in Dict:
                    Dict.update({VDI_DELETED_TAG: '0'})

                for tag in Dict.keys():
                    if tag == NAME_LABEL_TAG or tag == NAME_DESCRIPTION_TAG:
                        continue
                    sector2 += buildXMLElement(tag, Dict)

                sector2 += closingTag(VDI_TAG)
                vdi_info += getSector(sector2)
            return vdi_info

        except Exception as e:
            util.SMlog("Exception generating vdi info: %s. Error: %s" % \
                       (Dict, str(e)))
            raise

    def getSRInfoForSectors(self, sr_info, range):
        srinfo = b""

        try:
            # write header, name_labael and description in that function
            # as its common to all
            # Fill up the first sector
            if 0 in range:
                srinfo = getSector(buildHeader(SECTOR_SIZE))

            if 1 in range:
                srinfo += getSector(XML_HEADER
                                    + buildXMLElement(UUID_TAG, sr_info)
                                    + buildXMLElement(ALLOCATION_TAG, sr_info))

            if 2 in range:
                # Fill up the SR name_label
                srinfo += buildXMLSector(NAME_LABEL_TAG,
                    xml.sax.saxutils.escape(sr_info[NAME_LABEL_TAG]))

            if 3 in range:
                # Fill the name_description
                srinfo += buildXMLSector(NAME_DESCRIPTION_TAG,
                    xml.sax.saxutils.escape(sr_info[NAME_DESCRIPTION_TAG]))

            return srinfo

        except Exception as e:
            util.SMlog("Exception getting SR info with parameters: sr_info: %s," \
                       "range: %s. Error: %s" % (sr_info, range, str(e)))
            raise
