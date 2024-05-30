#!/usr/bin/python3
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
# ISOSR: remote iso storage repository

import SR
import VDI
import SRCommand
import util
import nfs
import os
import re
import xs_errors
import cifutils

CAPABILITIES = ["VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH",
                "SR_SCAN", "SR_ATTACH", "SR_DETACH"]

CONFIGURATION = [
    ['location', 'path to mount (required) (e.g. server:/path)'],
    ['options',
     'extra options to pass to mount (e.g. \'-o ro\')'],
    ['type', 'cifs (SMB) or nfs_iso'],
    nfs.NFS_VERSION,
    ['vers', 'SMB version, default version 3'],
    ['username', r'Username to authenticate to SMB share with, can be domain\username'],
    ['cifspassword_secret', 'Secret ID containing the password to authenticate to SMB'],
    ['cifspassword', 'Password to authenticate to SMB, (deprecated see cifspassword_secret)']
]

DRIVER_INFO = {
    'name': 'ISO',
    'description': 'Handles CD images stored as files in iso format',
    'vendor': 'Citrix Systems Inc',
    'copyright': '(C) 2008 Citrix Systems Inc',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
    }

TYPE = "iso"
SMB_VERSION_1 = '1.0'
SMB_VERSION_3 = '3.0'
NFSPORT = 2049


def list_images(path):
    """
    Finds the iso and img files in a given directory that have valid unicode
    names. Returns a list of these, together with a count of number of image
    files that had to be ignored due to encoding issues in their names.
    """
    # pylint: disable=no-member

    regex = re.compile(r"\.iso$|\.img$", re.I)
    images = []
    num_images_ignored = 0

    for filename in os.listdir(path):
        if not regex.search(filename):
            # Not an image file
            pass
        elif os.path.isdir(os.path.join(path, filename)):
            util.SMlog("list_images: '%s' is a directory. Ignore"
                       % loggable_filename(filename))
        else:
            try:
                if is_consistent_utf8_filename(filename):
                    images.append(filename)
                else:
                    num_images_ignored += 1
                    util.SMlog("WARNING: ignoring image file '%s' due to"
                               " encoding issues"
                               % loggable_filename(filename))
            except UnicodeDecodeError as e:
                num_images_ignored += 1
                util.SMlog("WARNING: ignoring image file '%s' as its name is"
                           " not UTF-8 compatible"
                           % loggable_filename(filename))

    return images, num_images_ignored


def loggable_filename(filename):
    # Strip the 'b"' and '"' off the string representation of bytes
    return str(os.fsencode(filename))[2:-1]


def is_consistent_utf8_filename(filename):
    """
    Determines whether a filename, which is assumed to come from a filesystem
    where names are UTF-8 encoded, is consistent in the sense that its name in
    the form we'd like to use it (that is, as valid unicode) is the same as
    the form it needs to take when passed to Python library functions (e.g.
    open, os.stat).

    Raises UnicodeDecodeError if the name on the filesystem isn't UTF-8
    encoded.
    """
    # We generally expect that names of files in the mounted file system will
    # be utf-8 encoded. That need not always be true - for example, mount.cifs
    # provides an "iocharset" option to control this. But we make no attempt
    # to cope with, say, latin-1, and so such a file name will cause an
    # exception.
    #
    # Even if a file's name is utf-8 encoded, we might still have to reject it
    # for being "inconsistent". That's because Python's filesystem encoding
    # (see `sys.getfilesystemencoding`) might be ascii rather than utf-8, in
    # which case non-ascii characters in file names will show up as "surrogate
    # escapes" - which makes them technically not valid as unicode.
    #
    # Although it would be easy enough to recover the originally intended name
    # for such a file, it would be awkward elsewhere in the code base either
    # to have invalid unicode in file paths, or to have file paths that needed
    # massaging before they could be used for actual file operations. Hence
    # we say the name is inconsistent.
    #
    # From Python 3.7 onwards it looks like it should be much more likely that
    # the filesystem encoding will be utf-8, which will hopefully mean that we
    # would then get previously rejected image files showing up, and working
    # without further code changes being necessary.

    filename_bytes = os.fsencode(filename)
    return filename == filename_bytes.decode("utf-8")


def tools_iso_name(filename):
    # The tools ISO used have a "xs-" prefix in its name.
    # We recognise both and set the name_label accordingly.
    if filename[:3] == "xs-":
        return "xs-tools.iso"
    else:
        return "guest-tools.iso"


class ISOSR(SR.SR):
    """Local file storage repository"""

    # Some helper functions:
    def _checkmount(self):
        """Checks that the mountpoint exists and is mounted"""
        if not util.pathexists(self.mountpoint):
            return False
        try:
            ismount = util.ismount(self.mountpoint)
        except util.CommandException as inst:
            return False
        return ismount

    def _checkTargetStr(self, location):
        if 'type' not in self.dconf:
            return
        if self.dconf['type'] == 'cifs':
            tgt = ''
            if re.search('^//', location):
                tgt = location.split('/')[2]
            elif re.search(r'^\\', location):
                l = location.split('\\')
                for i in location.split('\\'):
                    if i:
                        tgt = i
                        break
            if not tgt:
                raise xs_errors.XenError('ISOLocationStringError')
        else:
            if location.find(':') == -1:
                raise xs_errors.XenError('ISOLocationStringError')
            tgt = location.split(':')[0]

        try:
            util._convertDNS(tgt)
        except:
            raise xs_errors.XenError('DNSError')

    # pylint: disable=no-member
    uuid_file_regex = re.compile(
        r"([0-9a-f]{8}-(([0-9a-f]{4})-){3}[0-9a-f]{12})\.(iso|img)", re.I)

    def _loadvdis(self):
        """Scan the directory and get uuids either from the VDI filename, \
        or by creating a new one."""
        if self.vdis:
            return

        image_names, _ = list_images(self.path)

        for name in image_names:
            self.vdis[name] = ISOVDI(self, name)
            # Set the VDI UUID if the filename is of the correct form.
            # Otherwise, one will be generated later in VDI._db_introduce.
            m = self.uuid_file_regex.match(name)
            if m:
                self.vdis[name].uuid = m.group(1)

        # Synchronise the read-only status with existing VDI records
        __xenapi_records = util.list_VDI_records_in_sr(self)
        __xenapi_locations = {}
        for vdi in __xenapi_records.keys():
            __xenapi_locations[__xenapi_records[vdi]['location']] = vdi
        for vdi in self.vdis.values():
            if vdi.location in __xenapi_locations:
                v = __xenapi_records[__xenapi_locations[vdi.location]]
                sm_config = v['sm_config']
                if 'created' in sm_config:
                    vdi.sm_config['created'] = sm_config['created']
                    vdi.read_only = False

# Now for the main functions:
    def handles(type):
        """Do we handle this type?"""
        if type == TYPE:
            return True
        return False
    handles = staticmethod(handles)

    def content_type(self, sr_uuid):
        """Returns the content_type XML"""
        return super(ISOSR, self).content_type(sr_uuid)

    # pylint: disable=no-member
    vdi_path_regex = re.compile(r"[a-z0-9.-]+\.(iso|img)", re.I)

    def vdi(self, uuid):
        """Create a VDI class.  If the VDI does not exist, we determine
        here what its filename should be."""

        filename = util.to_plain_string(self.srcmd.params.get('vdi_location'))
        if filename is None:
            smconfig = self.srcmd.params.get('vdi_sm_config')
            if smconfig is None:
                # uh, oh, a VDI.from_uuid()
                import XenAPI # pylint: disable=import-error
                _VDI = self.session.xenapi.VDI
                try:
                    vdi_ref = _VDI.get_by_uuid(uuid)
                except XenAPI.Failure as e:
                    if e.details[0] != 'UUID_INVALID':
                        raise
                else:
                    filename = _VDI.get_location(vdi_ref)

        if filename is None:
            # Get the filename from sm-config['path'], or use the UUID
            # if the path param doesn't exist.
            if smconfig and 'path' in smconfig:
                filename = smconfig['path']
                if not self.vdi_path_regex.match(filename):
                    raise xs_errors.XenError('VDICreate', \
                                                 opterr='Invalid path "%s"' % filename)
            else:
                filename = '%s.img' % uuid

        return ISOVDI(self, filename)

    def load(self, sr_uuid):
        """Initialises the SR"""
        # First of all, check we've got the correct keys in dconf
        if 'location' not in self.dconf:
            raise xs_errors.XenError('ConfigLocationMissing')

        # Construct the path we're going to mount under:
        if "legacy_mode" in self.dconf:
            self.mountpoint = util.to_plain_string(self.dconf['location'])
        else:
            # Verify the target address
            self._checkTargetStr(self.dconf['location'])
            self.mountpoint = os.path.join(SR.MOUNT_BASE, sr_uuid)

        # Add on the iso_path value if there is one
        if "iso_path" in self.dconf:
            iso_path = util.to_plain_string(self.dconf['iso_path'])
            if iso_path.startswith("/"):
                iso_path = iso_path[1:]
            self.path = os.path.join(self.mountpoint, iso_path)
        else:
            self.path = self.mountpoint

        # Handle optional dconf attributes
        self.nfsversion = nfs.validate_nfsversion(self.dconf.get('nfsversion'))

        # Fill the required SMB version
        self.smbversion = SMB_VERSION_3

        # Check if smb version is specified from client
        self.is_smbversion_specified = False

        # Some info we need:
        self.sr_vditype = 'phy'

    def delete(self, sr_uuid):
        pass

    def attach(self, sr_uuid):
        """Std. attach"""
        # Very-Legacy mode means the ISOs are in the local fs - so no need to attach.
        if 'legacy_mode' in self.dconf:
            # Verify path exists
            if not os.path.exists(self.mountpoint):
                raise xs_errors.XenError('ISOLocalPath')
            return

        # Check whether we're already mounted
        if self._checkmount():
            return

        location = util.to_plain_string(self.dconf['location'])
        # TODO: Have XC standardise iso type string
        if 'type' in self.dconf:
            protocol = self.dconf['type']
        elif ":/" in location:
            protocol = 'nfs_iso'
        else:
            protocol = 'cifs'

        if protocol == 'nfs_iso':
            self._check_nfs_server(location)

        # Create the mountpoint if it's not already there
        if not util.isdir(self.mountpoint):
            util.makedirs(self.mountpoint)

        mountcmd = []
        options = ''

        if 'options' in self.dconf:
            options = self.dconf['options'].split(' ')
            if protocol == 'cifs':
                options = [x for x in options if x != ""]
            else:
                options = self.getNFSOptions(options)

        # SMB options are passed differently for create via
        # XC/xe sr-create and create via xe-mount-iso-sr
        # In both cases check if SMB version is passed are not.
        # If not use self.smbversion.
        if protocol == 'cifs':
            if 'type' in self.dconf:
                # Create via XC or sr-create
                # Check for username and password
                mountcmd = ["mount.cifs", location, self.mountpoint]
                if 'vers' in self.dconf:
                    self.is_smbversion_specified = True
                    self.smbversion = self.dconf['vers']
                    util.SMlog("self.dconf['vers'] = %s" % self.dconf['vers'])
                self.appendCIFSMountOptions(mountcmd)
            else:
                # Creation via xe-mount-iso-sr
                try:
                    mountcmd = ["mount", location, self.mountpoint]
                    if options and options[0] == '-o':
                        pos = options[1].find('vers=')
                        if pos == -1:
                            options[1] += ',' + self.getSMBVersion()
                        else:
                            self.smbversion = self.getSMBVersionFromOptions(
                                options[1])
                            self.is_smbversion_specified = True
                    else:
                        raise ValueError
                    mountcmd.extend(options)
                except ValueError:
                    raise xs_errors.XenError('ISOInvalidXeMountOptions')
            # Check the validity of 'smbversion'.
            # Raise an exception for any invalid version.
            if self.smbversion not in [SMB_VERSION_1, SMB_VERSION_3]:
                raise xs_errors.XenError('ISOInvalidSMBversion')

        # Attempt mounting
        smb3_fail_reason = None
        try:
            if protocol == 'nfs_iso':
                # For NFS, do a soft mount with tcp as protocol. Since ISO SR is
                # going to be r-only, a failure in nfs link can be reported back
                # to the process waiting.
                server, path, transport = self._parse_nfs_location(location)
                # Extract timeout and retrans values, if any
                io_timeout = nfs.get_nfs_timeout(self.other_config)
                io_retrans = nfs.get_nfs_retrans(self.other_config)
                nfs.soft_mount(self.mountpoint, server, path,
                               transport, useroptions=options, nfsversion=self.nfsversion,
                               timeout=io_timeout, retrans=io_retrans)
            else:
                if self.smbversion in SMB_VERSION_3:
                    util.SMlog('ISOSR mount over smb 3.0')
                    try:
                        self.mountOverSMB(mountcmd)
                    except util.CommandException as inst:
                        if not self.is_smbversion_specified:
                            util.SMlog('Retrying ISOSR mount over smb 1.0')
                            smb3_fail_reason = inst.reason
                            # mountcmd is constructed such that the last two
                            # items will contain -o argument and its value.
                            del mountcmd[-2:]
                            self.smbversion = SMB_VERSION_1
                            if not options:
                                self.appendCIFSMountOptions(mountcmd)
                            else:
                                if options[0] == '-o':
                                    # regex can be used here since we have
                                    # already validated version entry
                                    options[1] = re.sub('vers=3.0', 'vers=1.0',
                                                        options[1])
                                mountcmd.extend(options)
                            self.mountOverSMB(mountcmd)
                        else:
                            raise xs_errors.XenError(
                                'ISOMountFailure', opterr=inst.reason)
                else:
                    util.SMlog('ISOSR mount over smb 1.0')
                    self.mountOverSMB(mountcmd)
        except util.CommandException as inst:
            if not self.is_smbversion_specified:
                raise xs_errors.XenError(
                    'ISOMountFailure', opterr=smb3_fail_reason)
            else:
                raise xs_errors.XenError(
                    'ISOMountFailure', opterr=inst.reason)
        except nfs.NfsException as e:
            raise xs_errors.XenError('ISOMountFailure', opterr=str(e.errstr))

        # Check the iso_path is accessible
        if not self._checkmount():
            self.detach(sr_uuid)
            raise xs_errors.XenError('ISOSharenameFailure')

    def _parse_nfs_location(self, location):
        """
        Given the location of an NFS share, parse it to give
        a tuple of the remove server, remote path, and transport protocol to
        use.
        """
        serv_path = []
        transport = 'tcp'
        if location.startswith('['):
            # IPv6 target: remove brackets around the IPv6
            transport = 'tcp6'
            ip6 = location[1:location.index(']')]
            path = location[location.index(']') + 2:]
            serv_path = [ip6, path]
        else:
            serv_path = location.split(':')

        return serv_path[0], serv_path[1], transport

    def _check_nfs_server(self, location):
        """
        Given that we want to mount a given NFS share, checks that there is an
        NFS server running in the remote server, and that it supports the
        desired NFS version. Raises an appropriate exception if this is not
        the case.
        """
        server, _, transport = self._parse_nfs_location(location)

        try:
            util._testHost(server, NFSPORT, 'NFSTarget')
            if not nfs.check_server_tcp(server, transport, self.nfsversion):
                raise xs_errors.XenError('NFSVersion',
                                         opterr="Unsupported NFS version: %s" % self.nfsversion)
        except nfs.NfsException as e:
            raise xs_errors.XenError('NFSTarget', opterr=str(e.errstr))


    def after_master_attach(self, uuid):
        """Perform actions required after attaching on the pool master
        Return:
          None
        """
        # Nothing required here for ISOs and tools ISOs will fail if scanned
        pass

    def getSMBVersionFromOptions(self, options):
        """Extract SMB version from options """
        smb_ver = None
        options_list = options.split(',')
        for option in options_list:
            if option.startswith('vers='):
                version = option.split('=')
                if len(version) == 2:
                    smb_ver = version[1]
                break
        return smb_ver

    def getSMBVersion(self):
        """Pass smb version option to mount.cifs"""
        smbversion = "vers=%s" % self.smbversion
        return smbversion

    def mountOverSMB(self, mountcmd):
        """This function raises util.CommandException"""
        new_env, domain = cifutils.getCIFCredentials(self.dconf, self.session,
                                                     prefix="cifs")

        util.pread(mountcmd, True, new_env=new_env)
        try:
            if not self.is_smbversion_specified:
                # Store the successful smb version in PBD config
                self.updateSMBVersInPBDConfig()
        except Exception as exc:
            util.SMlog("Exception: %s" % str(exc))
            if self._checkmount():
                util.pread(["umount", self.mountpoint])
            raise xs_errors.XenError('SMBMount') from exc

    def updateSMBVersInPBDConfig(self):
        """Store smb version in PBD config"""
        pbd = util.find_my_pbd(self.session, self.host_ref, self.sr_ref)
        if pbd is not None:
            util.SMlog('Updating SMB version in PBD device config')
            dconf = self.session.xenapi.PBD.get_device_config(pbd)
            dconf['vers'] = self.smbversion
            self.session.xenapi.PBD.set_device_config(pbd, dconf)
        else:
            raise Exception('Could not find PBD for corresponding SR')

    def getNFSOptions(self, options):
        """Append options to mount.nfs"""
        #Only return any options specified with -o
        nfsOptions = ''
        for index, opt in enumerate(options):
            if opt == "-o":
                nfsOptions = options[index + 1]
                break

        return nfsOptions

    def appendCIFSMountOptions(self, mountcmd):
        """Append options to mount.cifs"""
        options = []
        try:
            options.append(self.getCacheOptions())

            if not cifutils.containsCredentials(self.dconf, prefix="cifs"):
                options.append('guest')
                domain = None
            else:
                _, domain = cifutils.splitDomainAndUsername(self.dconf['username'])

            options.append(self.getSMBVersion())

            if domain:
                options.append('domain=' + domain)
        except:
            util.SMlog("Exception while attempting to append mount options")
            raise

        # Extend mountcmd appropriately
        if options:
            options = ",".join(str(x) for x in options if x)
            mountcmd.extend(["-o", options])

    def getCacheOptions(self):
        """Pass cache options to mount.cifs"""
        return "cache=none"

    def detach(self, sr_uuid):
        """Std. detach"""
        if 'legacy_mode' in self.dconf or not self._checkmount():
            return

        try:
            util.pread(["umount", self.mountpoint])
        except util.CommandException as inst:
            raise xs_errors.XenError('NFSUnMount', \
                                         opterr='error is %d' % inst.code)

    def scan(self, sr_uuid):
        """Scan: see _loadvdis"""
        if not util.isdir(self.path):
            raise xs_errors.XenError('SRUnavailable', \
                    opterr='no such directory %s' % self.path)

        if ('legacy_mode' not in self.dconf) and (not self._checkmount()):
            raise xs_errors.XenError('SRUnavailable', \
                    opterr='directory not mounted: %s' % self.path)

        #try:
        if not self.vdis:
            self._loadvdis()
        self.physical_size = util.get_fs_size(self.path)
        self.physical_utilisation = util.get_fs_utilisation(self.path)
        self.virtual_allocation = self.physical_size

        self.other_config = self.session.xenapi.SR.get_other_config(self.sr_ref)

        if 'xenserver_tools_sr' in self.other_config and \
                self.other_config['xenserver_tools_sr'] == "true":
            # Out of all the xs-tools ISOs which exist in this dom0, we mark
            # only one as the official one.

            # Pass 1: find the latest version
            latest_build_vdi = None
            latest_build_number = "0"
            for vdi_name in self.vdis:
                vdi = self.vdis[vdi_name]

                if latest_build_vdi is None:
                    latest_build_vdi = vdi.location
                    latest_build_number = "0"

                if 'xs-tools-build' in vdi.sm_config:
                    bld = vdi.sm_config['xs-tools-build']
                    if bld >= latest_build_number:
                        latest_build_vdi = vdi.location
                        latest_build_number = bld

            # Pass 2: mark all VDIs accordingly
            for vdi_name in self.vdis:
                vdi = self.vdis[vdi_name]
                if vdi.location == latest_build_vdi:
                    vdi.sm_config['xs-tools'] = "true"
                else:
                    if "xs-tools" in vdi.sm_config:
                        del vdi.sm_config['xs-tools']

            # Synchronise the VDIs: this will update the sm_config maps of current records
            scanrecord = SR.ScanRecord(self)
            scanrecord.synchronise_new()
            scanrecord.synchronise_existing()

            # Everything that looks like an xs-tools ISO but which isn't the
            # primary one will also be renamed "Old version of ..."
            sr = self.session.xenapi.SR.get_by_uuid(sr_uuid)
            all_vdis = self.session.xenapi.VDI.get_all_records_where("field \"SR\" = \"%s\"" % sr)
            for vdi_ref in all_vdis.keys():
                vdi = all_vdis[vdi_ref]
                if 'xs-tools-version' in vdi['sm_config']:
                    name = tools_iso_name(vdi['location'])
                    if 'xs-tools' in vdi['sm_config']:
                        self.session.xenapi.VDI.set_name_label(vdi_ref, name)
                    else:
                        self.session.xenapi.VDI.set_name_label(vdi_ref, "Old version of " + name)


            # never forget old VDI records to cope with rolling upgrade
            for location in scanrecord.gone:
                vdi = scanrecord.get_xenapi_vdi(location)
                util.SMlog("Marking previous version of tools ISO: location=%s uuid=%s" % (vdi['location'], vdi['uuid']))
                vdi = self.session.xenapi.VDI.get_by_uuid(vdi['uuid'])
                name_label = self.session.xenapi.VDI.get_name_label(vdi)
                if not(name_label.startswith("Old version of ")):
                    self.session.xenapi.VDI.set_name_label(vdi, "Old version of " + name_label)
                # Mark it as missing for informational purposes only
                self.session.xenapi.VDI.set_missing(vdi, True)
                self.session.xenapi.VDI.remove_from_sm_config(vdi, 'xs-tools')

        else:
            return super(ISOSR, self).scan(sr_uuid)

    def create(self, sr_uuid, size):
        self.attach(sr_uuid)
        if 'type' in self.dconf:
            smconfig = self.session.xenapi.SR.get_sm_config(self.sr_ref)
            smconfig['iso_type'] = self.dconf['type']
            self.session.xenapi.SR.set_sm_config(self.sr_ref, smconfig)

        _, num_images_ignored = list_images(self.path)
        if num_images_ignored > 0:
            xapi = self.session.xenapi
            xapi.message.create("DISK_IMAGES_IGNORED",
                                "4",
                                "SR",
                                self.uuid,
                                "Ignored disk image file(s) due to"
                                " file name encoding issues")

        self.detach(sr_uuid)

        
class ISOVDI(VDI.VDI):
    def load(self, vdi_uuid):
        # Nb, in the vdi_create call, the filename is unset, so the following
        # will fail.
        self.vdi_type = "iso"
        try:
            stat = os.stat(self.path)
            self.utilisation = int(stat.st_size)
            self.size = int(stat.st_size)
            self.label = self.filename
        except:
            pass

    def __init__(self, mysr, filename):
        self.path = os.path.join(mysr.path, filename)
        VDI.VDI.__init__(self, mysr, None)
        self.location = filename
        self.filename = filename
        self.read_only = True
        self.label = filename
        self.sm_config = {}
        if "legacy_mode" in mysr.dconf:
            if filename.startswith("xs-tools") or filename.startswith("guest-tools"):
                self.label = tools_iso_name(filename)
                # Mark this as a Tools CD
                # self.sm_config['xs-tools'] = 'true'
                # Extract a version string, if present
                vsn = filename[filename.find("tools") + len("tools"):][:-len(".iso")].strip("-").split("-", 1)
                # "4.1.0"
                if len(vsn) == 1:
                    build_number = "0"  # string
                    product_version = vsn[0]
                # "4.1.0-1234"
                elif len(vsn) > 1:
                    build_number = vsn[1]
                    product_version = vsn[0]
                else:
                    build_number = 0
                    product_version = "unknown"
                util.SMlog("version=%s build=%s" % (product_version, build_number))
                self.sm_config['xs-tools-version'] = product_version
                self.sm_config['xs-tools-build'] = build_number

    def detach(self, sr_uuid, vdi_uuid):
        pass

    def attach(self, sr_uuid, vdi_uuid):
        try:
            os.stat(self.path)
            return super(ISOVDI, self).attach(sr_uuid, vdi_uuid)
        except:
            raise xs_errors.XenError('VDIMissing')

    def create(self, sr_uuid, vdi_uuid, size):
        self.uuid = vdi_uuid
        self.path = os.path.join(self.sr.path, self.filename)
        self.size = size
        self.utilisation = 0
        self.read_only = False
        self.sm_config = self.sr.srcmd.params['vdi_sm_config']
        self.sm_config['created'] = util._getDateString()

        if util.pathexists(self.path):
            raise xs_errors.XenError('VDIExists')

        try:
            handle = open(self.path, "w")
            handle.truncate(size)
            handle.close()
            self._db_introduce()
            return super(ISOVDI, self).get_params()
        except Exception as exn:
            util.SMlog("Exception when creating VDI: %s" % exn)
            raise xs_errors.XenError('VDICreate', \
                     opterr='could not create file: "%s"' % self.path)

    def delete(self, sr_uuid, vdi_uuid):
        util.SMlog("Deleting...")

        self.uuid = vdi_uuid
        self._db_forget()

        if not util.pathexists(self.path):
            return

        try:
            util.SMlog("Unlinking...")
            os.unlink(self.path)
            util.SMlog("Done...")
        except:
            raise xs_errors.XenError('VDIDelete')

    # delete, update, introduce unimplemented. super class will raise
    # exceptions

if __name__ == '__main__':
    SRCommand.run(ISOSR, DRIVER_INFO)
else:
    SR.registerSR(ISOSR)
