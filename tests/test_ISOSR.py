import unittest.mock as mock
import nfs
import ISOSR
import unittest
import util
import SR
import errno
import os
import sys
import tempfile
import testlib
import xs_errors


class FakeISOSR(ISOSR.ISOSR):
    uuid = None
    sr_ref = None
    session = None
    srcmd = None
    other_config = {}
    host_ref = None

    def __init__(self, srcmd, none):
        self.dconf = srcmd.dconf
        self.srcmd = srcmd

class TestISOSR_overLocal(unittest.TestCase):
    def create_isosr(self, location='/local_sr', sr_uuid='asr_uuid'):
        srcmd = mock.Mock()
        srcmd.dconf = {
            'location': location,
            'type': 'iso',
            'legacy_mode': True
        }
        srcmd.params = {
            'command': 'some_command'
        }
        isosr = FakeISOSR(srcmd, None)
        isosr.load(sr_uuid)
        return isosr

    @mock.patch('util.pread')
    def test_load(self, pread):
        self.create_isosr()
        # Check `mount/umount` is never called.
        self.assertFalse(pread.called)

    @mock.patch('os.path.exists', autospec=True)
    @mock.patch('util.pread')
    def test_attach_and_detach_local(self, pread, exists):
        isosr = self.create_isosr()
        isosr.attach(None)
        self.assertFalse(pread.called)
        isosr.detach(None)
        self.assertFalse(pread.called)

    @mock.patch('os.path.exists', autospec=True)
    @mock.patch('util.pread')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_and_detach_local_with_mounted_path(
        self, _checkmount, pread, exists
    ):
        _checkmount.return_value = True

        isosr = self.create_isosr()
        isosr.attach(None)
        self.assertFalse(pread.called)
        isosr.detach(None)
        self.assertFalse(pread.called)

    @testlib.with_context
    @mock.patch('os.path.exists')
    @mock.patch('util.pread')
    def test_attach_local_with_bad_path(self, context, pread, exists):
        context.setup_error_codes()

        # Local path doesn't exist, but error list yes.
        exists.side_effect = [False, True]

        isosr = self.create_isosr()
        with self.assertRaises(SR.SROSError) as ose:
            isosr.attach(None)
        self.assertEquals(ose.exception.errno, 226)
        self.assertFalse(pread.called)


class TestISOSR_overNFS(unittest.TestCase):

    def create_isosr(self, location='aServer:/aLocation', atype=None,
                     sr_uuid='asr_uuid', nfsversion=None):
        srcmd = mock.Mock()
        srcmd.dconf = {
            'location': location
        }
        if atype:
            srcmd.dconf.update({'type': atype})
        if nfsversion:
            srcmd.dconf.update({'nfsversion': nfsversion})
        srcmd.params = {
            'command': 'some_command'
        }
        isosr = FakeISOSR(srcmd, None)
        isosr.load(sr_uuid)
        return isosr

    def test_load(self):
        self.create_isosr()

    @mock.patch('nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_called(self, validate_nfsversion):
        isosr = self.create_isosr(nfsversion='aNfsversion')

        validate_nfsversion.assert_called_once_with('aNfsversion')

    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_returnused(self, validate_nfsversion,
                                                 Lock):
        validate_nfsversion.return_value = 'aNfsversion'

        self.assertEqual(self.create_isosr().nfsversion, 'aNfsversion')

    @mock.patch('NFSSR.Lock', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    def test_load_validate_nfsversion_exceptionraised(self,
                                                      validate_nfsversion,
                                                      Lock):
        validate_nfsversion.side_effect = nfs.NfsException('aNfsException')

        self.assertRaises(nfs.NfsException, self.create_isosr)

    @mock.patch('util.gen_uuid', autospec=True)
    @mock.patch('nfs.soft_mount', autospec=True)
    @mock.patch('util._convertDNS', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    @mock.patch('util.makedirs', autospec=True)
    @mock.patch('util._testHost', autospec=True)
    @mock.patch('nfs.check_server_tcp', autospec=True)
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_nfs(self, _checkmount, check_server_tcp, testHost, makedirs,
                        validate_nfsversion, convertDNS, soft_mount, gen_uuid):
        validate_nfsversion.return_value = 'aNfsversionChanged'
        isosr = self.create_isosr(location='aServer:/aLocation', atype='nfs_iso',
                                  sr_uuid='asr_uuid')
        _checkmount.side_effect = [False, True]
        gen_uuid.return_value = 'aUuid'
        check_server_tcp.return_value = ['aNfsversionChanged']

        isosr.attach(None)

        testHost.assert_called_once_with('aServer', 2049, 'NFSTarget')

        check_server_tcp.assert_called_once_with('aServer',
                                                 'tcp',
                                                 'aNfsversionChanged')

        soft_mount.assert_called_once_with('/var/run/sr-mount/asr_uuid',
                                           'aServer',
                                           '/aLocation',
                                           'tcp',
                                           retrans=4,
                                           timeout=200,
                                           useroptions='',
                                           nfsversion='aNfsversionChanged')

    @mock.patch('util.gen_uuid', autospec=True)
    @mock.patch('nfs.soft_mount', autospec=True)
    @mock.patch('util._convertDNS', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    @mock.patch('util.makedirs', autospec=True)
    @mock.patch('util._testHost', autospec=True)
    @mock.patch('nfs.check_server_tcp', autospec=True)
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_nfs_ipv6(self, _checkmount, check_server_tcp, testHost, makedirs,
                        validate_nfsversion, convertDNS, soft_mount, gen_uuid):
        validate_nfsversion.return_value = 'aNfsversionChanged'
        isosr = self.create_isosr(location='[aServer]:/aLocation', atype='nfs_iso',
                                  sr_uuid='asr_uuid')
        _checkmount.side_effect = [False, True]
        gen_uuid.return_value = 'aUuid'
        check_server_tcp.return_value = ['aNfsversionChanged']

        isosr.attach(None)

        testHost.assert_called_once_with('aServer', 2049, 'NFSTarget')

        check_server_tcp.assert_called_once_with('aServer',
                                                 'tcp6',
                                                 'aNfsversionChanged')

        soft_mount.assert_called_once_with('/var/run/sr-mount/asr_uuid',
                                           'aServer',
                                           '/aLocation',
                                           'tcp6',
                                           retrans=4,
                                           timeout=200,
                                           useroptions='',
                                           nfsversion='aNfsversionChanged')

    @mock.patch('util.gen_uuid', autospec=True)
    @mock.patch('util._convertDNS', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    @mock.patch('util.makedirs', autospec=True)
    @mock.patch('util._testHost', autospec=True)
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_nfs_no_server(
            self, _checkmount, testHost, makedirs, validate_nfsversion,
            convertDNS, gen_uuid):

        isosr = self.create_isosr(location='aServer:/aLocation', atype='nfs_iso',
                                  sr_uuid='asr_uuid')

        _checkmount.side_effect = [False]
        testHost.side_effect = xs_errors.SROSError(
            140, 'Incorrect DNS name, unable to resolve.')

        with self.assertRaises(xs_errors.SROSError) as ose:
            isosr.attach(None)

        self.assertEqual(140, ose.exception.errno)

    @mock.patch('util.gen_uuid', autospec=True)
    @mock.patch('nfs.soft_mount', autospec=True)
    @mock.patch('util._convertDNS', autospec=True)
    @mock.patch('nfs.validate_nfsversion', autospec=True)
    @mock.patch('util.makedirs', autospec=True)
    @mock.patch('util._testHost', autospec=True)
    @mock.patch('nfs.check_server_tcp', autospec=True)
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_nfs_wrong_version(
            self, _checkmount, check_server_tcp, testHost, makedirs,
            validate_nfsversion, convertDNS, soft_mount, gen_uuid):
        isosr = self.create_isosr(location='aServer:/aLocation', atype='nfs_iso',
                                  sr_uuid='asr_uuid')

        _checkmount.return_value = False
        validate_nfsversion.return_value = '4'
        check_server_tcp.return_value = False

        with self.assertRaises(xs_errors.SROSError) as cm:
            isosr.attach(None)

        self.assertRegex(str(cm.exception),
                         r"^Required NFS server version unsupported\b")


class TestISOSR_overSMB(unittest.TestCase):

    def create_smbisosr(self, location='\\aServer\aLocation', atype=None,
                        sr_uuid='asr_uuid', server='\\aServer',
                        serverpath='/aServerpath',
                        vers=None, options='',
                        dconf_update={}):
        srcmd = mock.Mock()
        srcmd.dconf = {
            'location': location,
            'server': server,
            'serverpath': serverpath,
            'options': options
        }
        if vers:
            srcmd.dconf.update({'vers': vers})
        if atype:
            srcmd.dconf.update({'type': atype})
        srcmd.params = {
            'command': 'some_command',
            'device_config': {}
        }
        srcmd.dconf.update(dconf_update)
        isosr = FakeISOSR(srcmd, None)
        isosr.load(sr_uuid)
        return isosr

    def test_load(self):
        self.create_smbisosr()

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_with_smb_version_1(self, context, _checkmount, pread,
                                       _checkTargetStr, makedirs):
        """
        Positive case, over XC/XE CLI with version 1.0.
        """
        smbsr = self.create_smbisosr(atype='cifs', vers='1.0')
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)
        pread.return_value = "Success"
        pread.assert_called_with(['mount.cifs', '\\aServer\x07Location',
                                  '/var/run/sr-mount/asr_uuid', '-o',
                                  'cache=none,guest,vers=1.0'], True, new_env=None)

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_with_smb_credentials(self, context, _checkmount, pread,
                                         _checkTargetStr, makedirs):
        """
        Positive case, over XC/XE CLI with version 1.0.
        """
        update = {'username': 'dot', 'cifspassword': 'winter2019'}
        smbsr = self.create_smbisosr(atype='cifs', vers='1.0',
                                     dconf_update=update)
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)
        pread.assert_called_with(['mount.cifs', '\\aServer\x07Location',
                                  '/var/run/sr-mount/asr_uuid', '-o',
                                 'cache=none,vers=1.0'], True,
                                 new_env={'PASSWD': 'winter2019', 'USER': 'dot'})

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_with_smb_credentials_domain(self, context,
                                                _checkmount, pread,
                                                _checkTargetStr, makedirs):
        """
        Positive case, over XC/XE CLI with version 1.0.
        """
        update = {'username': r'citrix\jsmith', 'cifspassword': 'winter2019'}
        smbsr = self.create_smbisosr(atype='cifs', vers='1.0',
                                     dconf_update=update)
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)
        pread.assert_called_with(['mount.cifs', '\\aServer\x07Location',
                                  '/var/run/sr-mount/asr_uuid', '-o',
                                  'cache=none,vers=1.0,domain=citrix'], True,
                                 new_env={'PASSWD': 'winter2019', 'USER': 'jsmith'})

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_with_smb_version_3(self, context, _checkmount, pread,
                                       _checkTargetStr, makedirs):
        """
        Positive case, over XC/XE CLI with version 3.0.
        """
        smbsr = self.create_smbisosr(atype='cifs', vers='3.0')
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)
        pread.assert_called_with(['mount.cifs', '\\aServer\x07Location',
                                  '/var/run/sr-mount/asr_uuid', '-o',
                                  'cache=none,guest,vers=3.0'], True, new_env=None)

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    @mock.patch('ISOSR.ISOSR.updateSMBVersInPBDConfig')
    def test_attach_with_smb_no_version(self, context,
                                        updateSMBVersInPBDConfig,
                                        _checkmount, pread,
                                        _checkTargetStr, makedirs):
        """
        Positive case, over XC/XE CLI without version.
        """
        smbsr = self.create_smbisosr(atype='cifs')
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)
        pread.assert_called_with(['mount.cifs', '\\aServer\x07Location',
                                  '/var/run/sr-mount/asr_uuid', '-o',
                                  'cache=none,guest,vers=3.0'], True, new_env=None)

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('ISOSR.ISOSR._checkmount')
    @mock.patch('util.pread', autospec=True)
    def test_attach_smb_via_xemount_version_1(self, context, pread, _checkmount,
                                              _checkTargetStr, makedirs, gen_uuid):
        """
        Positive case, over xe-sr-mount CLI with version 1.0.
        """
        smbsr = self.create_smbisosr(options='-o username=administrator,password=password,vers=1.0')
        smbsr.attach(None)
        self.assertEqual(0, pread.call_count)

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('ISOSR.ISOSR._checkmount')
    @mock.patch('util.pread', autospec=True)
    def test_attach_smb_via_xemount_version_3(self, context, pread,
                                              _checkmount, _checkTargetStr,
                                              makedirs, gen_uuid):
        """
        Positive case, over xe-sr-mount CLI with version 3.0.
        """
        smbsr = self.create_smbisosr(options='-o username=administrator,password=password,vers=3.0')
        smbsr.attach(None)
        self.assertEqual(0, pread.call_count)

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('ISOSR.ISOSR._checkmount')
    @mock.patch('ISOSR.ISOSR.updateSMBVersInPBDConfig')
    @mock.patch('util.pread', autospec=True)
    def test_attach_smb_via_xemount_no_version(self, context, pread,
                                               updateSMBVersInPBDConfig,
                                               _checkmount,
                                               _checkTargetStr, makedirs,
                                               gen_uuid):
        """
        Positive case, without version from xe-sr-mount.
        """
        smbsr = self.create_smbisosr(options='-o username=administrator,password=password')
        smbsr.attach(None)
        self.assertEqual(0, pread.call_count)

    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    def test_attach_smb_wrongversion(self, pread, _checkTargetStr,
                                     makedirs, gen_uuid):
        """
        Unsupported version from XC/XE CLI.
        """
        smbsr = self.create_smbisosr(atype='cifs', vers='2.0')
        with self.assertRaises(xs_errors.SROSError) as context:
            smbsr.attach(None)
        self.assertEqual(context.exception.errno, 227)
        self.assertEqual(
            str(context.exception),
            'Given SMB version is not allowed. Choose either 1.0 or 3.0'
        )

    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    def test_attach_smb_wrongversion_via_xemount(self,
                                                 _checkTargetStr, makedirs,
                                                 gen_uuid):
        """
        Unsupported version from xe-sr-mount.
        """
        smbsr = self.create_smbisosr(options='-o vers=2.0')
        with self.assertRaises(xs_errors.SROSError) as context:
            smbsr.attach(None)
        self.assertEqual(context.exception.errno, 227)
        self.assertEqual(
            str(context.exception),
            'Given SMB version is not allowed. Choose either 1.0 or 3.0'
        )

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    @mock.patch('ISOSR.ISOSR.updateSMBVersInPBDConfig')
    def test_attach_smb_version_fallback_with_smb_3_disabled(self, context,
            updateSMBVersInPBDConfig, _checkmount, pread, _checkTargetStr, makedirs, gen_uuid):
        """
        Fall back scenario from XC/XE CLI with smb3 diabled and smb1 enabled.
        """
        smbsr = self.create_smbisosr(atype='cifs')
        pread.side_effect = iter([util.CommandException(errno.EHOSTDOWN), " "])
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)
        pread.assert_called_with(['mount.cifs', '\\aServer\x07Location',
                                  '/var/run/sr-mount/asr_uuid', '-o',
                                  'cache=none,guest,vers=1.0'], True, new_env=None)

    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_smb_version_fallback_with_smb_1_3_disabled(self,
                                                               _checkmount,
                                                               pread,
                                                               _checkTargetStr,
                                                               makedirs,
                                                               gen_uuid):
        """
        Fall back scenario from XC/XE CLI with smb3 diabled and smb1 disabled.
        """
        smbsr = self.create_smbisosr(atype='cifs')
        pread.side_effect = iter([util.CommandException(errno.EHOSTDOWN), \
                util.CommandException(errno.EHOSTDOWN), util.CommandException(errno.EHOSTDOWN)])
        _checkmount.side_effect = [False, True]
        with self.assertRaises(xs_errors.SROSError) as context:
            smbsr.attach(None)
        self.assertEqual(context.exception.errno, 222)
        self.assertEqual(
            str(context.exception),
            'Could not mount the directory specified in Device Configuration [opterr=exec failed]'
        )

    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_smb_via_xemount_no_version_fallback(self,
                                                        _checkmount, pread,
                                                        _checkTargetStr,
                                                        makedirs, gen_uuid):
        """
        Fall back scenario from xe-sr-mount with smb3 diabled and smb1 enabled.
        """
        smbsr = self.create_smbisosr(options='-o username=administrator,password=password')
        pread.side_effect = iter([util.CommandException(errno.EHOSTDOWN), " "])

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_smb_version_fallback_error(self, context, _checkmount,
                                               pread, _checkTargetStr,
                                               makedirs):
        """
        Fall back scenario negative case from xe-sr-mount with smb3 diabled and smb1 disabled.
        """
        smbsr = self.create_smbisosr(atype='cifs')
        pread.side_effect = iter([util.CommandException(errno.EHOSTDOWN),
                             util.CommandException(errno.EHOSTDOWN)])
        _checkmount.side_effect = [False, True]
        with self.assertRaises(Exception):
            smbsr.attach(None)

    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread', autospec=True)
    @mock.patch('util.find_my_pbd')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_mountoversmb_will_raise_on_error(
            self, _checkmount, find_my_pbd, pread, _checkTargetStr, makedirs):
        """
        Test failure to store SMB version inside PBD config will raise exception
        """
        smbsr = self.create_smbisosr(atype='cifs')
        find_my_pbd.return_value = None
        _checkmount.side_effect = [False, True]
        with self.assertRaises(xs_errors.SROSError) as exp:
            smbsr.attach(None)
        self.assertEqual(exp.exception.errno, 111)


class TestISOSR_functions(unittest.TestCase):
    def test_is_consistent_utf8_filename(self):
        with PatchFSEncoder() as fs_encoder:
            # Expectations:
            # - ascii filename should always show up as consistent (assuming the
            #   file system encoding is either ascii or utf-8, which we do)
            # - non-ascii utf-8 filenames are consistent when the file system
            #   encoding is utf-8
            # - non-utf-8 filenames should always cause an exception

            simple_ascii = b"nothing fancy"
            non_ascii_utf8 = b"snowman: \xe2\x98\x83"
            non_utf8 = b"\xablatin-1\xbb"

            cases = {
                (simple_ascii, "utf-8"): True,
                (simple_ascii, "ascii"): True,
                (non_ascii_utf8, "utf-8"): True,
                (non_ascii_utf8, "ascii"): False,
                (non_utf8, "utf-8"): None,
                (non_utf8, "ascii"): None
            }

            for filename_bytes, encoding in cases:
                case_name = f"filename: {filename_bytes}, encoding '{encoding}'"
                fs_encoder.set_encoding(encoding)
                expectation = cases[filename_bytes, encoding]
                name = os.fsdecode(filename_bytes)

                if expectation is not None:
                    self.assertEqual(ISOSR.is_consistent_utf8_filename(name),
                                     expectation,
                                     msg=case_name)
                else:
                    with self.assertRaises(UnicodeDecodeError, msg=case_name):
                        ISOSR.is_consistent_utf8_filename(name)

    def test_list_images(self):
        with tempfile.TemporaryDirectory() as d:
            # Given
            should_find = {"ascii_name1.iso", "ascii_name2.img"}

            shouldnt_find = {
                "not_an_image.txt",
                "misleadingly_named_directory.iso",
                os.fsdecode(b"nom probl\xe9matique.iso")
            }

            # We anticipate that in Python 3.7 or later the fs encoding will
            # be utf-8, which means these would be found.
            might_find = {
                os.fsdecode(b"\xf0\x9f\x8d\x8b.iso"),
                os.fsdecode(b"nom_agr\xc3\xa9able.img")
            }

            for filename in should_find | shouldnt_find | might_find:
                if "directory" in filename:
                    os.mkdir(os.path.join(d, filename))
                else:
                    with open(os.path.join(d, filename), 'w'):
                        pass

            # When
            found, num_ignored = ISOSR.list_images(d)

            # Then
            for filename in found:
                self.assertTrue(os.path.isfile(os.path.join(d, filename)))
            self.assertEqual(set(found) & shouldnt_find, set())
            self.assertEqual(set(found) & should_find, should_find)
            self.assertEqual(len(found) + num_ignored,
                             len(should_find) + len(might_find) + 1)

    @testlib.with_context
    def test_list_images_filters_non_utf8_names(self, context):
        with PatchFSEncoder() as fs_encoder:
            # Given
            images_dir = "/tmp/images"

            fs_encoder.set_encoding("utf-8")

            os.makedirs(images_dir)

            for filename_bytes in (b"simple_ascii.iso",
                                   b"g\xc3\xbcltigen_unicode.iso",
                                   b"probl\xe9matique.iso"):
                filename = os.fsdecode(filename_bytes)
                with open(os.path.join(images_dir, filename), "w"):
                    pass

            # When
            found, num_ignored = ISOSR.list_images(images_dir)

            # Then
            self.assertEqual(set(found),
                             {"simple_ascii.iso", "g\u00fcltigen_unicode.iso"})
            self.assertEqual(num_ignored, 1)

    @testlib.with_context
    def test_list_images_filters_non_ascii_names(self, context):
        with PatchFSEncoder() as fs_encoder:
            # Given

            images_dir = "/tmp/images"

            # This is the fs encoding in Python 3.6 when it's not set to anything
            # else.
            fs_encoder.set_encoding("ascii")

            os.makedirs(images_dir)

            for filename_bytes in (b"simple_ascii.iso",
                                   b"g\xc3\xbcltigen_unicode.iso",
                                   b"probl\xe9matique.iso"):
                filename = os.fsdecode(filename_bytes)
                with open(os.path.join(images_dir, filename), "w"):
                    pass

            # When
            found, num_ignored = ISOSR.list_images(images_dir)

            # Then
            self.assertEqual(found, ["simple_ascii.iso"])
            self.assertEqual(num_ignored, 2)

    @mock.patch('util.SMlog', autospec=True)
    @mock.patch('os.path.isdir', autospec=True)
    @mock.patch('os.listdir', autospec=True)
    @mock.patch('ISOSR.is_consistent_utf8_filename')
    def test_list_images_reports_problem_filenames(self,
                                                   mock_is_consistent,
                                                   mock_listdir,
                                                   mock_isdir,
                                                   mock_SMlog):
        # Given
        images_dir = "aMountPoint"

        mock_isdir.return_value = False
        mock_listdir.return_value = ["bad.iso", "good.iso", "junk.iso"]
        mock_is_consistent.side_effect = [
            False,
            True,
            UnicodeDecodeError("utf-8", b'', 0, 0, "an error message")
        ]

        # When
        found, num_ignored = ISOSR.list_images(images_dir)

        # Then
        self.assertEqual(found, ["good.iso"])
        self.assertEqual(num_ignored, 2)

        self.assertEqual(len(mock_SMlog.mock_calls), 2)

        _, (log_message,), _ = mock_SMlog.mock_calls[0]
        self.assertIn("'bad.iso'", log_message)

        _, (log_message,), _ = mock_SMlog.mock_calls[1]
        self.assertIn("'junk.iso'", log_message)


class PatchFSEncoder:
    def __init__(self, encoding="ascii"):
        self.encoding = encoding
        self.patch_fsencode = mock.patch("os.fsencode",
                                         new=self.fake_fsencode)
        self.patch_fsdecode = mock.patch("os.fsdecode",
                                         new=self.fake_fsdecode)

    def __enter__(self):
        self.patch_fsencode.start()
        self.patch_fsdecode.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.patch_fsencode.stop()
        self.patch_fsdecode.stop()

    def set_encoding(self, encoding):
        self.encoding = encoding

    def fake_fsencode(self, s):
        return s.encode(self.encoding, "surrogateescape")

    def fake_fsdecode(self, bs):
        return bs.decode(self.encoding, "surrogateescape")
