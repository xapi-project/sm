import mock
import nfs
import ISOSR
import unittest
import xs_errors 
import util
import SR
import errno
import testlib 


class FakeISOSR(ISOSR.ISOSR):
    uuid = None
    sr_ref = None
    session = None
    srcmd = None

    def __init__(self, srcmd, none):
        self.dconf = srcmd.dconf
        self.srcmd = srcmd

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

        self.assertEquals(self.create_isosr().nfsversion, 'aNfsversion')

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
    # Can't use autospec due to http://bugs.python.org/issue17826
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_nfs(self, _checkmount, makedirs, validate_nfsversion,
                        convertDNS, soft_mount, gen_uuid):
        validate_nfsversion.return_value = 'aNfsversionChanged'
        isosr = self.create_isosr(location='aServer:/aLocation', atype='nfs_iso',
                                  sr_uuid='asr_uuid')
        _checkmount.side_effect = [False, True]
        gen_uuid.return_value = 'aUuid'

        isosr.attach(None)

        soft_mount.assert_called_once_with('/var/run/sr-mount/asr_uuid',
                                           'aServer',
                                           '/aLocation',
                                           'tcp',
                                           useroptions='',
                                           nfsversion='aNfsversionChanged')

class TestISOSR_overSMB(unittest.TestCase):

    def create_smbisosr(self, location='\\aServer\aLocation', atype=None,
                        sr_uuid='asr_uuid', server='\\aServer',
                        serverpath='/aServerpath', username='aUsername',
                        password='aPassword', vers=None, options=''):
        srcmd = mock.Mock()
        srcmd.dconf = {
            'location': location,
            'server': server,
            'serverpath': serverpath,
            'username': username,
            'password': password,
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
        isosr = FakeISOSR(srcmd, None)
        isosr.load(sr_uuid)
        return isosr

    def test_load(self):
        self.create_smbisosr()

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_with_smb_version_1(self, context, _checkmount, pread,
                                       _checkTargetStr, makedirs):
        """
        Positive case, over XC/XE CLI with version 1.0.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(atype='cifs', vers='1.0')
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_with_smb_version_3(self, context, _checkmount, pread,
                                       _checkTargetStr, makedirs):
        """
        Positive case, over XC/XE CLI with version 3.0.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(atype='cifs', vers='3.0')
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread')
    @mock.patch('ISOSR.ISOSR._checkmount')
    @mock.patch('ISOSR.ISOSR.updateSMBVersInPBDConfig')
    def test_attach_with_smb_no_version(self, context, updateSMBVersInPBDConfig,
                                        _checkmount, pread, _checkTargetStr, makedirs):
        """
        Positive case, over XC/XE CLI without version.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(atype='cifs')
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_smb_via_xemount_version_1(self, context, _checkmount,
                                              _checkTargetStr, makedirs, gen_uuid):
        """
        Positive case, over xe-sr-mount CLI with version 1.0.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(options='-o username=administrator,password=password,vers=1.0')
        smbsr.attach(None)

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_smb_via_xemount_version_3(self, context, _checkmount,
                                              _checkTargetStr, makedirs, gen_uuid):
        """
        Positive case, over xe-sr-mount CLI with version 3.0.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(options='-o username=administrator,password=password,vers=3.0')
        smbsr.attach(None)

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('ISOSR.ISOSR._checkmount')
    @mock.patch('ISOSR.ISOSR.updateSMBVersInPBDConfig')
    def test_attach_smb_via_xemount_no_version(self, context,
                                               updateSMBVersInPBDConfig,
                                               _checkmount,
                                               _checkTargetStr, makedirs,
                                               gen_uuid):
        """
        Positive case, without version from xe-sr-mount.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(options='-o username=administrator,password=password')
        smbsr.attach(None)

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread')
    def test_attach_smb_wrongversion(self, context, pread, _checkTargetStr,
                                     makedirs, gen_uuid):
        """
        Unsupported version from XC/XE CLI.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(atype='cifs', vers='2.0')
        raised_exception = None
        with self.assertRaises(SR.SROSError) as context:
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
    def test_attach_smb_wrongversion_via_xemount(self, context,
                                                 _checkTargetStr, makedirs,
                                                 gen_uuid):
        """
        Unsupported version from xe-sr-mount.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(options='-o vers=2.0')
        with self.assertRaises(SR.SROSError) as context:
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
    @mock.patch('util.pread')
    @mock.patch('ISOSR.ISOSR._checkmount')
    @mock.patch('ISOSR.ISOSR.updateSMBVersInPBDConfig')
    def test_attach_smb_version_fallback_with_smb_3_disabled(self, context,
            updateSMBVersInPBDConfig, _checkmount, pread, _checkTargetStr, makedirs, gen_uuid):
        """
        Fall back scenario from XC/XE CLI with smb3 diabled and smb1 enabled.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(atype='cifs')
        pread.side_effect = [util.CommandException(errno.EHOSTDOWN), " "]
        _checkmount.side_effect = [False, True]
        smbsr.attach(None)

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_smb_version_fallback_with_smb_1_3_disabled(self, context,
                                                               _checkmount,
                                                               pread,
                                                               _checkTargetStr,
                                                               makedirs,
                                                               gen_uuid):
        """
        Fall back scenario from XC/XE CLI with smb3 diabled and smb1 disabled.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(atype='cifs')
        pread.side_effect = [util.CommandException(errno.EHOSTDOWN), \
                util.CommandException(errno.EHOSTDOWN), util.CommandException(errno.EHOSTDOWN)]
        _checkmount.side_effect = [False, True]
        with self.assertRaises(SR.SROSError) as context:
            smbsr.attach(None)
        self.assertEqual(context.exception.errno, 222)
        self.assertEqual(
            str(context.exception),
            'Could not mount the directory specified in Device Configuration [opterr=exec failed]'
        )

    @testlib.with_context
    @mock.patch('util.gen_uuid')
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_smb_via_xemount_no_version_fallback(self, context,
                                                        _checkmount, pread,
                                                        _checkTargetStr,
                                                        makedirs, gen_uuid):
        """
        Fall back scenario from xe-sr-mount with smb3 diabled and smb1 enabled.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(options='-o username=administrator,password=password')
        pread.side_effect = [util.CommandException(errno.EHOSTDOWN), " "]
        smbsr.attach(None)

    @testlib.with_context
    @mock.patch('util.makedirs')
    @mock.patch('ISOSR.ISOSR._checkTargetStr')
    @mock.patch('util.pread')
    @mock.patch('ISOSR.ISOSR._checkmount')
    def test_attach_smb_version_fallback_error(self, context, _checkmount,
                                               pread, _checkTargetStr,
                                               makedirs):
        """
        Fall back scenario negative case from xe-sr-mount with smb3 diabled and smb1 disabled.
        """
        context.setup_error_codes()
        smbsr = self.create_smbisosr(atype='cifs')
        pread.side_effect = [util.CommandException(errno.EHOSTDOWN), \
                             util.CommandException(errno.EHOSTDOWN)]
        _checkmount.side_effect = [False, True]
        with self.assertRaises(util.CommandException):
            smbsr.attach(None)
