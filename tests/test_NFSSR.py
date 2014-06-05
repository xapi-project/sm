import unittest
import xmlrpclib
import mock

import testlib

import NFSSR
import SRCommand


def create_nfs_sr():
    command = SRCommand.SRCommand(driver_info=None)
    command_parameter = (
        {
            'device_config': {
                'server': 'someserver',
                'serverpath': '/somepath',
            },
            'command': 'irrelevant_some_command',
        },
        'irrelevant_method'
    )
    xmlrpc_arg = xmlrpclib.dumps(command_parameter)

    argv_patcher = mock.patch('sys.argv', new=[None, xmlrpc_arg])
    argv_patcher.start()
    command.parse()
    argv_patcher.stop()

    sr = NFSSR.NFSSR(command, '0')
    return sr


def fake_rpcinfo(args, stdin):
    return_code = 0
    stdout = ''
    stderr = ''
    return (return_code, stdout, stderr)


def fake_monunt_nfs(args, stdin):
    return_code = 0
    stdout = ''
    stderr = ''
    return (return_code, stdout, stderr)


class TestNFSSR(unittest.TestCase):
    @testlib.with_context
    def test_mount_remotepath(self, context):
        context.setup_error_codes()
        context.add_executable('/usr/sbin/rpcinfo', fake_rpcinfo)
        context.add_executable('mount.nfs', fake_monunt_nfs)
        sr = create_nfs_sr()
        context.setup_server('someserver', 2049)
        sr.mount_remotepath('mmm')
