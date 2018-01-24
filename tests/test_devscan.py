import testlib
import unittest
import mock

import SRCommand
import HBASR
import xmlrpclib

import devscan


def create_hba_sr():
    command = SRCommand.SRCommand(driver_info=None)
    command_parameter = (
        {
            'device_config': {},
            'command': 'irrelevant_some_command',
        },
        'irrelevant_method'
    )
    xmlrpc_arg = xmlrpclib.dumps(command_parameter)

    argv_patcher = mock.patch('sys.argv', new=[None, xmlrpc_arg])
    argv_patcher.start()
    command.parse()
    argv_patcher.stop()

    sr = HBASR.HBASR(command, '0')
    return sr


class TestScan(unittest.TestCase, testlib.XmlMixIn):
    @testlib.with_context
    def test_scanning_empty_sr(self, context):
        sr = create_hba_sr()
        sr._init_hbadict()

        result = devscan.scan(sr)

        self.assertXML("""
            <?xml version="1.0" ?>
            <Devlist/>
            """, result)

    @testlib.with_context
    def test_scanning_sr_with_devices(self, context):
        sr = create_hba_sr()
        adapter = context.add_adapter(testlib.SCSIAdapter())
        adapter.add_disk()
        sr._init_hbadict()

        result = devscan.scan(sr)

        self.assertXML("""
            <?xml version="1.0" ?>
            <Devlist>
                <Adapter>
                    <host>host0</host>
                    <name>Unknown</name>
                    <manufacturer>Unknown-description</manufacturer>
                    <id>0</id>
                </Adapter>
            </Devlist>
            """, result)

    @testlib.with_context
    def test_scanning_sr_includes_parameters(self, context):
        sr = create_hba_sr()
        adapter = context.add_adapter(testlib.SCSIAdapter())
        adapter.add_disk()
        sr._init_hbadict()
        adapter.add_parameter('fc_host', dict(port_name='VALUE'))

        result = devscan.scan(sr)

        self.assertXML("""
            <?xml version="1.0" ?>
            <Devlist>
                <Adapter>
                    <host>host0</host>
                    <name>Unknown</name>
                    <manufacturer>Unknown-description</manufacturer>
                    <id>0</id>
                    <fc_host>
                        <port_name>VALUE</port_name>
                    </fc_host>
                </Adapter>
            </Devlist>
            """, result)


class TestAdapters(unittest.TestCase):
    @testlib.with_context
    def test_no_adapters(self, context):
        result = devscan.adapters()

        self.assertEquals({'devs': {}, 'adt': {}}, result)

    @mock.patch('devscan.match_hbadevs', autospec=True)
    @testlib.with_context
    def test_exotic_adapter_with_security_device(self, context, match_hbadevs):
        adapter = context.add_adapter(testlib.AdapterWithNonBlockDevice())
        adapter.add_disk()

        match_hbadevs.return_value = 'lpfc'
        result = devscan.adapters()

        self.assertEquals(
            {
                'devs': {},
                'adt': {
                    'host0': 'lpfc'
                }
            },
            result)

    @testlib.with_context
    def test_adapter_and_disk_added(self, context):
        adapter = context.add_adapter(testlib.SCSIAdapter())
        adapter.add_disk()

        result = devscan.adapters()

        self.assertEquals(
            {
                'devs': {
                    'sda': {
                        'procname': 'Unknown',
                        'host': '0',
                        'target': '0'
                    }
                },
                'adt': {
                    'host0': 'Unknown'
                }
            },
            result)


class TestExtractDevName(unittest.TestCase):
    @testlib.with_context
    def test_26_kernel(self, context):
        context.kernel_version = '2.6'
        context.fake_makedirs('/somepath/block:sde')
        result = devscan._extract_dev_name('/somepath')

        self.assertEquals('sde', result)

    @testlib.with_context
    def test_3x_kernel(self, context):
        context.kernel_version = '3.2'
        context.fake_makedirs('/somepath/block/sde')
        result = devscan._extract_dev_name('/somepath')

        self.assertEquals('sde', result)

    @testlib.with_context
    def test_extract_dev_name_from_directory_without_block_device(
            self,
            context):
        context.kernel_version = '3.10'

        result = devscan._extract_dev_name('/nonexisting')

        self.assertEquals(devscan.INVALID_DEVICE_NAME, result)


class TestUpdateDevsDict(unittest.TestCase):
    def test_whencalled_updates_dict(self):
        devices = {}
        dev = 'dev'
        entry = 'entry'

        devscan.update_devs_dict(devices, dev, entry)

        self.assertEquals({'dev': 'entry'}, devices)

    def test_whencalled_with_empty_key_does_not_update_dict(self):
        devices = {}
        dev = devscan.INVALID_DEVICE_NAME
        entry = 'entry'

        devscan.update_devs_dict(devices, dev, entry)

        self.assertEquals({}, devices)
