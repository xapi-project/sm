import unittest
import unittest.mock as mock
import uuid
import xmlrpc.client

import SR
import SRCommand


class SomeException(Exception):
    pass


class TestStandaloneFunctions(unittest.TestCase):

    @mock.patch("sys.exit", autospec=True)
    @mock.patch('SRCommand.util.SMlog', autospec=True)
    @mock.patch('SRCommand.util.reduce', autospec=True)
    @mock.patch('SRCommand.SRCommand.run_statics', autospec=True)
    @mock.patch('SRCommand.SRCommand.parse', autospec=True)
    def test_run_correctly_log_all_exceptions(
            self,
            mock_parse,
            mock_run_statics,
            mock_reduce,
            mock_SMlog,
            mock_exit):

        """ Assert that any arbitrary exception raised and with a big message length is logged to SMlog. Only the first line of the message is asserted (traceback ommited).
        """

        from random import choice
        from string import ascii_letters
        from DummySR import DRIVER_INFO

        MSG_LEN = 2048

        # TestSRCommand data member to hold SMlog output.
        self.smlog_out = ""

        # Generate random exception message of MSG_LEN characters
        rand_huge_msg = ''.join(choice(ascii_letters) for _ in range(MSG_LEN))

        # Create function to raise exception in SRCommand.run()
        mock_driver = mock.Mock(side_effect=SomeException(rand_huge_msg))

        # MockSMlog replaces util.SMlog. Instead of printing to
        # /var/log/SMlog, it writes the output to self.smlog_out.
        def MockSMlog(str_arg):
            self.smlog_out = self.smlog_out + str_arg.strip()

        mock_reduce.return_value = ''
        mock_SMlog.side_effect = MockSMlog

        SRCommand.run(mock_driver, DRIVER_INFO)

        self.assertTrue(rand_huge_msg in self.smlog_out)

    @mock.patch('SRCommand.util.logException', autospec=True)
    @mock.patch('SRCommand.SRCommand.run_statics', autospec=True)
    @mock.patch('SRCommand.SRCommand.parse', autospec=True)
    def test_run_print_xml_error_if_SRException(
            self,
            mock_parse,
            mock_run_statics,
            mock_logException):

        """ If an SRException is thrown, assert that print
         <SRException instance>.toxml() is called.
        """

        import sys
        from io import StringIO
        from xs_errors import SRException
        from DummySR import DRIVER_INFO

        # Save original sys.stdout file object.
        saved_stdout = sys.stdout

        # Create a mock_stdout object and assign it to sys.stdout
        mock_stdout = StringIO()
        sys.stdout = mock_stdout

        # Create function to raise exception in SRCommand.run()
        mock_driver = mock.Mock(side_effect=SRException(
                                "[UnitTest] SRException thrown"))

        try:
            SRCommand.run(mock_driver, DRIVER_INFO)
        except SystemExit:
            pass

        # Write SRCommand.run() output to variable.
        actual_out = mock_stdout.getvalue()

        # Restore the original sys.stdout object.
        sys.stdout = saved_stdout

        expected_out = ("<?xml version='1.0'?>\n<methodResponse>\n<fault>\n"
                        "<value><struct>\n<member>\n<name>faultCode</name>\n"
                        "<value><int>22</int></value>\n</member>\n<member>\n"
                        "<name>faultString</name>\n<value><string>[UnitTest] "
                        "SRException thrown</string></value>\n</member>\n"
                        "</struct></value>\n</fault>\n</methodResponse>\n\n")

        self.assertEqual(actual_out, expected_out)

    @mock.patch("sys.exit", autospec=True)
    @mock.patch('SRCommand.util.logException', autospec=True)
    @mock.patch('SRCommand.SRCommand.run_statics', autospec=True)
    @mock.patch('SRCommand.SRCommand.parse', autospec=True)
    def test_run_wrapped_if_not_SRException(
            self,
            mock_parse,
            mock_run_statics,
            mock_logException,
            mock_exit):

        """ If an exception other than SR.SRException is thrown, assert that it is wrapped and not thrown.
        """

        from DummySR import DRIVER_INFO

        # Create function to raise exception in SRCommand.run()
        mock_driver = mock.Mock(side_effect=SomeException)

        SRCommand.run(mock_driver, DRIVER_INFO)

    @mock.patch("sys.exit", autospec=True)
    @mock.patch('SRCommand.util.logException', autospec=True)
    @mock.patch('SRCommand.SRCommand', autospec=True)
    def test_run_reports_protocol_error(
            self,
            mock_sr_command,
            mock_logException,
            mock_exit):

        """
        If XenAPI raises a protocol error convert to error code
        """
        from DummySR import DRIVER_INFO

        def raise_error(sr):
            raise xmlrpc.client.ProtocolError(
                'XapiUrl', 500,
                'Internal Error', {})

        # Create function to raise exception in SRCommand.run()
        mock_cmd = mock.MagicMock()
        mock_cmd.run.side_effect = raise_error
        mock_cmd.sr_uuid = str(uuid.uuid4())
        mock_sr_command.return_value = mock_cmd

        mock_driver = mock.MagicMock(autospec=SR.SR)

        with mock.patch('builtins.print') as mock_print:
            SRCommand.run(mock_driver, DRIVER_INFO)

        self.assertIn('<value><int>154</int></value>',
                      mock_print.call_args[0][0])

    @mock.patch("SRCommand.os.fsencode",
                new=lambda s: s.encode("ascii", "surrogateescape"))
    @mock.patch("SRCommand.os.fsdecode",
                new=lambda bs: bs.decode("ascii", "surrogateescape"))
    @mock.patch('sm.core.util.gen_uuid', autospec=True)
    def test_parse_handles_wide_chars(self, gen_uuid):
        import os
        import xmlrpc.client
        from DummySR import DRIVER_INFO

        gen_uuid.return_value = '13c4384e-897b-e745-6b3e-9a89c06537be'
        xmlrpc_method = "vdi_create"
        xmlrpc_params = {
            'host_ref': 'OpaqueRef:133c7c46-f4d9-3695-83c4-bf8574b89fb9',
            'command': 'vdi_create',
            'args': [
                '10737418240',
                '\u4e2d\u6587\u673a\u5668 0',
                'Created by template provisioner',
                '',
                'false',
                '19700101T00:00:00Z',
                '',
                'false'
            ],
            'device_config': {
                'SRmaster': 'true',
                'device': '/dev/disk/by-id/scsi-3600508b1001c25e9eea8ead175fd83fb-part3'
            },
            'session_ref': 'OpaqueRef:c2c628b6-93c3-5e29-00cf-4f15a34e1555',
            'sr_ref': 'OpaqueRef:c523a79a-8a60-121c-832e-d507586cb117',
            'vdi_type': 'system',
            'sr_uuid': '13c4384e-897b-e745-6b3e-9a89c06537be',
            'vdi_sm_config': {},
            'subtask_of': 'DummyRef:|0c533c65-d321-59c2-f540c-e66efbe3b1b7|VDI.create'
        }

        # We are trying to simulate how a UTF8-encoded request passed on the
        # command line shows up in sys.argv. FS encoding always makes use of
        # "surrogateescape" (see https://peps.python.org/pep-0383/) but the
        # actual encoding would probably depend on locale settings.

        request = xmlrpc.client.dumps((xmlrpc_params,), xmlrpc_method)
        argv = ["foo.py", os.fsdecode(request.encode("utf-8"))]
        with mock.patch("sys.argv", new=argv):
            srcommand = SRCommand.SRCommand(DRIVER_INFO)
            srcommand.parse()

            self.assertEqual(srcommand.cmd, xmlrpc_method)
            self.assertEqual(srcommand.params, xmlrpc_params)
