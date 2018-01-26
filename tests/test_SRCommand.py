import unittest
import mock

import SRCommand
import SR

class SomeException(Exception):
    pass


class TestStandaloneFunctions(unittest.TestCase):

    @mock.patch("sys.exit", autospec=True)
    @mock.patch('util.SMlog', autospec=True)
    @mock.patch('__builtin__.reduce', autospec=True)
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


    @mock.patch('util.logException', autospec=True)
    @mock.patch('SRCommand.SRCommand.run_statics', autospec=True)
    @mock.patch('SRCommand.SRCommand.parse', autospec=True)
    def test_run_print_xml_error_if_SRException(
            self,
            mock_parse,
            mock_run_statics,
            mock_logException):

        """ If an SR.SRException is thrown, assert that print <SR.SRException instance>.toxml()" is called.
        """

        import sys
        from StringIO import StringIO
        from SR import SRException
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
    @mock.patch('util.logException', autospec=True)
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
