import cbtutil
import mock
import unittest
import uuid


class TestCbtutil(unittest.TestCase):

    @mock.patch('cbtutil._call_cbt_util', autospec=True)
    def test_createCBTLog(self, mock_util):
        cbtutil.create_cbt_log('testlog', 4096)
        mock_util.assert_called_with([cbtutil.CBT_UTIL, 'create', '-n', 'testlog', '-s', '4096'])

    @mock.patch('cbtutil._call_cbt_util', autospec=True)
    def test_setCBTParent(self, mock_util):
        parent = uuid.uuid4()
        cbtutil.set_cbt_parent('testlog', parent)
        mock_util.assert_called_with([cbtutil.CBT_UTIL, 'set', '-n', 'testlog', '-p', str(parent)])

    @mock.patch('cbtutil._call_cbt_util', autospec=True)
    def test_setCBTChild(self, mock_util):
        child = uuid.uuid4()
        cbtutil.set_cbt_child('testlog', child)
        mock_util.assert_called_with([cbtutil.CBT_UTIL, 'set', '-n', 'testlog', '-c', str(child)])

    @mock.patch('cbtutil._call_cbt_util', autospec=True)
    def test_setCBTConsistency_consistent_success(self, mock_util):
        cbtutil.set_cbt_consistency('testlog', True)
        mock_util.assert_called_with([cbtutil.CBT_UTIL, 'set', '-n', 'testlog', '-f', '1'])

    @mock.patch('cbtutil._call_cbt_util', autospec=True)
    def test_setCBTConsistency_not_consistent_success(self, mock_util):
        cbtutil.set_cbt_consistency('testlog', False)
        mock_util.assert_called_with([cbtutil.CBT_UTIL, 'set', '-n', 'testlog', '-f', '0'])

    @mock.patch('cbtutil._call_cbt_util', autospec=True)
    def test_getCBTConsistency_consistent(self, mock_util):
        mock_util.return_value = '1'
        consistent = cbtutil.get_cbt_consistency('testlog')
        self.assertEquals(consistent, True)

    @mock.patch('cbtutil._call_cbt_util', autospec=True)
    def test_getCBTConsistency_not_consistent(self, mock_util):
        mock_util.return_value = '0'
        consistent = cbtutil.get_cbt_consistency('testlog')
        self.assertEquals(consistent, False)
