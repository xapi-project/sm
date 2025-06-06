import unittest
import testlib
import os
import unittest.mock as mock
import errno

from sm import refcounter


class TestRefCounter(unittest.TestCase):
    @testlib.with_context
    def test_get_whencalled_creates_namespace(self, context):
        os.makedirs(refcounter.RefCounter.BASE_DIR)

        refcounter.RefCounter.get('not-important', False, 'somenamespace')

        self.assertEqual(
            ['somenamespace'],
            os.listdir(os.path.join(refcounter.RefCounter.BASE_DIR)))

    @testlib.with_context
    def test_get_whencalled_returns_counters(self, context):
        os.makedirs(refcounter.RefCounter.BASE_DIR)

        result = refcounter.RefCounter.get(
            'not-important', False, 'somenamespace')

        self.assertEqual(1, result)

    @testlib.with_context
    def test_get_whencalled_creates_refcounter_file(self, context):
        os.makedirs(refcounter.RefCounter.BASE_DIR)

        refcounter.RefCounter.get('someobject', False, 'somenamespace')

        self.assertEqual(
            ['someobject'],
            os.listdir(os.path.join(
                refcounter.RefCounter.BASE_DIR, 'somenamespace')))

    @testlib.with_context
    def test_get_whencalled_refcounter_file_contents(self, context):
        os.makedirs(refcounter.RefCounter.BASE_DIR)

        refcounter.RefCounter.get('someobject', False, 'somenamespace')

        path_to_refcounter = os.path.join(
            refcounter.RefCounter.BASE_DIR, 'somenamespace', 'someobject')

        refcounter_file = open(path_to_refcounter, 'r')
        contents = refcounter_file.read()
        refcounter_file.close()

        self.assertEqual('1 0\n', contents)

    @testlib.with_context
    def test_put_is_noop_if_already_zero(self, context):
        os.makedirs(refcounter.RefCounter.BASE_DIR)

        result = refcounter.RefCounter.put(
            'someobject', False, 'somenamespace')

        self.assertEqual(0, result)

    @testlib.with_context
    def test_writeCount_returns_true_if_file_found(self, context):
        os.makedirs('/existing')

        result = refcounter.RefCounter._writeCount('/existing/file', 1, 1)

        self.assertTrue(result)

    @testlib.with_context
    def test_writeCount_returns_false_if_file_not_found(self, context):
        result = refcounter.RefCounter._writeCount('/nonexisting/file', 1, 1)

        self.assertFalse(result)

    @mock.patch('sm.refcounter.os.rmdir', autospec=True)
    @mock.patch('sm.refcounter.os.unlink', autospec=True)
    @mock.patch('sm.refcounter.util.pathexists', autospec=True)
    def test_removeObject_ignores_if_directory_already_removed(self,
                                                               pathexists,
                                                               unlink,
                                                               rmdir):
        rmdir.side_effect = OSError(errno.ENOENT, 'ignored')

        refcounter.RefCounter._removeObject('namespace', 'obj')

        rmdir.assert_called_once_with(
            os.path.join(refcounter.RefCounter.BASE_DIR, 'namespace'))

    @mock.patch('sm.refcounter.os.rmdir', autospec=True)
    @mock.patch('sm.refcounter.os.unlink', autospec=True)
    @mock.patch('sm.refcounter.util.pathexists', autospec=True)
    def test_removeObject_ignores_if_directory_not_empty(self,
                                                         pathexists,
                                                         unlink,
                                                         rmdir):
        rmdir.side_effect = OSError(errno.ENOTEMPTY, 'ignored')

        refcounter.RefCounter._removeObject('namespace', 'obj')

        rmdir.assert_called_once_with(
            os.path.join(refcounter.RefCounter.BASE_DIR, 'namespace'))

# Re-use legacy tests embedded in refcounter
testcase = unittest.FunctionTestCase(refcounter.RefCounter._runTests)
with mock.patch.object(refcounter.RefCounter, "BASE_DIR", "./fakesm/refcount"):
    unittest.TextTestRunner().run(testcase)
