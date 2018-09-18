import errno
import mock
import os
import unittest
import stat
import service
import shutil
import thread
import signal
import time
from argparse import ArgumentError
from service import ServerInstance
from service import ClientInstance
from service import parseArguments

def sig_me():
    time.sleep(1)
    os.kill(os.getpid(), signal.SIGINT)

def prepare_clean_start():
    try:
        shutil.rmtree('/tmp/test_service/run/')
    except Exception:
        pass

    script_cmd = "touch " + ServerTest.PIPE_BASE_PATH + "target"
    script_file = os.open("/tmp/test_service_script.sh", os.O_WRONLY | os.O_CREAT)
    os.write(script_file, "#!/bin/sh\n")
    os.write(script_file, script_cmd)
    os.close (script_file)
    os.chmod("/tmp/test_service_script.sh", stat.S_IEXEC| stat.S_IWRITE | stat.S_IREAD)

class ServerTest(ServerInstance):

    def __init__(self, name, script, basedir):

        ServerInstance.PIPE_BASE_PATH = '/tmp/test_service/run/'
        if basedir:
            try:
                os.mkdir('/tmp/test_service/', 0755)
            except Exception:
                pass
        else:
            shutil.rmtree('/tmp/test_service/')

        super(ServerTest, self).__init__(name, script)

    def _testCloseFifo(self):
        try:
            os.close(self.fifo)
        except OSError:
            raise

    def _shutdown(self):
        self.shutdown = True

    def logErr(self, logtxt):
        pass

    def logInfo(self, logtxt):
        pass

class ClientTest(ClientInstance):
    def __init__(self, name):

        ClientInstance.PIPE_BASE_PATH = '/tmp/test_service/run/'

        super(ClientTest, self).__init__(name)

    def _testCloseFifo(self):
        try:
            os.close(self.fifo)
        except OSError:
            raise

    def logErr(self, logtxt):
        pass

    def logInfo(self, logtxt):
        pass

def create_server():
    serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)
    serv.work()

def create_client():
    clnt = ClientTest('testservice')
    clnt.work()

class ServiceTests(unittest.TestCase):
    def test_server_init_success_clean(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)

        self.assertTrue(stat.S_ISFIFO(os.stat(serv.pipePath).st_mode))

    def test_server_init_success_garbage(self):
        prepare_clean_start()

        try:
            os.mkdir('/tmp/test_service/run/')
        except Exception:
            pass

        try:
            script_file = os.open("/tmp/test_service/run/testservice", os.O_WRONLY)
            os.write(script_file, "#!/bin/sh\n")
            os.close(script_file)
        except Exception:
            pass

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)

        self.assertTrue(stat.S_ISFIFO(os.stat(serv.pipePath).st_mode))


    def test_server_init_mkdir_fail(self):
        try:
            shutil.rmtree('/tmp/test_service/')
        except Exception:
            pass
        
        with self.assertRaises(OSError) as ctx:
            serv = ServerTest('testservice', ServerTest.PIPE_BASE_PATH + "test_service_script.sh", False)

        self.assertEquals(ctx.exception.errno, 2)

    def test_server_init_clean_fail(self):
        prepare_clean_start()

        try:
            os.mkdir('/tmp/test_service/run/')
        except Exception:
            pass

        try:
            os.symlink("/dev/null", '/tmp/test_service/run/testservice')
        except Exception:
            raise

        script_cmd = "touch " + ServerTest.PIPE_BASE_PATH + "target"
        script_file = os.open("/tmp/test_service_script.sh", os.O_WRONLY | os.O_CREAT)
        os.write(script_file, "#!/bin/sh\n")
        os.write(script_file, script_cmd)
        os.close (script_file)
        os.chmod("/tmp/test_service_script.sh", stat.S_IEXEC| stat.S_IWRITE)

        with self.assertRaises(OSError) as ctx:
            serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)

        self.assertEquals(ctx.exception.errno, errno.EEXIST)

    def test_server_close_fifo(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)
        serv._openRead()
        serv._closeFIFO()
        self.assertTrue(True)

    def test_server_openRead_fail(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)
        try:
            os.remove(serv.pipePath)
        except Exception:
            pass

        with self.assertRaises(OSError) as ctx:
            serv._openRead()
        
        self.assertEquals(ctx.exception.errno, 2)

    def test_server_readRequest_nofifo(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)
        serv._openRead()
        serv._closeFIFO()
      
        self.assertFalse(serv._readRequest())

    def test_server_readRequest_again(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)
        serv._openRead()

        clnt = ClientTest('testservice')
        clnt._openWrite()
        clnt._writeRequest()

        self.assertTrue(serv._readRequest())
        self.assertTrue(serv._readRequest())

    def test_server_readRequest_fail(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)
        serv._openRead()
        serv._testCloseFifo()

        with self.assertRaises(OSError) as ctx:
            serv._readRequest()

        self.assertEquals(ctx.exception.errno, 9)
        
    def test_server_waitForInput_false(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)
        serv._openRead()

        thread.start_new_thread(sig_me, ())
        self.assertFalse(serv._waitForInput())

    def test_work(self):
        prepare_clean_start()

        thread.start_new_thread(create_server, ())
        time.sleep(1)
        thread.start_new_thread(create_client, ())
        time.sleep(1)

        self.assertTrue(os.path.exists(ServerTest.PIPE_BASE_PATH + "target"))

    def test_client_openWrite_fail(self):
        prepare_clean_start()

        clnt = ClientTest('testservice')
        try:
            os.remove(clnt.pipePath)
        except Exception:
            pass

        with self.assertRaises(OSError) as ctx:
            clnt._openWrite()

        self.assertEquals(ctx.exception.errno, 2)

    def test_client_writeRequest_fail(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True)
        serv._openRead()

        clnt = ClientTest('testservice')
        clnt._openWrite()
        clnt._testCloseFifo()

        with self.assertRaises(OSError) as ctx:
            clnt._writeRequest()

        self.assertEquals(ctx.exception.errno, 9)

    def test_argParse_wrong_mode(self):
        testArgs = ["-m", "testmode"]
        
        with self.assertRaises(SystemExit):
            parseArguments(testArgs)

    def test_argParse_server_missing_arg(self):
        testArgs = ["-m", "server"]

        with self.assertRaises(SystemExit):
            parseArguments(testArgs)

    def test_argParse_server_no_file(self):
        testArgs = ["-m", "server", "-e", "/tmp/no_such_file"]

        with self.assertRaises(SystemExit):
            parseArguments(testArgs)

