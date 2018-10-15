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
import xcp.logger as logger
from argparse import ArgumentError
from service import ServerInstance
from service import ClientInstance
from service import parseArguments

logInfoStr = ''
servInst = None

def sig_me():
    time.sleep(1)
    os.kill(os.getpid(), signal.SIGINT)

def prepare_clean_start(childHold=False):
    try:
        shutil.rmtree('/tmp/test_service/run/')
    except Exception:
        pass

    if childHold:
        script_cmd = "sleep 3"
    else:
        script_cmd = "touch " + ServerTest.PIPE_BASE_PATH + "target"
    script_file = os.open("/tmp/test_service_script.sh", os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    os.write(script_file, "#!/bin/sh\n")
    os.write(script_file, script_cmd)
    os.close (script_file)
    os.chmod("/tmp/test_service_script.sh", stat.S_IEXEC| stat.S_IWRITE | stat.S_IREAD)

class ServerTest(ServerInstance):

    def __init__(self, name, script, basedir, uniq, block):

        ServerInstance.PIPE_BASE_PATH = '/tmp/test_service/run/'
        if basedir:
            try:
                os.mkdir('/tmp/test_service/', 0755)
            except Exception:
                pass
        else:
            shutil.rmtree('/tmp/test_service/')

        super(ServerTest, self).__init__(name, script, uniq, block)
        self.logInfoNum = 0

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
        global logInfoStr
        logInfoStr = logtxt

class ClientTest(ClientInstance):
    def __init__(self, name, wait):

        ClientInstance.PIPE_BASE_PATH = '/tmp/test_service/run/'

        super(ClientTest, self).__init__(name, wait)

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
    serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)
    serv.work()

def create_uniq_server():
    serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, True, True)
    global servInst
    servInst = serv
    serv.work()

def create_client():
    clnt = ClientTest('testservice', False)
    clnt.work()

def create_two_client():
    clnt = ClientTest('testservice', False)
    clnt.work()

    time.sleep(1)

    clnt = ClientTest('testservice', False)
    clnt.work()

def create_wait_client():
    clnt = ClientTest('testservice', True)
    clnt.work()

class ServiceTests(unittest.TestCase):
    def test_server_init_success_clean(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)

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

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)

        self.assertTrue(stat.S_ISFIFO(os.stat(serv.pipePath).st_mode))


    def test_server_init_mkdir_fail(self):
        try:
            shutil.rmtree('/tmp/test_service/')
        except Exception:
            pass
        
        with self.assertRaises(OSError) as ctx:
            serv = ServerTest('testservice', ServerTest.PIPE_BASE_PATH + "test_service_script.sh", False, False, True)

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
            serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)

        self.assertEquals(ctx.exception.errno, errno.EEXIST)

    def test_server_close_fifo(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)
        serv._openRead()
        serv._closeFIFO()
        self.assertTrue(True)

    def test_server_openRead_fail(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)
        try:
            os.remove(serv.pipePath)
        except Exception:
            pass

        with self.assertRaises(OSError) as ctx:
            serv._openRead()
        
        self.assertEquals(ctx.exception.errno, 2)

    def test_server_readRequest_nofifo(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)
        serv._openRead()
        serv._closeFIFO()
      
        self.assertFalse(serv._readRequest())

    def test_server_readRequest_again(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)
        serv._openRead()

        clnt = ClientTest('testservice', False)
        clnt._openWrite()
        clnt._writeRequest()

        self.assertTrue(serv._readRequest())
        self.assertTrue(serv._readRequest())

    def test_server_readRequest_fail(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)
        serv._openRead()
        serv._testCloseFifo()

        with self.assertRaises(OSError) as ctx:
            serv._readRequest()

        self.assertEquals(ctx.exception.errno, 9)
        
    def test_server_waitForInput_false(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)
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

    def test_work_skip(self):
        prepare_clean_start(True)

        thread.start_new_thread(create_uniq_server, ())
        time.sleep(1)
        create_two_client()
        time.sleep(1)

        self.assertEquals(logInfoStr, "Skip invoke scripts, due to previous one in progress")

    def test_wait_client_work(self):
        prepare_clean_start()

        thread.start_new_thread(create_wait_client, ())
        time.sleep(1)
        thread.start_new_thread(create_server, ())
        time.sleep(1)

        self.assertTrue(os.path.exists(ServerTest.PIPE_BASE_PATH + "target"))

    def test_no_server_client_fail(self):
        prepare_clean_start()

        with self.assertRaises(OSError) as ctx:
            create_client()

        self.assertEquals(ctx.exception.errno, 6)

    def test_client_openWrite_fail(self):
        prepare_clean_start()

        try:
            os.mkdir('/tmp/test_service/', 0755)
        except Exception:
            pass

        clnt = ClientTest('testservice', False)
        try:
            os.remove(clnt.pipePath)
        except Exception:
            pass

        with self.assertRaises(OSError) as ctx:
            clnt._openWrite()

        self.assertEquals(ctx.exception.errno, 2)

    def test_client_writeRequest_fail(self):
        prepare_clean_start()

        serv = ServerTest('testservice', "/tmp/test_service_script.sh", True, False, True)
        serv._openRead()

        clnt = ClientTest('testservice', False)
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
        testArgs = ["-m", "server", "-s", "testservice", "-e", "/tmp/no_such_file"]

        with self.assertRaises(ArgumentError):
            parseArguments(testArgs)

