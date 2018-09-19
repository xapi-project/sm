#!/usr/bin/env python
#
# Copyright (C) Citrix Systems Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation; version 2.1 only. #
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# This script can be used as both server as client. For server mode, it is expected to
# be started as daemon, wait on some named pipe depending on input, and upon wake up
# (client side writes the pipe), read out all content from the pipe, and execute a
# pre-configed (through input parameter) scripts. This can avoid input storm so if multiple
# client side invoke, server side just do one invoke of the executable (assume the 
# script do not need to be executed multiple times in case of multiple trigger). For
# client side, it is just simple logic to open the same service pipe as server, and write
# something to wake up the server to do the work.
#
# There are multiple input parameters,
#    -s/--service:  this is the service name for both server and client, supposing a short name
#    -m/--mode: this is the mode specify 'client' or 'server'
#    -e/--executable: this is the executable script to be invoked upon server wake up, 
#                     only needed for server logic. Arguments have to be included along with
#                     script name as one string.
#
# Example:
#    server:
#       service.py -s usbscan -m server -e /opt/xensource/libexec/usb_change
#
#    client:
#        service.py -s usbscan -m client
#

import argparse
import os
import select
import subprocess
import logging
import xcp.logger as logger
import errno
import shutil
import stat
import sys

BUFFER_SIZE = 2048

class ServiceSkeleton(object):
    """ base class for both client and server, maily as utilities """

    PIPE_BASE_PATH = '/var/lib/misc/'
    
    def __init__(self, name):
        super(ServiceSkeleton, self).__init__()
        self.name = name
        self.pipePath = self.PIPE_BASE_PATH + name
        self._checkPath()
        self.logPrefix = name + ":"
        self.fifo = None

    def _checkPath(self):
        """ check if the paths and files exist, otherwise create them """
        
        if not os.path.exists(self.PIPE_BASE_PATH):
            try:
                os.mkdir(self.PIPE_BASE_PATH, 0755)
            except OSError as err:
                self.logErr("create base path failed!")
                raise
        
        create_fifo = False
        if os.path.exists(self.pipePath):
            if not stat.S_ISFIFO(os.stat(self.pipePath).st_mode):
                # as this is a specific file in our specific dir,
                # assume it is used exclusively, so the current none pipe
                # stuff is considered garbage and try to delete that here.
                if os.path.isfile(self.pipePath):
                    os.remove(self.pipePath)  # remove the file
                    self.logInfo("removed file:" + self.pipePath)
                elif os.path.isdir(self.pipePath):
                    shutil.rmtree(self.pipePath)  # remove dir and all contains
                    self.logInfo("removed dir:" + self.pipePath)
                else:
                    self.logErr("Unexpected file node at:" + self.pipePath)
                    raise OSError(errno.EEXIST, "Unexpected file node exists", self.pipePath)

                create_fifo = True
        else:
                create_fifo = True

        if create_fifo:
            try:
                os.mkfifo(self.pipePath)
            except OSError as err:
                if err.errno != errno.EEXIST:
                    self.loggErr("Error encoutnered while create pipe")
                    raise

    def _closeFIFO(self):
        if self.fifo != None:
            try:
                os.close(self.fifo)
            except OSError:
                pass # just ignore

            self.fifo = None
                
    def logInfo(self, logtxt):
        logger.info(self.logPrefix + logtxt)
        
    def logErr(self, logtxt):
        logger.error(self.logPrefix + logtxt)
        
    def work(self):
        """ virtual function for server and client """
        pass

class ServerInstance(ServiceSkeleton):
    """ server side implementation """

    def __init__(self, name, script):
        super(ServerInstance, self).__init__(name)
        self.script = script
        self.epollObj = select.epoll()
        
    def _openRead(self):
        """ server side open named pipe to read """
        try:
            # here to have write permission also is necessary to keep one writer
            # to avoid pipe 'broken' in case of other closure.
            self.fifo = os.open(self.pipePath, os.O_RDWR | os.O_NONBLOCK)
            self.epollObj.register(self.fifo, select.EPOLLIN)
        except OSError as err:
            self.logErr("open file read error:" + str(err))
            raise

    def _executeSub(self):
        """ execute the passed script with subprocess """
        self.logInfo("will invoke scripts:" + self.script)
        try:
            subprocess.Popen(self.script)
        except OSError as err:
            self.logErr("execute script error:" + str(err))
            
    def _readRequest(self):
        """ read all that is written in the pipe, so in case of input storm,
            just do work once.
        """
        if self.fifo != None:
            try:
                while True:
                    self.buffer = os.read(self.fifo, BUFFER_SIZE)
                    if len(self.buffer) == 0:
                        # hung up
                        self.logErr("hung up found while read, exiting")
                        raise IOError(errno.EPIPE, 'hung up occurred while read pipe', self.pipePath)
            except OSError as err:
                if err.errno == errno.EAGAIN or err.errno == errno.EWOULDBLOCK:
                    return True # assume already read as invoked after poll
                else:
                    self.logErr("Error occurred while read pipe:" + str(err))
                    raise
        else:
            return False
            
    def _waitForInput(self):
        """ hang on the poll to wait for incoming requests.
            As we hold one writer, there should be no EPOLLHUP when others closed the pipe.
        """
        try:
            events = self.epollObj.poll(-1)
        except KeyboardInterrupt:
            return False
        except IOError as err:
            if err.errno == errno.EINTR:
                return False
            else:
                self.logErr("Error at polling fd:" + str(err))
                raise

        for fileno, event in events:
            if fileno == self.fifo and event & select.EPOLLIN:
                return True
            elif fileno == self.fifo and event & (select.EPOLLERR | select.EPOLLHUP):
                # probably some log and raise failure
                self.logErr("exit due to unexpected events:" + str(event))
                raise IOError(errno.EPIPE, 'hung up or error occurred while poll', self.pipePath)

        return False
                
    def work(self):
        """ server side daemon logic """
        self.logInfo("server started")
        
        try:
            self._openRead()
            while True:
                if self._waitForInput():
                    if self._readRequest():
                        self._executeSub()
        except Exception as err:
            self.logErr("Exit at error:" + str(err))
            self._closeFIFO()


class ClientInstance(ServiceSkeleton):
    """ client side implementation, to invoke the server/daemon to do work """
    def __init__(self, name):
        super(ClientInstance, self).__init__(name)
        
    def _writeRequest(self):
        """ write the pipe to wake up the daemon to do the work """
        if self.fifo == None:
            self._openWrite()
            
        try:
            os.write(self.fifo, self.name)
        except OSError as err:
            self.logErr("Write error at:" + str(err))
            raise

    def _openWrite(self):
        """ open pipe for write """
        try:
            self.fifo = os.open(self.pipePath, os.O_WRONLY)
        except OSError as err:
            self.logErr("open file write error:" + str(err))
            raise
            
    def work(self):
        """ simply write the request and end ourselves """
        self.logInfo("client started")
        
        try:
            self._openWrite()
            self._writeRequest()
        except Exception as err:
            self.logErr("error at:" + str(err))
            raise
        finally:
            self._closeFIFO()
            
def parseArguments(args):
    parser = argparse.ArgumentParser()
    argServ = parser.add_argument("-s", "--service", required=True)
    argMode = parser.add_argument("-m", "--mode", choices=['client', 'server'], required=True)
    argExec = parser.add_argument("-e", "--executable", required=False)
    args = parser.parse_args(args)
    
    if args.mode == 'server':
        # more check for server side, for the script parameter and whether the file exists.
        if not args.executable or args.executable == "":
            logger.error("missing parameter --executable")
            raise argparse.ArgumentError(argExec, "missing parameter --executable when mode is 'server'")
        else:
            subargs = args.executable.split(' ')
            if not os.path.isfile(subargs[0]):
                logger.error("script file not found")
                raise argparse.ArgumentError(argExec, "script to execute is not found")

    return args

if __name__ == "__main__":
    logger.logToSyslog(level=logging.DEBUG)
    
    args = parseArguments(sys.argv[1:])
    service = args.service
    mode = args.mode
    executable = args.executable
    
    try:
        if mode == 'client':
            inst = ClientInstance(service)
        else: # server
            inst = ServerInstance(service, executable)
    except Exception as err:
        logger.error("Unable to initialize '" + service + "' service, mode:" + mode + " err:" + str(err))
        rasie
    
    inst.work()
