#!/usr/bin/python
#
# Copyright (c) 2010 Citrix Systems, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by Citrix Systems, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of Citrix Systems, Inc. in the United States and/or other countries.
#

"""Manual Xen Certification script"""

from optparse import OptionParser
import StorageHandler
from StorageHandlerUtil import Print

storage_type = "storage type (lvmoiscsi, lvmohba, nfs, isl)"

# argument format:
#  keyword
#  text
#  white space
#  default value
#  short form of option
#  log form of option
__nfs_args__ = [
    ["server",          "server name/IP addr", " : ", None,        "required", "-n", ""   ],
    ["serverpath",      "exported path", " : ", None,        "required", "-e", ""     ] ]

__lvmohba_args__ = [
    ["adapters",       "comma separated list of HBAs to test against", " : ", None,        "optional", "-a", ""   ] ]

__isl_args__ = [
    ["file",       "configuration file describing target array paramters", " : ", None,        "required", "-F", ""   ] ]

__lvmoiscsi__ = [
    ["target",          "comma separated list of Target names/IP addresses", " : ", None,        "required", "-t", ""      ],
    ["targetIQN",       "comma separated list of target IQNs OR \"*\"", " : ", None,        "required", "-q", ""      ],
    ["SCSIid",        "SCSIid to use for SR creation",                  " : ", '',          "optional", "-s", ""    ],
    ["chapuser",        "username for CHAP", " : ", '',        "optional", "-x", ""    ],
    ["chappasswd",      "password for CHAP", " : ", '',        "optional", "-w", ""  ] ]

__common__ = [    
    ["functional", "perform functional tests",                          " : ", None, "optional", "-f", ""],
    ["control", "perform control path tests",                           " : ", None, "optional", "-c", ""],
    ["multipath", "perform multipath configuration verification tests", " : ", None, "optional", "-m", ""],
    ["pool", "perform pool verification tests",                         " : ", None, "optional", "-o", ""],
    ["data", "perform data verification tests",                         " : ", None, "optional", "-d", ""],
    ["metadata", "perform metadata tests",                              " : ", None, "optional", "-M", ""],
    ["help",    "show this help message and exit",                                  " : ", None,        "optional", "-h", "" ]]

__commonparams__ = [
    ["storage_type",    storage_type,                     " : ", None, "required", "-b", ""],
    ["pathHandlerUtil", "absolute path to admin provided callout utility which blocks/unblocks a list of paths, path related information should be provided with the -i option below",
                                                                                    " : ", None, "optional", "-u", ""],
    ["pathInfo", "pass-through string used to pass data to the callout utility above, for e.g. login credentials etc. This string is passed as-is to the callout utility. ",
                                                                                    " : ", None, "optional", "-i", ""],
    ["count", "count of iterations to perform in case of multipathing failover testing",
                                                                                    " : ", None, "optional", "-g", ""]]

def parse_args(version_string):
    """Parses the command line arguments"""
    
    opt = OptionParser("usage: %prog [arguments seen below]",
            version=version_string,
           add_help_option=False)
    
    for element in __nfs_args__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])
    
    for element in __lvmohba_args__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])
   
    for element in __isl_args__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])

    for element in __lvmoiscsi__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])
        
    for element in __commonparams__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])
    
    for element in __common__:
        opt.add_option(element[5], element[6],
                       action="store_true",
                       default=element[3],
                       help=element[1],
                       dest=element[0])

    return opt.parse_args()

def store_configuration(g_storage_conf, options):
    """Stores the command line arguments in a class"""

    g_storage_conf["storage_type"] = options.storage_type
    try:
        g_storage_conf["slavehostname"] = options.slavehostname
    except:
        pass

def valid_arguments(options, g_storage_conf):
    """ validate arguments """
    if not options.storage_type in ["lvmohba", "nfs", "lvmoiscsi", "isl"]:
        Print("Error: storage type (lvmohba, nfs, isl or lvmoiscsi) is required")
        return 0

    for element in __commonparams__:
        if not getattr(options, element[0]):
            if element[4] == "required":
                Print("Error: %s argument (%s: %s) for storage type %s" \
                       % (element[4], element[5], element[1], options.storage_type))
                return 0
            else:
                g_storage_conf[element[0]] = "" 
        value = getattr(options, element[0])
        g_storage_conf[element[0]] = value

    if options.storage_type == "nfs":
        subargs = __nfs_args__
    elif options.storage_type == "lvmohba":
        subargs = __lvmohba_args__
    elif options.storage_type == "isl":
        subargs = __isl_args__
    elif options.storage_type == "lvmoiscsi":
        subargs = __lvmoiscsi__

    for element in subargs:
        if not getattr(options, element[0]):
            if element[4] == "required":
                Print("Error: %s argument (%s: %s) for storage type %s" \
                       % (element[4], element[5], element[1], options.storage_type))
                DisplayUsage(options.storage_type)
                return 0
            else:
                g_storage_conf[element[0]] = "" 
        value = getattr(options, element[0])
        g_storage_conf[element[0]] = value
        
    return 1

def GetStorageHandler(g_storage_conf):
    # Factory method to instantiate the correct handler
    if g_storage_conf["storage_type"] == "lvmoiscsi":
        return StorageHandler.StorageHandlerISCSI(g_storage_conf)
    
    if g_storage_conf["storage_type"] == "lvmohba":
        return StorageHandler.StorageHandlerHBA(g_storage_conf)
        
    if g_storage_conf["storage_type"] == "nfs":
        return StorageHandler.StorageHandlerNFS(g_storage_conf)
    
    if g_storage_conf["storage_type"] == "isl":
        return StorageHandler.StorageHandlerISL(g_storage_conf)

    return None

def DisplayCommonOptions():
    Print("usage: XenCert [arguments seen below] \n\
\n\
Common options:\n")
    for item in __common__:
        printHelpItem(item)
    
def DisplayiSCSIOptions():
    Print(" Storage type lvmoiscsi:\n")
    for item in __lvmoiscsi__:
        printHelpItem(item)
 
def DisplayNfsOptions():
    Print(" Storage type nfs:\n")
    for item in __nfs_args__:
        printHelpItem(item)
  
def DisplayHBAOptions():
    Print(" Storage type lvmohba:\n")
    for item in __lvmohba_args__:
        printHelpItem(item)    

def DisplayiSLOptions():
    Print(" Storage type isl:\n")
    for item in __isl_args__:
        printHelpItem(item)    
  
def DisplayTestSpecificOptions():
    Print("Test specific options:")
    Print("Multipathing test options (-m above):\n")
    for item in __commonparams__:
        printHelpItem(item)

def DisplayStorageSpecificUsage(storage_type):
    if storage_type == 'lvmoiscsi':
        DisplayiSCSIOptions()
    elif storage_type == 'nfs':
        DisplayNfsOptions()
    elif storage_type == 'lvmohba':
        DisplayHBAOptions()
    elif storage_type == 'isl':
        DisplayiSLOptions()
    elif storage_type == None:
        DisplayiSCSIOptions()
        Print("")
        DisplayNfsOptions()
        Print("")
        DisplayHBAOptions()        
        Print("")
        DisplayiSLOptions()        
     
def DisplayUsage(storage_type = None):
    DisplayCommonOptions();
    Print("\nStorage specific options:\n")
    DisplayStorageSpecificUsage(storage_type)
    Print("")
    DisplayTestSpecificOptions();

def printHelpItem(item):
    Print(" %s %-20s\t[%s] %s" % (item[5], item[0], item[4], item[1]))
    
