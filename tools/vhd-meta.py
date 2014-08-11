#!/usr/bin/python

# Script for collecting VHD metadata for a given VHD chain (e.g. from a remote 
# site), and for recreating the full VHD files (with zeros written to data 
# blocks) from the metadata (e.g. to reproduce a VHD problem locally)

import sys
import os
import re
import shutil
import logging
import subprocess
import tarfile

META_FILE_EXTN = ".meta"
LOG_FILE = "/var/log/collect_vhd_meta.log"
BUF_SIZE = 10 * 1024
BITMAP_SIZE = 512
INTER_BLOCK_DISTANCE = 2 * 1024 * 1024 + 4 * 1024 # at least for XenServer VHDs

vhd_header = {}

def runCmd(command, with_stdout = False, with_stderr = False, inputtext = None):
    cmd = subprocess.Popen(command, bufsize = 1,
                           stdin = (inputtext and subprocess.PIPE or None),
                           stdout = subprocess.PIPE,
                           stderr = subprocess.PIPE,
                           shell = isinstance(command, str))

    (out, err) = cmd.communicate(inputtext)
    rv = cmd.returncode

    l = "Ran: %s; rc %d" % (str(command), rv)
    if inputtext:
        l += " with input %s" % inputtext
    if out != "":
        l += "\nSTANDARD OUT:\n" + out
    if err != "":
        l += "\nSTANDARD ERROR:\n" + err
    logging.debug(l)

    if with_stdout and with_stderr:
        return rv, out, err
    elif with_stdout:
        return rv, out
    elif with_stderr:
        return rv, err
    return rv

def parse_vhd_headers(fn):
    rv, out = runCmd("vhd-util read -p -n %s" % fn, with_stdout = True)
    if rv != 0:
        raise Exception("reading metadata for %s: %d" % (fn, rv))
    for line in out.split("\n"):
        m = re.match("Current disk size\s*:\s(\d+)", line)
        if m != None:
            set_disk_size(int(m.group(1)))
            continue
        m = re.match("Max BAT size\s*:\s(\d+)", line)
        if m != None:
            set_max_bat_size(int(m.group(1)))
            continue
        m = re.match("Block size\s*:\s(\d+)", line)
        if m != None:
            set_block_size(int(m.group(1)))
            continue

""" Set the disk size (in MB) into the dict"""
def set_disk_size(value):
    global vhd_header
    vhd_header['disk_size'] = value

""" Return virtual disk size in MB"""
def get_disk_size():
    try:
        return vhd_header['disk_size']
    except:
        print "disk_size not in dict"
        raise

""" Set the block size (in B) into the dict"""
def set_block_size(value):
    global vhd_header
    vhd_header['block_size'] = value

""" Return block size in bytes"""
def get_block_size():
    try:
        return vhd_header['block_size']
    except:
        print "block_size not in dict"
        raise

""" Set into the dict the maximum number of BAT entries
    physically allowed before the VHD header needs rearrangement.
"""
def set_max_bat_size(value):
    global vhd_header
    vhd_header['max_BAT_size'] = value

""" Return the max BAT entries.
    See set_max_bat_size for explanations.
"""
def get_max_bat_size():
    try:
        return vhd_header['max_BAT_size']
    except:
        print "BAT_size not in dict"
        raise

""" Set into the dict the maximum number of BAT entries compatible
    with the current virtual VHD size
"""
def set_bat_size(value):
    global vhd_header
    vhd_header['BAT_size'] = value

""" Return the max BAT entries.
    See set_bat_size for explanations.
"""
def get_bat_size():
    if not vhd_header.has_key('BAT_size'):
        max_bat = get_max_bat_size()
        disk_size = get_disk_size()
        block_size = get_block_size() / (1024*1024)
        bat = int(disk_size / block_size)
        if bat != max_bat:
           print "allocated BAT entries (%s) and max (%s) mismatch, "\
                 "using the latter" %(max_bat, bat)
        set_bat_size(bat)
    return vhd_header['BAT_size']
      

def get_data_offset(fn, num_blocks):
    rv, out = runCmd("vhd-util read -b 0 -c %d -n %s" % (num_blocks, fn),
            with_stdout = True)
    if rv != 0:
        raise Exception("reading blocks for %s: %d" % (fn, rv))
    first_block = sys.maxint
    for line in out.split("\n"):
        if not line:
            continue
        m = re.match("block: (\d+): offset: (.*)", line)
        if m == None:
            raise Exception("parsing block offset in %s" % line)
        if m.group(2) != "not allocated":
            off = int(m.group(2))
            if off < first_block:
                first_block = off
    return first_block

def get_parent_fn(fn):
    rv, out = runCmd("vhd-util query -p -n %s" % fn, with_stdout = True)
    if rv != 0:
        raise Exception("querying parent for %s: %d" % (fn, rv))
    parent = out.strip()
    if parent.find("has no parent") != -1:
        return None
    return parent

def get_meta_fn(path):
    fn = os.path.basename(path)
    return fn + META_FILE_EXTN

def collect_meta(fn):
    num_blocks = get_bat_size()
    outfn = get_meta_fn(fn)
    first_block_off = get_data_offset(fn, num_blocks)
    if first_block_off == sys.maxint:
        shutil.copyfile(fn, outfn)
        return outfn

    fsize = os.path.getsize(fn)
    fin = open(fn, 'r')
    fout = open(outfn, 'w') 

    total = first_block_off
    while total > 0:
        chunk = BUF_SIZE
        if chunk > total:
            chunk = total
        fout.write(fin.read(chunk))
        total -= chunk

    off = first_block_off
    while off < fsize:
        fin.seek(off)
        buf = fin.read(BITMAP_SIZE)
        fout.write(buf)
        off += INTER_BLOCK_DISTANCE

    # TODO: get the last 512 bytes (footer) separately for completeness

    fout.close()
    fin.close()
    return outfn

def get_tar_name(fn):
    return "vhd-chain-meta-%s.tgz" % os.path.basename(fn)

def collect_chain_meta(fn):
    leaf = fn
    files = []
    while fn:
        print "Collecting metadata for %s..." % fn
        outfn = collect_meta(fn)
        files.append(outfn)
        fn = get_parent_fn(fn)

    print "Creating a tar archive..."
    tarfn = get_tar_name(leaf)
    tar = tarfile.open(tarfn, "w|gz")
    for name in files:
        tar.add(name)
    tar.close()
    for name in files:
        os.unlink(name)
    print "Created %s" % tarfn

def get_vhd_fn(fn):
    assert(fn.endswith(META_FILE_EXTN))
    return fn[0:len(fn) - len(META_FILE_EXTN)]

def inflate(fn):
    num_blocks = get_bat_size()
    outfn = get_vhd_fn(fn)
    first_block_off = get_data_offset(fn, num_blocks)
    if first_block_off == sys.maxint:
        os.rename(fn, outfn)
        return outfn

    fsize = os.path.getsize(fn)
    fin = open(fn, 'r')
    fout = open(outfn, 'w') 

    total = first_block_off
    while total > 0:
        chunk = BUF_SIZE
        if chunk > total:
            chunk = total
        fout.write(fin.read(chunk))
        total -= chunk

    in_off = first_block_off
    out_off = first_block_off
    while in_off < fsize:
        fout.write(fin.read(BITMAP_SIZE))
        in_off += BITMAP_SIZE
        out_off += INTER_BLOCK_DISTANCE
        fout.seek(out_off)
    fout.close()
    fin.close()
    os.unlink(fn)
    rv = runCmd("vhd-util repair -n %s" % outfn)
    if rv != 0:
        print "WARNING: vhd-util repair on %s failed: %d" % (outfn, rv)
    return outfn

def inflate_chain(tarfn, dest):
    print "Extracting %s..." % tarfn
    tar = tarfile.open(tarfn, "r|gz")
    tar.extractall(path = dest)
    files = tar.getnames()
    for fn in files:
        print "Inflating %s..." % fn
        inflate(os.path.join(dest, fn))
    print "Done"

def usage():
    print "Usage:"
    print "   collect <PATH_TO_VHD>"
    print "   inflate <TAR_ARCHIVE> <DESTINATION_DIR>"
    print "\nThe 'collect' command will collect the VHD metadata for the " + \
            "entire chain and create a tar file containing all of the " + \
            "metadata together (the name of the resulting tar file is " + \
            "printed to standard out.\n" + \
            "The 'inflate' command will extract the metadata " + \
            "information from the tar archive and add zeros in place of " + \
            "real data to recreate the full VHD files.\n"

def main():
    cmd = None
    if len(sys.argv) == 3 and sys.argv[1] == "collect":
        cmd = "collect"
        fn = sys.argv[2]
    elif len(sys.argv) == 4 and sys.argv[1] == "inflate":
        cmd = "inflate"
        tarfn = sys.argv[2]
        dest = sys.argv[3]
    else:
        usage()
        return

    logging.basicConfig(filename=LOG_FILE, level = logging.DEBUG,
            format="%(asctime)s " + cmd + ": %(message)s")

    # Initialize dictionary
    parse_vhd_headers(fn)

    if cmd == "collect":
        collect_chain_meta(fn)
    elif cmd == "inflate":
        inflate_chain(tarfn, dest)


main()
