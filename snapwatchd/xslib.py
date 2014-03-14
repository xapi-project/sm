import sys
import xen.lowlevel.xs

def get_xs_handle():
    return xen.lowlevel.xs.xs()

def dirlist(h, base):
    return h.ls('', base)

def getval(h, path):
    return h.read('', path)

def setval(h, path, value):
    try:
        if h.write('', path, value) == None:
            return True
        else:
            return False
    except:
        return False

def xs_exists(h, path):
    try:
        if getval(h, path) != None:
            return True
        else:
            return False
    except:
        return False

def remove_xs_entry(h, dom_uuid, dom_path):
    path = "/vss/%s/%s" %(dom_uuid, dom_path)
    if xs_exists(h, path):
        try:
            h.rm('', path)
        except:
            raise "Unable to remove xenstore-node"
    else:
        raise "Invalid dom and path specified"

def set_watch(h, path):
    return h.watch(path, '')

def unwatch(h, path):
    return h.unwatch(path, '')

