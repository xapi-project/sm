#! /usr/bin/env python
import sys, os
import glob

DEVBYMPPPATH = "/dev/disk/by-mpp"
def is_RdacLun(scsi_id):
    path = os.path.join(DEVBYMPPPATH,"%s" % scsi_id)
    mpppath = glob.glob(path)
    if len(mpppath):
        return True
    else:
        return False

def usage():
    print "Usage:";
    print "%s is_rdaclun <scsi_id>" % sys.argv[0]

def main():
    if len(sys.argv) < 3:
        usage()
        sys.exit(-1)

    scsi_id = sys.argv[2]
    mode = sys.argv[1]

    if mode == "is_rdaclun":
        if (is_RdacLun(scsi_id)):
            print "It is a RDAC Lun"
            return True
        else:
            print "It is not a RDAC Lun"
            return False
    else:
        usage()
        sys.exit(-1)
if __name__ == "__main__":
    main()
