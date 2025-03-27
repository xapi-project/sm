# Compatibility layer providing sm-core-libs methods in terms of sm-libs
# as much as possible
import re

from sm.core import iscsi
from sm.core import util
from sm.core import f_exceptions

# Import specific functions to satisfy callers using this name
from sm.core.iscsi import parse_IP_port, discovery, get_iscsi_interfaces

# COMPATIBILITY FUNCTIONS BELOW
# =============================
# Instead of importing these functions, provide methods which match the
# sm-core-libs interface (which is different, but with the same function names)

def login(portal, target, username, password, uuid, rescan=False):
    """This is a slightly revised version of iscsi.login

    We are trying to stick as close as possible to the original one but fixing
    or improving whatever can be improved, in order to later fix it to the
    original one and start using that one both for the transport code and
    the old SM code.

    For example, 'uuid' is a new addition in order to be able to automatically
    refcount in here, instead of relying on the caller to do that.
    """

    if username != "" and password != "":
        iscsi.set_chap_settings(portal, target, username, password, "", "")


    refcount = 0
    legacy_refcount = 0

    portal_target_ref = iscsi.get_portal_target_ref(portal, target)

    # Increment portal/target ref count
    refcount = util._incr_iscsiSR_refcount(portal_target_ref, uuid)

    if refcount == 1:
        cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", target, "-l"]
        try:
            failuremessage = "Failed to login to target."
            (stdout, stderr) = iscsi.exn_on_failure(cmd, failuremessage)
            # Increment legacy refcounter
            if uuid:
                legacy_refcount = util._incr_iscsiSR_refcount(target, uuid)
        except Exception as exc:
            util.SMlog("Failed: {}".format(" ".join(cmd)),
                       ident="Transport")
            # Rollback refcount if needed
            if uuid:
                # This should be in its own try/block and be chained
                # with main exception below
                util._decr_iscsiSR_refcount(portal_target_ref, uuid)
            raise f_exceptions.XenError('ISCSILogin', reason=str(exc))

    # Rescan if requested and more than one refcount of either form
    if rescan and (refcount > 1 or legacy_refcount > 1):
        util.SMlog("Session already logged in for {}, rescan requested".
                   format(target))
        iscsi.rescan_target(portal, target)

def logout(portal, target, uuid):
    """Modified version of iscsi.py:logout to handle refcounting

    Given the nature of this refcounting, it is not possible to specifically
    logout from one ip/iqn pair without breaking the refcounting.
    For this reason, this version does not accept a specific ip and the
    parameter 'all'.
    """
    util.SMlog("decrease logout refcount of {} ({})".
               format(target, uuid),
               ident="Transport")

    portal_target_ref = iscsi.get_portal_target_ref(portal, target)

    if util._decr_iscsiSR_refcount(portal_target_ref, uuid):
        # still logged in
        return

    # Decrement the legacy refcount
    if not util._decr_iscsiSR_refcount(target, uuid):
        try:
            cmd = ["iscsiadm", "-m", "node", "-T", target, "-u"]
            failuremessage = "Failed to log out of target"
            util.SMlog("logging out {} for {}".
                       format(target, uuid),
                       ident="Transport")
            (_, _) = iscsi.exn_on_failure(cmd, failuremessage)
        except Exception as exc:
            raise f_exceptions.XenError('ISCSILogout', reason=str(exc))
