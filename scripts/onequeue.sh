#!/usr/bin/bash
exec </dev/null &>/dev/null

## This is a script more general than just SM and may eventually want
## to move to another package. Its purpose is to help with running
## a process to act on some event while other events may be arriving.
##
## Invoke as
##
## /etc/udev/scripts/onequeue.sh <lockname> <command> [args...]
##
## The first argument is a name for a lock (not a pathname, just a name).
## This will be used for a wait lock (LOCK.wait) and a run lock (LOCK.run).
## The remaining arguments are the command to run and arguments for that
## command. Every different command/args set which might need to run
## separately will need its own unique <lockname>
##
## onequeue will use the locks to ensure that there is never more than
## one instance of <command> running, plus one instance waiting to run,
## thus rate-limiting the running of <command> in the face of a storm of
## events.
##
## It will further ensure that at least one instance of <command> will be
## run following any given invocation via this mechanism, ensuring that
## (for example in the case of a command which scans for changes), there
## is never a change which gets lost.

## Make somewhere to keep our lockfiles.
LOCKDIR=/run/lock/onequeue
/usr/bin/mkdir -p $LOCKDIR
if [ $? -ne 0 ]; then
    /usr/bin/logger -t onequeue -p daemon.notice "ERROR: Failed to create /run/lock/onequeue"
fi

## Get the name for the lock to use
LOCK="$1"; shift
## Get the remaining arguments as the program to run and its arguments
PROGARGS="$*"

runprog() {
    (
        /usr/bin/logger -t onequeue -p daemon.notice "'$PROGARGS' waiting to run"
        ## Acquire an exclusive lock on fd 10 (LOCK.run). Wait as long as necessary
        /usr/bin/flock -x 10
        if [ $? -ne 0 ]; then
            /usr/bin/logger -t onequeue -p daemon.notice "WARNING: '$PROGARGS' Failed to acquire $LOCK.run"
            exit 1
        fi
        ## Now we have the run lock, drop the wait lock. Note this happens before
        ## we invoke the handler command, which means a new waiter can queue from this
        ## point, ensuring we never miss anything.
        /usr/bin/flock -u 9
        /usr/bin/logger -t onequeue -p daemon.notice "'$PROGARGS' running"
        ## Invoke the handler command
        $PROGARGS
        /usr/bin/logger -t onequeue -p daemon.notice "'$PROGARGS' done"
    ) 10>"$LOCKDIR/$LOCK.run"
}

(
	## Attempt to acquire an exclusive lock on fd 9 (LOCK.wait)
    /usr/bin/flock -x -n 9
    if [ $? -ne 0 ]; then
        ## If we didn't get it, someone is already waiting, so just exit.
        /usr/bin/logger -t onequeue -p daemon.notice "'$PROGARGS' already queued on $LOCK; skipping."
        exit 0
    fi
    ## Start the wait for LOCK.run in the background.
    runprog &
) 9>"$LOCKDIR/$LOCK.wait"

