#!/bin/bash

BUS=${1}
NAME=${2}
HOST=${BUS%%:*}
NODE=${3}
OUT=${4}
LUN=`echo $BUS | cut -d: -f4`

# The required files are located in an extra layer of directories in 3.x kernels
kernel=`uname -r | cut -d. -f1-2`
case $kernel in
    2.6)
        targetIQNPath="/device/session*/iscsi_session*"
        targetIPPortPath="/device/session*/connection*/iscsi_connection*"
        ;;
    *)
        targetIQNPath="/device/session*/iscsi_session/session*"
        targetIPPortPath="/device/session*/connection*/iscsi_connection/connection*"
        ;;
esac

if [ $# -gt 5 -o $# -lt 4 ]; then
        echo "Incorrect number of arguments"
        exit 1
fi

if [[ $OUT == "scsi_bybus" && -z ${5} ]]; then
    SCSIID=`/usr/lib/udev/scsi_id --whitelisted --replace-whitespace --device=$NODE`
    if [ $? == 0 ]; then
        echo "disk/by-scsibus/${SCSIID}-${BUS} disk/by-scsid/${SCSIID}/$2"
    fi
    exit
fi

[ -e /sys/class/scsi_host/host${HOST}/proc_name ] || exit 1

driver=$(cat /sys/class/scsi_host/host${HOST}/proc_name)

ISCSIPATH="/sys/class/iscsi_host/host${HOST}"

[ -e ${ISCSIPATH} ] || exit 1

# Query targetIQN
targetIQNfile="${ISCSIPATH}${targetIQNPath}/targetname"
targetIQN=$(cat ${targetIQNfile})

# Query target address and port
targetIPfile="${ISCSIPATH}${targetIPPortPath}/persistent_address"
targetPortfile="${ISCSIPATH}${targetIPPortPath}/persistent_port"
targetIP=$(cat ${targetIPfile})
targetPort=$(cat ${targetPortfile})

basepath="iscsi/${targetIQN}/${targetIP}:${targetPort}"

# Handle the Kernel assigned device number
if [ -z ${5} ]; then
   DEV="LUN${LUN}"
else
   DEV="LUN${LUN}_${5}"
fi

if [ ${OUT} == "base" ]; then
    echo "${basepath}/${DEV}"
elif [ ${OUT} == "session" ]; then
    echo "${basepath}/SESSIONID-${HOST}/${DEV}"
elif [[ ${OUT} == "serial" && -z ${5} ]]; then
    SERIAL=`/opt/xensource/bin/xe-getserialhex ${NODE}`
    if [ $? == 0 ]; then
	echo "${basepath}/SERIAL-${SERIAL}"
    fi
fi
