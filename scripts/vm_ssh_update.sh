#!/bin/bash

# Script on client that is sent to SSH machine. This is then run on the SSH machine.
# Ensure relevant directories exist
NETID="$1"
LOGURL="$2"
LOGNAME="$3"
ID="$4"

TOPDIR="/home/${NETID}/ece428"

[ ! -d "$TOPDIR" ] && mkdir -p "$TOPDIR"

cd "$TOPDIR"

if [ -d "ece428_mp1" ] 
then 
    cd ece428_mp1
    echo "ece428_mp1 directory found! Performing git pull!"
    git pull --ff-only
else
    echo "ece428_mp1 directory not found! Performing git clone!"
    git clone git@gitlab.engr.illinois.edu:tvitkin2/ece428_mp1.git
fi

cd "$TOPDIR"

[ -d "${TOPDIR}/build" ] && rm -r build
mkdir build
cd build
echo "Working directory: $(pwd)"
cmake ../ece428_mp1
make

[ ! -d "${TOPDIR}/ece428_mp1/logs" ] && mkdir "${TOPDIR}/ece428_mp1/logs"
cd "${TOPDIR}/ece428_mp1/logs"
wget --no-verbose --no-check-certificate -O "$LOGNAME" "$LOGURL"
echo "Downloaded log ${LOGNAME} from ${LOGURL} !"

cd "${TOPDIR}/ece428_mp1/scripts"

# load id env variable
export ID=$ID

echo "Done update for VM!"
