#!/bin/bash

# Script on client that is sent to SSH machine. This is then run on the SSH machine.
# Ensure relevant directories exist
NETID="$1"
ID="$2"

TOPDIR="/home/${NETID}"

[ ! -d "$TOPDIR" ] && mkdir -p "$TOPDIR"

cd "$TOPDIR"

if [ -d "ece428_mp3" ]
then
    cd ece428_mp3
    echo "ece428_mp3 directory found! Performing git pull!"
    git pull origin master
else
    echo "ece428_mp3 directory not found! Performing git clone!"
    git clone git@gitlab.engr.illinois.edu:tvitkin2/ece428_mp3.git
fi

cd "$TOPDIR"


# load id env variable
export ID=$ID

echo "Done update for VM $ID!"
