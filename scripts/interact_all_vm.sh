#!/bin/bash

# Loops through all VMs, by automating SSH into it. 
# Also posible to automate commands

read -p 'netID (tvitkin2 or zhuxuan2): ' NETID

if [ "$NETID" != "tvitkin2" ] && [ "$NETID" != "zhuxuan2" ]
then
    echo "Invalid netID specified! Terminating!"
    return 1 2> /dev/null || exit 1
fi

VMSTRL="f\"${NETID}@fa22-cs425-25{"
VMSTRR=':02}.cs.illinois.edu"'

for (( i=1; i<=10; i++ )) 
do
    VMSTR="${VMSTRL}${i}${VMSTRR}"
    PYSTR="print(${VMSTR})"
    STR=$(echo "$PYSTR" | python3) 
    ssh -i ~/.ssh/cs425 $STR "echo \"PASSWORD HERE\" | sudo -S yum install python39 -y"
    ssh -i ~/.ssh/cs425 $STR "python3.9 -m pip install rpyc" &
done
