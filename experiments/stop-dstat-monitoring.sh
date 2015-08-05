#!/bin/bash

HOSTS=/home/bbdc/repos/adaptive-iterations/wally-conf/slaves

if [ -z "$1" ]; then
    LOG_FOLDER="$1"
else
    TIME=`date +%Y_%m_%d_%H_%M`
    LOG_FOLDER=/data/bbdc/dstat/${TIME}
    mkdir -p $LOG_FOLDER
fi

pssh -h $HOSTS "/home/bbdc/repos/adaptive-iterations/experiments/dstat-stop.sh"

#log analyser
array=()

# Read the file in parameter and fill the array named "array"
getArray() {
    i=0
    while read line # Read a line
    do
        array[i]=$line # Put it into the array
        i=$(($i + 1))
    done < $1
}

getArray $HOSTS

for host in "${array[@]}"
        do 
                scp ${host}:/data/bbdc/dstat/stats.csv $LOG_FOLDER
                mv $LOG_FOLDER/stats.csv $LOG_FOLDER/stats-${host}.csv
        done

echo "dstat log files copied to $LOG_FOLDER"

#clean up
pssh -h $HOSTS "rm /data/bbdc/dstat/stats.csv"
