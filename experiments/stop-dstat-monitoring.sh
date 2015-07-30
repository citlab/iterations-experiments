#!/bin/bash

HOSTS=/home/bbdc/repos/adaptive-iterations/wally-conf/slaves

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

TIME=`date +%Y_%m_%d_%H_%M`
LOG_FOLDER=/data/bbdc/dstat/${TIME}
mkdir $LOG_FOLDER

for host in "${array[@]}"
        do 
                scp ${host}:/data/bbdc/dstat/stats.csv $LOG_FOLDER
                mv $LOG_FOLDER/stats.csv $LOG_FOLDER/stats-${host}.csv
        done

echo "dstat log files copied to $LOG_FOLDER"

#clean up
pssh -h $HOSTS "rm /data/bbdc/dstat/stats.csv"
