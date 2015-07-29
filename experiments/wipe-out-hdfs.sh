#!/bin/bash

/home/bbdc/hadoop-2.7.0/sbin/stop-dfs.sh

pssh -h /home/bbdc/repos/adaptive-iterations/wally-conf/slaves "rm -rf /data/bbdc/hdfs"
pssh -h /home/bbdc/repos/adaptive-iterations/wally-conf/slaves "mkdir /data/bbdc/hdfs"

/home/bbdc/hadoop-2.7.0/bin/hadoop namenode -format
