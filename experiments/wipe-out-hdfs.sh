#!/bin/bash

/home/lauritz/hadoop-2.7.0/sbin/stop-dfs.sh

pssh -h /home/lauritz/repos/adaptive-iterations/wally-conf/slaves "rm -rf /data/lauritz/hdfs"
pssh -h /home/lauritz/repos/adaptive-iterations/wally-conf/slaves "mkdir /data/lauritz/hdfs"

/home/lauritz/hadoop-2.7.0/bin/hadoop namenode -format
