#!/bin/bash

# flink tmp folders
pssh -h /home/bbdc/repos/adaptive-iterations/wally-conf/slaves "mkdir /data/bbdc/flink/"
pssh -h /home/bbdc/repos/adaptive-iterations/wally-conf/slaves "mkdir /data/bbdc/flink/tmp"
