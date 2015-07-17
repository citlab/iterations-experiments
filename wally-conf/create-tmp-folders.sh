#!/bin/bash

# flink tmp folders
pssh -h /home/lauritz/repos/adaptive-iterations/wally-conf/slaves "mkdir /data/lauritz/flink/"
pssh -h /home/lauritz/repos/adaptive-iterations/wally-conf/slaves "mkdir /data/lauritz/flink/tmp"
