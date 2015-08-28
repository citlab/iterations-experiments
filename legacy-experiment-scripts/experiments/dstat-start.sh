#!/bin/bash

dstat --epoch --cpu -C total,0,1,2,3,4,5,6,7 --mem --net -N eth0 --disk --noheaders --nocolor --output /data/bbdc/dstat/stats.csv 1 > /dev/null &
