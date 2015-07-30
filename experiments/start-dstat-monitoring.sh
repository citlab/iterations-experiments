#!/bin/bash

HOSTS=/home/bbdc/repos/adaptive-iterations/wally-conf/slaves

pssh -h $HOSTS "mkdir -p /data/bbdc/dstat/"

pssh -h $HOSTS "/home/bbdc/repos/adaptive-iterations/experiments/dstat-start.sh"

