#!/bin/bash

pids=`ps axuf | grep python | grep dstat | awk '{print $2}'`

if [ -n "$pids" ]
then
	kill -9 $pids 2>/dev/null >/dev/null
fi
