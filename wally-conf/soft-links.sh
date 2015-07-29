#!/bin/bash

cd /home/bbdc/
rm flink
ln -s /home/bbdc/repos/adaptive-iterations/flink-0.9.0/build-target flink

cd /home/bbdc/hadoop-2.7.0/etc/hadoop
rm slaves core-site.xml hdfs-site.xml hadoop-env.sh
ln -s /home/bbdc/repos/adaptive-iterations/wally-conf/slaves slaves
ln -s /home/bbdc/repos/adaptive-iterations/wally-conf/hdfs/core-site.xml core-site.xml
ln -s /home/bbdc/repos/adaptive-iterations/wally-conf/hdfs/hdfs-site.xml hdfs-site.xml
ln -s /home/bbdc/repos/adaptive-iterations/wally-conf/hdfs/hadoop-env.sh hadoop-env.sh

cd /home/bbdc/flink/conf/
rm flink-conf.yaml slaves
ln -s /home/bbdc/repos/adaptive-iterations/wally-conf/slaves slaves
ln -s /home/bbdc/repos/adaptive-iterations/wally-conf/flink/flink-conf.yaml flink-conf.yaml
