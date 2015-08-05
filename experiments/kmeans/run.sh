#!/bin/bash

cd /home/bbdc/flink/

HDFS_KMEANS_POINTS=/kmeans/points
HDFS_KMEANS_CENTERS=/kmeans/centers

TIME=`date +%Y_%m_%d_%H_%M`
LOG_FOLDER=/data/bbdc/dstat/${TIME}

mkdir -p $LOG_FOLDER

/home/bbdc/repos/adaptive-iterations/experiments/start-dstat-monitoring.sh
time bin/flink run ./examples/flink-java-examples-0.9.0-KMeans.jar hdfs://$HDFS_KMEANS_POINTS hdfs://$HDFS_KMEANS_CENTERS hdfs:///kmeans/output_of_run_$RANDOM 5 >> $LOG_FOLDER/flink_jobmanager.log
/home/bbdc/repos/adaptive-iterations/experiments/stop-dstat-monitoring.sh $LOG_FOLDER
