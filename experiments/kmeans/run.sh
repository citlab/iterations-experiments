#!/bin/bash

cd /home/bbdc/flink/

HDFS_KMEANS_POINTS=/kmeans/points
HDFS_KMEANS_CENTERS=/kmeans/centers

/home/bbdc/repos/adaptive-iterations/experiments/start-dstat-monitoring.sh
time bin/flink run ./examples/flink-java-examples-0.9.0-KMeans.jar hdfs://$HDFS_KMEANS_POINTS hdfs://$HDFS_KMEANS_CENTERS hdfs:///kmeans/output_of_run_$RANDOM 5
/home/bbdc/repos/adaptive-iterations/experiments/stop-dstat-monitoring.sh
