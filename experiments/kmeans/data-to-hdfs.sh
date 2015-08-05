#!/bin/bash

HADOOP_PATH="/home/bbdc/hadoop-2.7.0"

$HADOOP_PATH/bin/hdfs dfs -mkdir /kmeans

echo "Put kmeans data into hdfs"
$HADOOP_PATH/bin/hdfs dfs -put -f /data/bbdc/datasets/kmeans/10gb_points_in_10_clusters /kmeans/points
$HADOOP_PATH/bin/hdfs dfs -put -f /data/bbdc/datasets/kmeans/10_centers /kmeans/centers
