#!/bin/bash

# Paths
HADOOP_PATH="/home/bbdc/hadoop-2.7.0"
KMEANS_HDFS_PATH="/kmeans"

# Remove kmeans 
$HADOOP_PATH/bin/hdfs dfs -rm $KMEANS_HDFS_PATH/points $KMEANS_HDFS_PATH/centers
