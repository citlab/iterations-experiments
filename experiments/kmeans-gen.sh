#!/bin/bash

#Paths
GENERATOR_JAR="/home/lauritz/adaptive-iterations/experiments/flink-java-examples-0.9-SNAPSHOT-KMeans.jar"
HADOOP_PATH="/home/lauritz/hadoop-2.7.0"

#Kmean variables
points=10000
centers=5

#Kmean generate into /tmp/ folder 
echo "Generate kmeans data"
java -cp $GENERATOR_JAR org.apache.flink.examples.java.clustering.util.KMeansDataGenerator $points $centers

echo "$i: put kmeans data into hdfs"
$HADOOP_PATH/bin/hdfs dfs -put /tmp/points /kmeans/
$HADOOP_PATH/bin/hdfs dfs -put /tmp/centers /kmeans/

rm /tmp/points
rm /tmp/centers
