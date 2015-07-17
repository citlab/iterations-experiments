#!/bin/bash

#Paths
GENERATOR_JAR="/home/lauritz/repos/adaptive-iterations/experiments/kmeans/flink-java-examples-0.9-SNAPSHOT-KMeans.jar"
HADOOP_PATH="/home/lauritz/hadoop-2.7.0"

#Kmean variables
points=40000000
centers=5

#Kmean generate into /tmp/ folder 
echo "Generate kmeans data"
java -cp $GENERATOR_JAR org.apache.flink.examples.java.clustering.util.KMeansDataGenerator $points $centers

$HADOOP_PATH/bin/hdfs dfs -mkdir /kmeans

echo "Put kmeans data into hdfs"
$HADOOP_PATH/bin/hdfs dfs -put -f /tmp/points /kmeans/
$HADOOP_PATH/bin/hdfs dfs -put -f /tmp/centers /kmeans/

rm /tmp/points
rm /tmp/centers
