#!/bin/bash

#Paths
JARS="/home/bbdc/flink/lib/flink-dist-0.9.0.jar:/home/bbdc/flink/examples/flink-java-examples-0.9.0-KMeans.jar"
HADOOP_PATH="/home/bbdc/hadoop-2.7.0"

#Kmean variables
points=400000000
centers=3
skew="1,1,1"

#Kmean generate into /tmp/ folder 
echo "Generate kmeans data"
java -cp $JARS org.apache.flink.examples.java.clustering.util.KMeansDataGenerator -points $points -k $centers -skew $skew

$HADOOP_PATH/bin/hdfs dfs -mkdir /kmeans

echo "Put kmeans data into hdfs"
$HADOOP_PATH/bin/hdfs dfs -put -f /tmp/points /kmeans/
$HADOOP_PATH/bin/hdfs dfs -put -f /tmp/centers /kmeans/

rm /tmp/points
rm /tmp/centers
