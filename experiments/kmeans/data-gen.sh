#!/bin/bash

#Paths
JARS="/home/bbdc/flink/lib/flink-dist-0.9.0.jar:/home/bbdc/flink/examples/flink-java-examples-0.9.0-KMeans.jar"
DATASETS_DIR=/data/bbdc/datasets

#Kmean variables
points=800000000
centers=10
skew=

#Kmean generate into /tmp/ folder 
echo "Generate kmeans data"
java -cp $JARS org.apache.flink.examples.java.clustering.util.KMeansDataGenerator -points $points -k $centers -skew $skew -output $DATASETS_DIR/kmeans
