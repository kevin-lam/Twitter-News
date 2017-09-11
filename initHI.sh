#!/bin/sh

hadoop-ssh-keys.sh

start-dfs.sh
start-yarn.sh

export JAVAC_PATH=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.111-1.b15.el7_2.x86_64/lib
export HADOOP_CLASSPATH=$(hadoop classpath):$JAVAC_PATH:src:lib/jackson-databind-2.8.5.jar:lib/jackson-core-2.8.5.jar:lib/jackson-annotations-2.8.5.jar:lib/org.json.jar

hdfs dfs -mkdir hadoop
hdfs dfs -mkdir hadoop/input
hdfs dfs -put input/* hadoop/input
