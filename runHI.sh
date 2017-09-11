#!/bin/bash

hdfs dfs -rm -r -f $2

javac -classpath $HADOOP_CLASSPATH -sourcepath src src/HadoopDriver.java
jar cf lib/hdpindex.jar -C src .

hadoop jar lib/hdpindex.jar HadoopDriver -libjars lib/jackson-core-2.8.5.jar,lib/jackson-databind-2.8.5.jar,lib/jackson-annotations-2.8.5.jar,lib/org.json.jar $1 $2

hdfs dfs -get $2/tweetii hadoopoutput/

javac -classpath $HADOOP_CLASSPATH -sourcepath src src/TableDriver.java
java -classpath $HADOOP_CLASSPATH TableDriver
