#!/bin/bash

gradle build
$HADOOP_HOME/bin/hadoop fs -rm -r /TP
$HADOOP_HOME/bin/hadoop fs -mkdir /TP
$HADOOP_HOME/bin/hadoop fs -put testdata.txt /TP
$HADOOP_HOME/bin/hadoop fs -rm -r /TP/output
$SPARK_HOME/bin/spark-submit --class cs455.TP.WordCount build/libs/TP-0.1.0.jar
$HADOOP_HOME/bin/hadoop fs -cat /TP/output/part-00002