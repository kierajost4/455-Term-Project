#!/bin/bash

gradle build
$HADOOP_HOME/bin/hadoop fs -rm -r /TP
$HADOOP_HOME/bin/hadoop fs -mkdir /TP
$HADOOP_HOME/bin/hadoop fs -put data/institutions/$1 /TP
$HADOOP_HOME/bin/hadoop fs -put data/county_population.csv /TP
$HADOOP_HOME/bin/hadoop fs -rm -r /TP/output
$SPARK_HOME/bin/spark-submit --class cs455.TP.InstitutionsPerCapita build/libs/TP-0.1.0.jar $1
$HADOOP_HOME/bin/hadoop fs -cat /TP/output/part-00001