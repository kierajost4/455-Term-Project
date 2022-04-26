#!/bin/bash

$HADOOP_HOME/bin/hadoop fs -rm -r /TP/output/$2

$SPARK_HOME/bin/spark-submit --class cs455.TP.InstitutionsPerCapita build/libs/TP-0.1.0.jar $1 $2
