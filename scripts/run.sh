#!/bin/bash

if [[ $1 == "clean" ]]; then
  $HADOOP_HOME/bin/hadoop fs -rm -r /TP
  exit 0
fi

if [[ $1 == "add" ]]; then

  $HADOOP_HOME/bin/hadoop fs -mkdir /TP
  $HADOOP_HOME/bin/hadoop fs -mkdir /TP/data
  $HADOOP_HOME/bin/hadoop fs -put data/institutions/child_care_centers.csv /TP/data
  $HADOOP_HOME/bin/hadoop fs -put data/institutions/hospitals.csv /TP/data
  $HADOOP_HOME/bin/hadoop fs -put data/institutions/local_law_enforcement_locations.csv /TP/data
  $HADOOP_HOME/bin/hadoop fs -put data/institutions/places_of_worship.csv /TP/data
  $HADOOP_HOME/bin/hadoop fs -put data/institutions/private_schools.csv /TP/data
  $HADOOP_HOME/bin/hadoop fs -put data/institutions/public_schools.csv /TP/data
  $HADOOP_HOME/bin/hadoop fs -put data/gisjoin_data/cleaned_meta_data.csv /TP/data
  $HADOOP_HOME/bin/hadoop fs -put data/crime_data.csv /TP/data
  exit 0
  
fi

gradle build

$HADOOP_HOME/bin/hadoop fs -rm -r /TP/output
$SPARK_HOME/bin/spark-submit --class cs455.TP.Main build/libs/TP-0.1.0.jar
$HADOOP_HOME/bin/hadoop fs -cat /TP/output/part-00001
$HADOOP_HOME/bin/hadoop fs -cat /TP/output/part-00002