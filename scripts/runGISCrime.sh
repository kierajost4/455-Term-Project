
$HADOOP_HOME/bin/hadoop fs -rm -r /TP/output/crimeRate

$SPARK_HOME/bin/spark-submit --class cs455.TP.GISJOINcrime build/libs/TP-0.1.0.jar
#$HADOOP_HOME/bin/hadoop fs -cat /TP/output/crimeRate/*