echo "Data Join"
gradle build

$HADOOP_HOME/bin/hadoop fs -rm -r /TP/corrData

$SPARK_HOME/bin/spark-submit --class cs455.TP.DataJoin build/libs/TP-0.1.0.jar