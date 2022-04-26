gradle build
$HADOOP_HOME/bin/hadoop fs -rm -r /TP
$HADOOP_HOME/bin/hadoop fs -mkdir /TP
$HADOOP_HOME/bin/hadoop fs -put iris.data /TP
$HADOOP_HOME/bin/hadoop fs -rm -r /TP/output
$SPARK_HOME/bin/spark-submit --class cs455.TP.Iris build/libs/TP-0.1.0.jar
$HADOOP_HOME/bin/hadoop fs -cat /TP/output/part-00002