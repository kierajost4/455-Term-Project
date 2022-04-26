echo "Generate the Correlation Matrix"
gradle build

$HADOOP_HOME/bin/hadoop fs -rm -r /TP/corrMat

$SPARK_HOME/bin/spark-submit --class cs455.TP.GenCorr build/libs/TP-0.1.0.jar
