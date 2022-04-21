package cs455.TP;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class WordCount {
  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setAppName("Word Count");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    //directory and file has to exist in hdfs on hadoop cluster
    JavaRDD<String> textFile = sc.textFile("/TP/testdata.txt");
    JavaPairRDD<String, Integer> counts = textFile
      .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
      .mapToPair(word -> new Tuple2<>(word, 1))
      .reduceByKey((a, b) -> a + b);
      //file outputs to hdfs
     counts.saveAsTextFile("/TP/output"); 

  }
  
}
