package cs455.TP;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

import scala.Tuple2;
public class GISJOINcrime {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("Join GIS data to crime stats");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //directory and file has to exist in hdfs on hadoop cluster
        JavaRDD<String> gisData = sc.textFile("/TP/cleaned_meta_data.csv");
        JavaRDD<String> crimeData = sc.textFile("/TP/crime_data.csv");
        String header = crimeData.first();
        crimeData = crimeData.filter((String row) -> {
          return !row.equals(header);
        });

        System.out.println(crimeData.first());

        // JavaPairRDD<String, Integer> counts = textFile
        //   .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        //   .mapToPair(word -> new Tuple2<>(word, 1))
        //   .reduceByKey((a, b) -> a + b);
        //   //file outputs to hdfs
        //  counts.saveAsTextFile("/TP/output");
    
         sc.close();
    
      }
}
