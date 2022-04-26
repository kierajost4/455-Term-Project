package cs455.TP;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class InstitutionsPerCapita {
  public static void main(String[] args) {
    String institutionsFile = args[0];
    String outputFileName = args[1];

    SparkConf sparkConf = new SparkConf().setAppName("Institutions Per Capita");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // calc num institutions per county
    JavaRDD<String> institutionFile = sc.textFile("/TP/data/" + institutionsFile);
    JavaPairRDD<String, Integer> counts = institutionFile
      .map(s -> s.split(",")[0])
      .mapToPair(word -> new Tuple2<>(word, 1))
      .reduceByKey((a, b) -> a + b);

    // build rdd with num institutions and population for each county
    JavaRDD<String> populationFile = sc.textFile("/TP/data/county_population.csv");
    JavaPairRDD<String, Integer> population = populationFile
        .mapToPair(s -> new Tuple2<>(s.split(",")[0].replace("\"", ""), Integer.parseInt(s.split(",")[s.split(",").length - 4])));

    // calc institutions per capita for each county
    JavaPairRDD<String, Double> perCapita = counts
        .join(population)
        .mapToPair(f -> new Tuple2<>(f._1, (double)f._2._1 / (double)f._2._2));

    // save to hdfs
    perCapita.saveAsTextFile("/TP/output/" + outputFileName);

    sc.close();
  }
  
}
