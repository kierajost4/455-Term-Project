package cs455.TP;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class InstitutionsPerCapita {
  JavaSparkContext sc;

  public InstitutionsPerCapita(JavaSparkContext sc){
    this.sc = sc;
  }
  
  public JavaPairRDD<String, String> getInsitutionPerCapita(String institutionsFile){

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
    JavaPairRDD<String, String> perCapita = counts
        .join(population)
        .mapToPair(f -> new Tuple2<>(f._1, String.valueOf((double)f._2._1 / (double)f._2._2)));

    return perCapita;

  }
  
}
