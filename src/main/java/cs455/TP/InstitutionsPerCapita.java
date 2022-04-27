package cs455.TP;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class InstitutionsPerCapita {
  JavaSparkContext sc;
  JavaPairRDD<String, Integer> population;

  public InstitutionsPerCapita(JavaSparkContext sc, int minIncome, int maxIncome){
    this.sc = sc;

    // build rdd with population for each county
    JavaRDD<String> populationFile = sc.textFile("/TP/data/county_population.csv");
    population = populationFile
        .mapToPair(s -> new Tuple2<>(s.split(",")[0].replace("\"", ""), Integer.parseInt(s.split(",")[s.split(",").length - 4])));

    // build rdd with income for each county
    JavaRDD<String> incomeFile = sc.textFile("/TP/data/income.csv");
    JavaPairRDD<String, Integer> income = incomeFile
        .mapToPair(s -> new Tuple2<>(s.split(",")[0], Integer.parseInt(s.split(",")[5])));

    // filter income
    income = income.filter(e -> (e._2 >= minIncome && e._2 <= maxIncome));

    // filter population to only include counties within filtered income
    population = population.join(income).mapToPair(e -> new Tuple2<>(e._1, e._2._1));
  }
  
  public JavaPairRDD<String, String> getInsitutionPerCapita(String institutionsFile){

    // calc num institutions per county
    JavaRDD<String> institutionFile = sc.textFile("/TP/data/" + institutionsFile);
    JavaPairRDD<String, Integer> counts = institutionFile
      .map(s -> s.split(",")[0])
      .mapToPair(word -> new Tuple2<>(word, 1))
      .reduceByKey((a, b) -> a + b);

    // calc institutions per capita for each county
    JavaPairRDD<String, String> perCapita = counts
        .join(population)
        .mapToPair(f -> new Tuple2<>(f._1, String.valueOf((double)f._2._1 / (double)f._2._2)));

    return perCapita;
  }
  
}
