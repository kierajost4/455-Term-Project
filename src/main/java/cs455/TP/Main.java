package cs455.TP;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

  public static void main(String[] args){

    SparkConf sparkConf = new SparkConf().setAppName("Institution and Crime Correlation");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    CrimeRate cr = new CrimeRate(sc);
    InstitutionsPerCapita ipc = new InstitutionsPerCapita(sc, Integer.parseInt(args[0]), Integer.parseInt(args[1]));

    //aggregates instituion and crime data
    Aggregate aggregate = new Aggregate(cr, ipc);
    JavaRDD<String> data = aggregate.AggregateData();

    Correlation corr = new Correlation(sc);
    corr.getCorrelation(data);

    sc.close();

  }
  
}
