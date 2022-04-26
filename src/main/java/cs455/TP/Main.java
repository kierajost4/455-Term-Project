package cs455.TP;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

  public static void main(String[] args){

    SparkConf sparkConf = new SparkConf().setAppName("Institution and Crime Correlation");
     JavaSparkContext sc = new JavaSparkContext(sparkConf);

    CrimeRate cr = new CrimeRate(sc);
    InstitutionsPerCapita ipc = new InstitutionsPerCapita(sc);

    String[] institution_csvs = {
      "child_care_centers.csv", "hospitals.csv", 
      "local_law_enforcement_locations.csv", 
      "places_of_worship.csv", 
      "private_schools.csv", "public_schools.csv"
    };

    //aggregates instituion and crime data
    Aggregate aggregate = new Aggregate(sc , cr, ipc);
    aggregate.AggregateData(institution_csvs);

  }
  
}
