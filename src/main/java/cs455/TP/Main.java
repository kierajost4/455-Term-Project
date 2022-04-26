package cs455.TP;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Main {

  public static void main(String[] args){

    SparkConf sparkConf = new SparkConf().setAppName("Institutions Per Capita");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    String[] institution_csvs = {
      "child_care_centers.csv", "hospitals.csv", 
      "local_law_enforcement_locations.csv", 
      "places_of_worship.csv", 
      "private_schools.csv", "public_schools.csv"
    };

    InstitutionsPerCapita ipc = new InstitutionsPerCapita();

    JavaPairRDD<String, Double> childCarePerCapita  = ipc.getInstitutionsPerCapita(institution_csvs[0], sparkConf, sc);
    JavaPairRDD<String, Double> lawEnforcementPerCapita2 = ipc.getInstitutionsPerCapita(institution_csvs[1], sparkConf, sc);

    JavaPairRDD<String, Tuple2<Double, Double>> percapita =  childCarePerCapita
        .join(lawEnforcementPerCapita2);


    percapita.saveAsTextFile("/TP/output");
    

    // for(int i = 0; i < institution_csvs.length; i++){
    //   JavaPairRDD<String, Double> perCapita 
    //     = ipc.getInstitutionsPerCapita(institution_csvs[i], sparkConf, sc);
    // }
  }
  
}
