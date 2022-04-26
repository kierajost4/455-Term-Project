package cs455.TP;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;
public class Aggregate {

  CrimeRate cr;
  InstitutionsPerCapita ipc;

  public Aggregate( CrimeRate cr, InstitutionsPerCapita ipc){
    this.cr = cr;
    this.ipc = ipc;
  }

  //aggregate insitution and crime data 
  public JavaRDD<String> AggregateData(){

    JavaPairRDD<String,String> crimeRate = cr.getCrimeData();
    JavaPairRDD<String,String> careCenters = ipc.getInsitutionPerCapita("child_care_centers.csv");
    JavaPairRDD<String,String> hospitals = ipc.getInsitutionPerCapita("hospitals.csv");
    JavaPairRDD<String,String> localLaw = ipc.getInsitutionPerCapita("local_law_enforcement_locations.csv");
    JavaPairRDD<String,String> privateSchools = ipc.getInsitutionPerCapita( "private_schools.csv");
    JavaPairRDD<String,String> publicSchools = ipc.getInsitutionPerCapita("public_schools.csv");
    JavaPairRDD<String,String> worship = ipc.getInsitutionPerCapita("places_of_worship.csv");

    JavaPairRDD<String, String> joined = crimeRate.join(careCenters)
      .mapToPair(q -> new Tuple2 <String, String>(q._1,q._2._1 + "," + q._2._2))
      .join(hospitals)
      .mapToPair(q -> new Tuple2 <String, String>(q._1,q._2._1 + "," + q._2._2))
      .join(localLaw)
      .mapToPair(q -> new Tuple2 <String, String>(q._1,q._2._1 + "," + q._2._2))
      .join(privateSchools)
      .mapToPair(q -> new Tuple2 <String, String>(q._1,q._2._1 + "," + q._2._2))
      .join(publicSchools)
      .mapToPair(q -> new Tuple2 <String, String>(q._1,q._2._1 + "," + q._2._2))
      .join(worship)
      .mapToPair(q -> new Tuple2 <String, String>(q._1,q._2._1 + "," + q._2._2));
  
    JavaRDD<String> data = joined.map(z -> z._2);

    return data;
  }
}
