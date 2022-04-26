package cs455.TP;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Aggregate {

  JavaSparkContext sc;
  CrimeRate cr;
  InstitutionsPerCapita ipc;

  public Aggregate(JavaSparkContext sc, CrimeRate cr, InstitutionsPerCapita ipc){
    this.sc = sc;
    this.cr = cr;
    this.ipc = ipc;
  }

  //aggregate insitution and crime data 
  public void AggregateData(String[] institution_csvs){

    JavaPairRDD<String,String> careCenters = sc.textFile("/TP/output/careCenters/*")
      .map(x -> x.substring(1, x.length()-1).split(","))
      .mapToPair(f -> new Tuple2<String, String>(f[0], f[1]));
    JavaPairRDD<String,String> crimeRate = sc.textFile("/TP/output/crimeRate/*")
      .map(x -> x.substring(1, x.length()-1).split(","))
      .mapToPair(f -> new Tuple2<String, String>(f[0], f[1]));
    JavaPairRDD<String,String> hospitals = sc.textFile("/TP/output/hospitals/*")
      .map(x -> x.substring(1, x.length()-1).split(","))
      .mapToPair(f -> new Tuple2<String, String>(f[0], f[1]));
    JavaPairRDD<String,String> localLaw = sc.textFile("/TP/output/localLaw/*")
      .map(x -> x.substring(1, x.length()-1).split(","))
      .mapToPair(f -> new Tuple2<String, String>(f[0], f[1]));
    JavaPairRDD<String,String> privateSchools = sc.textFile("/TP/output/privateSchools/*")
      .map(x -> x.substring(1, x.length()-1).split(","))
      .mapToPair(f -> new Tuple2<String, String>(f[0], f[1]));
    JavaPairRDD<String,String> publicSchools = sc.textFile("/TP/output/publicSchools/*")
      .map(x -> x.substring(1, x.length()-1).split(","))
      .mapToPair(f -> new Tuple2<String, String>(f[0], f[1]));
    JavaPairRDD<String,String> worship = sc.textFile("/TP/output/worship/*")
      .map(x -> x.substring(1, x.length()-1).split(","))
      .mapToPair(f -> new Tuple2<String, String>(f[0], f[1]));

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
    
    //JavaPairRDD<String,Iterable<String>> allFiles =  sc.textFile(filePaths)
      // .map(x -> x.substring(1, x.length()-1).split(","))
      // .mapToPair(f -> new Tuple2<String, String>(f[0], f[1]))
      // .groupByKey();
  
    JavaRDD<String> out = joined.map(z -> z._2);

    out.saveAsTextFile("/TP/corrData/");
    sc.close();
  }
}
