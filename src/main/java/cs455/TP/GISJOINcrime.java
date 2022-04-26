package cs455.TP;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
public class GISJOINcrime {

    public JavaPairRDD<String,Double> gisjoinCrime(String csv, SparkConf sparkConf, JavaSparkContext sc){

        //directory and file has to exist in hdfs on hadoop cluster
        JavaPairRDD<String, String> gisData = sc.textFile("/TP/cleaned_meta_data.csv")
          .map(line -> line.split(","))
          .mapToPair(s -> new Tuple2<String , String>(s[2] +","+ s[3], s[0]));

        JavaRDD<String> crimeData = sc.textFile("/TP/crime_data.csv");
        String header = crimeData.first();
        crimeData = crimeData.filter((String row) -> {
          return !row.equals(header);
        });

        JavaPairRDD<String, String> crimeDataSplit = crimeData
          .map(line -> line.split(","))
          .mapToPair(s -> new Tuple2<String , String>(s[1].substring(1, s[1].length()-1) + "," + s[0].substring(1) , s[2]));

        JavaPairRDD<String,Double> county_crime = null;
          try {
          county_crime = crimeDataSplit
            .join(gisData)
            .mapToPair(f -> new Tuple2<>(f._2._2, Double.parseDouble(f._2._1))
          );

          return county_crime;

        } catch(Exception e){
          System.err.println("could not parse score to double");
        }

         return county_crime;
      }    
}
