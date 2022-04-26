package cs455.TP;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
public class GISJOINcrime {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("Join GIS data to crime stats");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //directory and file has to exist in hdfs on hadoop cluster

        JavaPairRDD<String, String> gisData = sc.textFile("/TP/data/cleaned_meta_data.csv")
          .map(line -> line.split(","))
          .mapToPair(s -> new Tuple2<String , String>(s[2] +","+ s[3], s[0]));

        JavaRDD<String> crimeData = sc.textFile("/TP/data/crime_data.csv");
        String header = crimeData.first();
        crimeData = crimeData.filter((String row) -> {
          return !row.equals(header);
        });

        JavaPairRDD<String, String> crimeData2 = crimeData
          .map(line -> line.split(","))
          .mapToPair(s -> new Tuple2<String , String>(s[1].substring(1, s[1].length()-1) + "," + s[0].substring(1) , s[2]));

        JavaRDD<String> joined = crimeData2.join(gisData)
          .map(x ->"("+x._2._2 + "," + x._2._1+")");

        // System.out.println("##################################");
        // System.out.println(gisData.first().toString());
        // System.out.println(crimeData2.first().toString());
        joined.saveAsTextFile("/TP/output/crimeRate");
        sc.close();
    
      }    
}
