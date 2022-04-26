package cs455.TP;

import java.util.Arrays;

import org.apache.directory.shared.kerberos.codec.types.HostAddrType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

public class DataJoin {
    public static void main(String[] args) {
        // String institutionsFile = args[0];
        // String outputName = args[1];
        String filePaths = "/TP/output/*/*";
        //String filePaths = "/TP/output/careCenters/*,/TP/output/hospitals/*,/TP/output/crimeRate/*";
        SparkConf sparkConf = new SparkConf().setAppName("Aggregate all the data together");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
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

        JavaPairRDD<String, String> joined = careCenters.join(crimeRate)
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
      
        JavaRDD<String> out = joined.map(z -> z._1 + "," + z._2);

        out.saveAsTextFile("/TP/corrData/");
        sc.close();
      }
}
