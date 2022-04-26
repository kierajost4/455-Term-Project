package cs455.TP;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class GenCorr {
    public static void main(String[] args) {
        // String institutionsFile = args[0];
        // String outputName = args[1];
        String filePath = "/TP/corrData/*";
        //String filePaths = "/TP/output/careCenters/*,/TP/output/hospitals/*,/TP/output/crimeRate/*";
        SparkConf sparkConf = new SparkConf().setAppName("Aggregate all the data together");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
       

        JavaRDD<String> filtered =  sc.textFile(filePath)
            .map(x -> x.split(","))
            .filter(f -> f.length == 8)
            .map(z -> Arrays.toString(z));
          
      

        filtered.saveAsTextFile("/TP/corrMat/");
        sc.close();
      }
}
