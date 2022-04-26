package cs455.TP;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Iris {

  public static void main(String [] args){
    SparkConf sparkConf = new SparkConf().setAppName("Iris");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    String dataFile = "/TP/iris.data";
    JavaRDD<String> data = sc.textFile(dataFile);

    JavaRDD<Vector> inputData = data.map(line -> {
      String[] parts = line.split(",");
      double[] v = new double[parts.length - 1];
      for (int i = 0; i < parts.length - 1; i++) {
          v[i] = Double.parseDouble(parts[i]);
      }
      return Vectors.dense(v);
    });

    System.out.println("\n\n\n\n\n");
    for(Vector line:inputData.collect()){
      System.out.println("* "+line);
    }
    System.out.println("\n\n\n\n\n");
    Matrix correlMatrix = Statistics.corr(inputData.rdd(), "pearson");
    System.out.println("Correlation Matrix:");
    System.out.println(correlMatrix.toString());

  }

}