package cs455.TP;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.linalg.Matrix;

public class GenCorr {
    public static void main(String[] args) {
        // String institutionsFile = args[0];
        // String outputName = args[1];
        String filePath = "/TP/corrData/*";
        //String filePaths = "/TP/output/careCenters/*,/TP/output/hospitals/*,/TP/output/crimeRate/*";
        SparkConf sparkConf = new SparkConf().setAppName("Aggregate all the data together");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
       

        JavaRDD<String> filtered =  sc.textFile(filePath);
        JavaRDD<Vector> data  = filtered
            .map(line ->{
                String[] parts = line.split(",");
                double[] v = new double[parts.length - 1];
                for (int i = 0; i < parts.length - 1; i++) {
                    v[i] = Double.parseDouble(parts[i]);
                }
                return Vectors.dense(v);
            });
          
        Matrix correlMatrix = Statistics.corr(data.rdd(), "pearson");
        //System.out.println("Correlation Matrix:");
        System.out.println(correlMatrix.toString());
        
        System.out.println(Arrays.toString(correlMatrix.toArray()));
        String b = Arrays.toString(correlMatrix.toArray());
        JavaRDD<String> mat = sc.parallelize(Arrays.asList(b));
      

        MultivariateStatisticalSummary summary = Statistics.colStats(data.rdd());
        String[] stats = new String[3];
        stats[0] = "Summary Mean: " + summary.mean();
        stats[1] ="Summary Variance: " + summary.variance();
        stats[2] ="Summary Non-zero: " + summary.numNonzeros();
        JavaRDD<String> s = sc.parallelize(Arrays.asList(stats));
            
        
        mat.saveAsTextFile("/TP/corrMat/");
        s.saveAsTextFile("/TP/stats/");
        sc.close();
      }
}
