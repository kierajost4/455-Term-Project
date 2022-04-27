package cs455.TP;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.linalg.Matrix;

public class Correlation {

  JavaSparkContext sc;

  public Correlation(JavaSparkContext sc){
      this.sc = sc;
  }

  public void getCorrelation(JavaRDD<String> data) {
    
    JavaRDD<Vector> filtered  = data
      .map(line ->{
        String[] parts = line.split(",");
        double[] v = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
          v[i] = Double.parseDouble(parts[i]);
        }
        return Vectors.dense(v);
      });
          
    Matrix correlMatrix = Statistics.corr(filtered.rdd(), "pearson");

    String b = Arrays.toString(correlMatrix.toArray());
    JavaRDD<String> mat = sc.parallelize(Arrays.asList(b));
        

    MultivariateStatisticalSummary summary = Statistics.colStats(filtered.rdd());
    String[] stats = new String[3];
    stats[0] = "Summary Mean: " + summary.mean();
    stats[1] ="Summary Variance: " + summary.variance();
    stats[2] ="Summary Non-zero: " + summary.numNonzeros();
    JavaRDD<String> s = sc.parallelize(Arrays.asList(stats));

    mat.saveAsTextFile("/TP/CorrelationMatrix");
    s.saveAsTextFile("/TP/MultivariateStatisticalSummary");
        
  }
}
