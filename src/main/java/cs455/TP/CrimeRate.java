package cs455.TP;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
public class CrimeRate {

  JavaSparkContext sc;

    public CrimeRate(JavaSparkContext sc){
      this.sc = sc;
    }

  public JavaPairRDD<String, String> getCrimeData(){
      
    //directory and file has to exist in hdfs on hadoop cluster
    //gisData entry: (State, County Name, GISJOIN ID)
    JavaPairRDD<String, String> gisData = sc.textFile("/TP/data/cleaned_meta_data.csv")
      .map(line -> line.split(","))
      .mapToPair(s -> new Tuple2<String , String>(s[2] +","+ s[3], s[0]));

    //read in each line from data/crime_data.csv file and remove header 
    //crimeData enrtry contains entire line of data/crime_data.csv file
    JavaRDD<String> tempCrimeData = sc.textFile("/TP/data/crime_data.csv");
    String header = tempCrimeData.first();
    tempCrimeData = tempCrimeData.filter((String row) -> {
      return !row.equals(header);
    });

    //extract state, county, and crime rate fields from tempCrimeData line
    //entry of crimeData: (State, County Name, Crime Rate)
    JavaPairRDD<String, String> crimeData = tempCrimeData
      .map(line -> line.split(","))
      .mapToPair(s -> new Tuple2<String , String>(s[1].substring(1, s[1].length()-1) + "," + s[0].substring(1) , s[2]));

    //Join gisData and crimeData by County Name and extract GISJOIN and Crime Rate Score
    //entry of county_crime: (GISJOIN, crimeRate)
    JavaPairRDD<String, String> county_crime = crimeData
      .join(gisData)
      .mapToPair(f -> new Tuple2<>(f._2._2, f._2._1)
    );
  
    return county_crime;
  }    
}
