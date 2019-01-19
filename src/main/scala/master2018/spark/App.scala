package master2018.spark

/**
 * @author ${user.name}
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession


import org.apache.log4j.{Level, Logger}

object App {

  
  def main(args : Array[String]) {
    
    //Reduce verbosity
    Logger.getLogger("org").setLevel(Level.WARN)
    
    // Create the spark configuration and context
    val conf = new SparkConf().setAppName("Spark app for predicting flight delays")
    val sc = new SparkContext(conf)
    
    // To start with  sql, we need a spark session
    val spark = SparkSession
                .builder()
                .appName("Spark SQL")
                .config("some option", "value")
                .enableHiveSupport()
                .getOrCreate()
                
    import spark.implicits._
    
    // Get input data paths from the args we send by console
    val inFilePath = args(0)

    val data = spark.read.format("csv").option("header", value = true).csv(inFilePath)
      .withColumn("Year", $"Year".cast("int"))
      .withColumn("Month", $"Month".cast("int"))
      .withColumn("DayofMonth", $"DayofMonth".cast("int"))
      .withColumn("DayOfWeek", $"DayOfWeek".cast("int"))
      .withColumn("DepTime", $"DepTime".cast("int"))
      .withColumn("CRSElapsedTime", $"CRSElapsedTime".cast("int"))
      .withColumn("CRSArrTime", $"CRSArrTime".cast("int"))
      .withColumn("UniqueCarrier", $"UniqueCarrier".cast("int"))
      .withColumn("FlightNum", $"FlightNum".cast("int"))
      .withColumn("TailNum", $"TailNum".cast("int"))
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))
      .withColumn("DepDelay", $"DepDelay".cast("int"))
      .withColumn("Origin", $"Origin".cast("int"))
      .withColumn("Dest", $"Dest".cast("int"))
      .withColumn("Distance", $"Distance".cast("int"))
      .withColumn("TaxiOut", $"TaxiOut".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("int"))
      .withColumn("CancellationCode", $"CancellationCode".cast("int"))
      .withColumn("Diverted", $"Diverted".cast("int"))
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))
      
      .withColumn("Year", $"Year".cast("int"))
      .withColumn("Month", $"Month".cast("int"))
      .withColumn("DayofMonth", $"DayofMonth".cast("int"))
      .withColumn("DayOfWeek", $"DayOfWeek".cast("int"))
      .withColumn("CRSElapsedTime", $"CRSElapsedTime".cast("int"))
      .withColumn("DepDelay", $"DepDelay".cast("int"))
      .withColumn("Distance", $"Distance".cast("int"))
      .withColumn("TaxiOut", $"TaxiOut".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("int"))
      .withColumn("Diverted", $"Diverted".cast("int"))
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))
    
}

}
