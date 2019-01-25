package master2018.spark

/**
 * @author ${user.name}
 */

import master2018.spark.pipelines.LinearRegressionPipeline
import master2018.spark.utils.DataPreparation
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, LogManager, Logger}

object App {
  
  def main(args : Array[String]) {

    val logger = LogManager.getLogger("org")

    //Reduce verbosity
    logger.setLevel(Level.WARN)
    
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
      .withColumn("CRSDepTime", $"CRSDepTime".cast("int"))
      .withColumn("ArrTime", $"ArrTime".cast("int"))
      .withColumn("CRSArrTime", $"CRSArrTime".cast("int"))
      .withColumn("UniqueCarrier", $"UniqueCarrier".cast("string"))
      .withColumn("FlightNum", $"FlightNum".cast("int"))
      .withColumn("TailNum", $"TailNum".cast("int"))
      .withColumn("ActualElapsedTime", $"ActualElapsedTime".cast("int"))
      .withColumn("CRSElapsedTime", $"CRSElapsedTime".cast("int"))
      .withColumn("AirTime", $"AirTime".cast("int"))
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))
      .withColumn("DepDelay", $"DepDelay".cast("int"))
      .withColumn("Origin", $"Origin".cast("string"))
      .withColumn("Dest", $"Dest".cast("string"))
      .withColumn("Distance", $"Distance".cast("int"))
      .withColumn("TaxiIn", $"TaxiIn".cast("int"))
      .withColumn("TaxiOut", $"TaxiOut".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("boolean"))
      .withColumn("CancellationCode", $"CancellationCode".cast("string"))
      .withColumn("Diverted", $"Diverted".cast("boolean"))
      .withColumn("CarrierDelay", $"CarrierDelay".cast("int"))
      .withColumn("WeatherDelay", $"WeatherDelay".cast("int"))
      .withColumn("NASDelay", $"NASDelay".cast("int"))
      .withColumn("SecurityDelay", $"SecurityDelay".cast("int"))
      .withColumn("LateAircraftDelay", $"LateAircraftDelay".cast("int"))

    // Get the Data prepared and split it in training and test
    val Array(train, test) = DataPreparation.select(data).randomSplit(Array(0.7, 0.3))

    // Get the best Linear Regression model
    val bestLrModel = new LinearRegressionPipeline().bestParamsModel(train)

    logger.info(s"The best Linear Regression model is: $bestLrModel" )

}

}
