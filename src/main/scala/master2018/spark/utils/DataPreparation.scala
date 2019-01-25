package master2018.spark.utils

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object DataPreparation {
  
  val logger = LogManager.getLogger("org")
  
  val spark = SparkSession
                .builder()
                .appName("Spark SQL")
                .config("some option", "value")
                .enableHiveSupport()
                .getOrCreate()
                
   import spark.implicits._         
   def select (data: Dataset[_]): Dataset[_] = {
    
    logger.info("Selecting the variables that are going to be used")
    
    // Remove the forbidden variables, except diverted (which has to be used for filtering diverted flights)
    val forbidden = Seq ( "ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn","CarrierDelay",
        "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
    logger.info(s"Removing the forbidden variables: ${forbidden.mkString(",")}")
    var dataset = data.drop(forbidden: _*)
    
    // Filter out cancelled and diverted flights
    dataset = dataset.filter($"Diverted" === 0)
    dataset = dataset.filter($"Cancelled" === 0).drop("Cancelled", "CancellationCode")
    
    // Now remove "Diverted" variable.
    dataset = data.drop("Diverted")
    
    
    
    
    dataset
}
  
}