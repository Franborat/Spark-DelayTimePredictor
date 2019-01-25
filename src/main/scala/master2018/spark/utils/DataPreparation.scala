package master2018.spark.utils

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

class DataPreparation {
  private val logger = LogManager.getLogger("org")

  private val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    .config("some option", "value")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  def prepare (data: Dataset[_]): Dataset[_] = {
    logger.info("Selecting the variables that are going to be used")
    // Remove the forbidden variables, except diverted (which has to be used for filtering diverted flights)
    val forbidden = Seq ( "ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "CarrierDelay",
      "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
    logger.info(s"Removing the forbidden variables: ${forbidden.mkString(",")}")
    var set = data.drop(forbidden: _*)
    // Filter out cancelled and diverted flights
    set = set.filter($"Diverted" === 0)
    set = set.filter($"Cancelled" === 0).drop("Cancelled", "CancellationCode")
    // Now remove "Diverted" variable, and the other variables we consider are useless for ML training.
    val removed = Seq ("Diverted", "FlightNum", "TailNum", "UniqueCarrier", "Origin", "Dest", "DepTime", "CRSDepTime")
    set = set.drop(removed: _*)

    // Now it is moment to transform the variables from this data set to understandable ones, extracting meaning.
    set.withColumn("FormattedYear", date_format($"Year", "yyyy"))
    set.withColumn("FormattedMonth", date_format($"Month", "MM"))
    set.withColumn("FormattedDay", date_format($"Day", "dd"))
    val dates = Seq ("Year", "Month", "Day")
    set = set.drop(dates: _*)
    set.withColumnRenamed("FormattedYear", "Year")
    set.withColumnRenamed("FormattedMonth", "Month")
    set.withColumnRenamed("FormattedDay", "Day")
    // Finally, we create new variables that could be helpful to create a better model: WeekDay and CRSArrDate.
    val aux = set.col("CRSArrTime")
    val crs = (aux/100) + ":" + (aux%100)
    set.withColumn("CRSArrDate", date_format(crs, format = "hh:mm"))
    val x = set.col("DayOfWeek").getField("DayOfWeek")
    if (x.toString() == "1") {
      set.col("WeekDay") === "Monday"
    } else if (x.toString() == "2") {
      set.col("WeekDay") === "Tuesday"
    } else if (x.toString() == "3") {
      set.col("WeekDay") === "Wednesday"
    } else if (x.toString() == "4") {
      set.col("WeekDay") === "Thursday"
    } else if (x.toString() == "5") {
      set.col("WeekDay") === "Friday"
    } else if (x.toString() == "6") {
      set.col("WeekDay") === "Saturday"
    } else {
      set.col("WeekDay") === "Sunday"
    }
    val old = Seq ("DayOfWeek", "CRSArrTime")
    set = set.drop(old: _*)
    set
  }
}