package master2018.spark.utils

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object DataPreparation {
  private val logger = LogManager.getLogger("org")

  private val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    .config("some option", "value")
    .enableHiveSupport()
    .getOrCreate()

  def explore (data: Dataset[_]): Unit = {

    import spark.implicits._

    logger.info("Data exploration")
    val groupedCarrier = data.groupBy("UniqueCarrier")
    val groupedOrigin = data.groupBy("Origin")
    val groupedDest = data.groupBy("Dest")
    val groupedWeekday = data.groupBy("DayOfWeek")

    logger.info("Total delay by carrier:")
    groupedCarrier.agg(sum($"ArrDelay")).show(12)

    logger.info("Total delay by origin:")
    groupedOrigin.agg(sum($"ArrDelay")).show(12)

    logger.info("Total delay by destination:")
    groupedDest.agg(sum($"ArrDelay")).show(12)

    logger.info("Total delay by day of the week:")
    groupedWeekday.agg(sum($"ArrDelay")).show(7)
  }


  def prepare (data: Dataset[_]): Dataset[_] = {

    import spark.implicits._

    logger.info("Check null values in target column")
    // Check null values in target column. They are not expected so I want to inspect
    val nullValuesDf = data.filter(data("ArrDelay").isNull)
    if (nullValuesDf.count() > 0) {
      logger.warn("We still have null values! Please check why! We already have removed the expected source of nulls.")
      nullValuesDf.show()
      logger.info("Removing remaining null values")
      data.filter(data("ArrDelay").isNotNull)
    }
    else {
      logger.info("No null values in target column")
    }

    logger.info("Selecting the variables that are going to be used")
    // Remove the forbidden variables, except diverted (which has to be used for filtering diverted flights)
    val forbidden = Seq("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "CarrierDelay",
      "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")

    var set = data.drop(forbidden: _*)
    // Filter out cancelled and diverted flights
    set = set.filter($"Diverted" === 0)
    set = set.filter($"Cancelled" === 0).drop("Cancelled", "CancellationCode")

    // Now remove "Diverted" variable, and the other variables we consider are useless for ML training.
    val removed = Seq("Diverted", "FlightNum", "TailNum", "UniqueCarrier", "Origin", "Dest", "DepTime", "CRSDepTime")
    set = set.drop(removed: _*)

    // Now it is moment to transform the variables from this data set to understandable ones, extracting meaning.
    val milesToKm = udf(
      (distance: Int) => {
        val distanceKM = distance*1.60934
        distanceKM
      }
    )
    set = set.withColumn("Distance", milesToKm(data("Distance")))

    /*set.withColumn("FormattedYear", date_format($"Year", "yyyy"))
    set.withColumn("FormattedMonth", date_format($"Month", "MM"))
    set.withColumn("FormattedDay", date_format($"DayofMonth", "dd"))

    val dates = Seq ("Year", "Month", "DayofMonth")
    set = set.drop(dates: _*)
    set.withColumnRenamed("FormattedYear", "Year")
    set.withColumnRenamed("FormattedMonth", "Month")
    set.withColumnRenamed("FormattedDay", "DayofMonth")*/


    // val aux = set.col("CRSArrTime")
    // val crs = (aux/100) + ":" + (aux%100)
    // set.withColumn("CRSArrDate", date_format(crs, format = "hh:mm"))

    /* val dayConverter = udf(
      (weekDay: Int) => {
      val output =
        if (weekDay == 1) {
          "Monday"
      } else if (weekDay == 2) {
          "Tuesday"
      } else if (weekDay == 3) {
          "Wednesday"
      } else if (weekDay == 4) {
          "Thursday"
      } else if (weekDay == 5) {
          "Friday"
      } else if (weekDay == 6) {
          "Saturday"
      } else {
          "Sunday"
      }
        output
    })
    set = set.withColumn("WeekDay", dayConverter(set("DayOfWeek")))
    set = set.drop("DayOfWeek")*/

    // Finally, we create new variables that could be helpful to create a better model: CRSArrMinutes
    // Convert in minutes since midnight, creating a new user defined function.
    val minutesConverter = udf(
      (timeString: String) => {
        val hours =
          if (timeString.length > 2)
            timeString.substring(0, timeString.length - 2).toInt
          else
            0
        hours * 60 + timeString.takeRight(2).toInt
      })

    logger.info("Converting time columns")
    set = set.withColumn("CRSArrMinutes", minutesConverter(data("CRSArrTime")))
    set = set.drop("CRSArrTime")

    set.show(15)
    set
  }

  /*def preprocess(dataset: Dataset[_]): Dataset[_] = {


    val forbiddenVariables = Seq(
      "ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted",
      "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
    logger.info(s"Removing forbidden variables: ${forbiddenVariables.mkString(", ")}")
    var ds = dataset.drop(forbiddenVariables: _*)


    logger.info("Removing cancelled flights")
    ds = ds.filter(ds("Cancelled") === 0).drop("Cancelled", "CancellationCode")

    // Check null values in target column. They are not expected so I want to inspect
    val nullValuesDf = ds.filter(ds("ArrDelay").isNull)
    if (nullValuesDf.count() > 0) {
      logger.warn("We still have null values! Please check why! We already have removed the expected source of nulls.")
      nullValuesDf.show()
      logger.info("Removing remaining null values")
      ds = ds.filter(ds("ArrDelay").isNotNull)
    }
    else {
      logger.info("No null values in target column")
    }

    // Convert in minutes since midnight
    val minutesConverter = udf(
      (timeString: String) => {
        val hours =
          if (timeString.length > 2)
            timeString.substring(0, timeString.length - 2).toInt
          else
            0
        hours * 60 + timeString.takeRight(2).toInt
      })

    logger.info("Converting time columns")
    ds = ds
      .withColumn("CRSDepTimeMin", minutesConverter(ds("CRSDepTime")))
      .withColumn("DepTime", minutesConverter(ds("DepTime")))

    // Adding the route, can be interesting
    logger.info(s"Adding route column")
    ds = ds.select(ds("*"), concat(ds("Origin"), lit("-"), ds("Dest")).as("Route"))

    // We drop columns that we do not think to be worth it
    val toDrop = Array("CRSDepTime", "DepTime", "CRSArrTime", "FlightNum", "TailNum")
    logger.info(s"Dropping non-worthy columns: ${toDrop.mkString(", ")}")
    ds = ds.drop(toDrop: _*)

    logger.info("Final DataFrame:")
    ds.show(15)

    ds
  }*/


}