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

    logger.info("Selecting the variables that are going to be used")

    // Remove the forbidden variables, except diverted (which has to be used for filtering diverted flights)
    val forbidden = Seq ( "ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "CarrierDelay",
      "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")

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

  def preprocess(dataset: Dataset[_]): Dataset[_] = {


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
  }


}