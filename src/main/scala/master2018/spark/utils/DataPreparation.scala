package master2018.spark.utils

import org.apache.log4j.LogManager
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline

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


    // Origin and Dest HotEncoding
    val indexer1 = new StringIndexer().setInputCol("Origin").setOutputCol("OriginIndex")
    val indexer2 = new StringIndexer().setInputCol("Dest").setOutputCol("DestIndex")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(indexer1.getOutputCol, indexer2.getOutputCol))
      .setOutputCols(Array("OriginVec", "DestVec"))

    val pipeline = new Pipeline().setStages(Array(indexer1, indexer2, encoder))

    set = pipeline.fit(set).transform(set)

    //set = encoder.fit(set).transform(set)

    // Remove "Diverted" variable,
    // We also remove "FlightNum", "TailNum", "UniqueCarrier", "Origin", "Dest", "DepTime", "CRSDepTime",as we consider are useless for ML training.
    val removed = Seq("Diverted", "FlightNum", "TailNum", "UniqueCarrier", "DepTime", "CRSDepTime")
    set = set.drop(removed: _*)

    // Now it is moment to transform the variables from this data set to understandable ones, extracting meaning.
    val milesToKm = udf(
      (distance: Int) => {
        val distanceKM = distance*1.60934
        distanceKM
      }
    )
    set = set.withColumn("Distance", milesToKm(data("Distance")))

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

    //set.write.option("header", "true").format("com.databricks.spark.csv").save("dataFiltered.csv")


    set.show(15)
    set
  }


}