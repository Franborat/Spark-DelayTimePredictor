package master2018.spark

/**
 * @author ${user.name}
 */
import utils.DataPreparation
import pipelines._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator

import scala.collection.mutable.HashMap

object App {

  private val logger = LogManager.getLogger("org")


  def main(args : Array[String]) {

    import spark.implicits._

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
    
    // Get input data paths from the args we send by console
    val inFilePath = args(0)

    val data = spark.read.format("csv").option("header", value = true).csv(inFilePath)
      .withColumn("Year", $"Year".cast("date"))
      .withColumn("Month", $"Month".cast("date"))
      .withColumn("DayofMonth", $"DayofMonth".cast("date"))
      .withColumn("DayOfWeek", $"DayOfWeek".cast("int"))
      .withColumn("DepTime", $"DepTime".cast("int"))
      .withColumn("CRSDepTime", $"CRSDepTime".cast("int"))
      .withColumn("ArrTime", $"ArrTime".cast("int"))
      .withColumn("CRSArrTime", $"CRSArrTime".cast("int"))
      .withColumn("UniqueCarrier", $"UniqueCarrier".cast("String"))
      .withColumn("FlightNum", $"FlightNum".cast("int"))
      .withColumn("TailNum", $"TailNum".cast("int"))
      .withColumn("ActualElapsedTime", $"ActualElapsedTime".cast("int"))
      .withColumn("CRSElapsedTime", $"CRSElapsedTime".cast("int"))
      .withColumn("AirTime", $"AirTime".cast("int"))
      .withColumn("ArrDelay", $"ArrDelay".cast("int"))
      .withColumn("DepDelay", $"DepDelay".cast("int"))
      .withColumn("Origin", $"Origin".cast("String"))
      .withColumn("Dest", $"Dest".cast("String"))
      .withColumn("Distance", $"Distance".cast("int"))
      .withColumn("TaxiIn", $"TaxiIn".cast("int"))
      .withColumn("TaxiOut", $"TaxiOut".cast("int"))
      .withColumn("Cancelled", $"Cancelled".cast("int"))
      .withColumn("CancellationCode", $"CancellationCode".cast("int"))
      .withColumn("Diverted", $"Diverted".cast("int"))
      .withColumn("CarrierDelay", $"CarrierDelay".cast("int"))
      .withColumn("WeatherDelay", $"WeatherDelay".cast("int"))
      .withColumn("NASDelay", $"NASDelay".cast("int"))
      .withColumn("SecurityDelay", $"SecurityDelay".cast("int"))
      .withColumn("LateAircraftDelay", $"LateAircraftDelay".cast("int"))


    /* DataPreparation.explore(data)

    val Array(training, test) = DataPreparation.prepare(data).randomSplit(Array(0.8, 0.2))
    val (modelLR, lrEval) = new LinearRegressionPipeline().bestParamsModel(training)
    val (modelRF, rfEval) = new RandomForestPipeline().bestParamsModel(training)

    val models : HashMap[PipelineModel, Double] = HashMap(
        (modelLR, lrEval),
        (modelRF, rfEval)
    )

    logger.info("Evaluate best model on the test set")
    val best: PipelineModel = models.minBy(_._2)._1
    val predictions = best.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("Prediction")
      .setMetricName("mae")

    val testEvaluation = evaluator.evaluate(predictions)
    logger.info(s"The value for metric Mean Average Error on test set is $testEvaluation")*/
}

}
