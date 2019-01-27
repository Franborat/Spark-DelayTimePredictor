package master2018.spark

/**
 * @author ${user.name}
 */

import master2018.spark.pipelines._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession

import master2018.spark.pipelines.LinearRegressionPipeline
import master2018.spark.utils.DataPreparation

import scala.collection.mutable.HashMap

object App {

  private val logger: Logger = LogManager.getLogger("org")


  def main(args : Array[String]) {


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

    DataPreparation.explore(data)

    // Get the Data prepared and split it in training and test
    val Array(train, test) = DataPreparation.prepare(data).randomSplit(Array(0.7, 0.3))

    // Get the best Linear Regression model
    val bestLrModel = new LinearRegressionPipeline().bestParamsModel(train)

    // Get the best Random Forest model
    val bestRfModel = new RandomForestPipeline().bestParamsModel(train)
    // val bestGlrModel = new GeneralizedLinearRegressionPipeline().bestParamsModel(train)
    // val bestDtModel = new DecisionTreeRegressionPipeline().bestParamsModel(train)
    // val bestGBTModel = new GradientBoostedTreeRegressionPipeline().bestParamsModel(train)

    // Compare the best models of each algorithm and get the overall best Model according to its RSquare
    val modelSelected = new LinearRegressionPipeline().compareModelsMetricsAndSelectBest()(bestLrModel, bestRfModel)


    val bestTest = modelSelected.transform(test)

    val regEval = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    val result = regEval.evaluate(bestTest)
    logger.info(s"The best model has a determination coefficient equal to $result")
    println(s"The best model has a determination coefficient equal to $result")

}

}
