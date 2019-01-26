package master2018.spark.pipelines

import master2018.spark.App.logger
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.Model
import org.apache.spark.ml.tuning.CrossValidatorModel


abstract class ParametersTuningPipeline {
  
  protected def getEstimatorAndParams: (PipelineStage, Array[ParamMap])
  
  val (estimator, paramGrid) = getEstimatorAndParams
  
  // Method bestParamsModel: Returns the best model after doing the cross validation gridSearch
  def bestParamsModel(training: Dataset[_]): CrossValidatorModel = {

    //Prepare the assembler that will transform the attributes to a feature vector for the ML algorithms
    val assembler = new VectorAssembler()
      .setInputCols(Array("Year", "Month", "DayofMonth", "DayOfWeek","CRSArrMinutes", "CRSElapsedTime", "DepDelay", "Distance", "TaxiOut"))
      .setOutputCol("features")

    
    val pipeline = new Pipeline()
      .setStages(Array(assembler, estimator))
  
    // Define CrossValidator
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator()
        .setLabelCol("ArrDelay")
        .setPredictionCol("prediction")
        .setMetricName("r2"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(training)

    cvModel
    
  }

  def compareModelsMetricsAndSelectBest() (bestLrModel: CrossValidatorModel, bestRfModel: CrossValidatorModel): (PipelineModel) = {

    val rfRSquare = bestRfModel.avgMetrics.max

    def bestRfEstimatorParamMap: ParamMap = {
      bestRfModel.getEstimatorParamMaps
        .zip(bestRfModel.avgMetrics)
        .maxBy(_._2)
        ._1
    }

    println(s"The best Random Forest model is $bestRfEstimatorParamMap and its determination coefficient is $rfRSquare")


    val lrRSquare = bestLrModel.avgMetrics.max

      def bestLrEstimatorParamMap: ParamMap = {
        bestLrModel.getEstimatorParamMaps
          .zip(bestLrModel.avgMetrics)
          .maxBy(_._2)
          ._1

      }

    println(s"The best Linear Regression model is $bestLrEstimatorParamMap and its determination coefficient is $lrRSquare")

    var modelSelected: CrossValidatorModel = bestLrModel
    if (lrRSquare < rfRSquare) {
      modelSelected = bestRfModel
    }

    modelSelected.bestModel.asInstanceOf[PipelineModel]

  }
  
  //Method compareModelMetrics: Shows the best model metrics and score
  def showModelMetrics(cvModel: CrossValidatorModel, cvModel2: CrossValidatorModel): Unit = {
    
    val avgMetrics = cvModel.avgMetrics
    // val avgMetrics2 = cvModel.avgMetrics

    cvModel.getEstimatorParamMaps.zip(avgMetrics).zipWithIndex.foreach {
      case ((params, metric), index) =>
        print(s"Model ${index + 1}:\n$params -> value = $metric")
    }
}

}