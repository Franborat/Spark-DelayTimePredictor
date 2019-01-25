package master2018.spark.pipelines

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.CrossValidatorModel


abstract class ParametersTuningPipeline {
  
  protected def getEstimatorAndParams: (PipelineStage, Array[ParamMap])
  
  val (estimator, paramGrid) = getEstimatorAndParams

  
  // Method bestParamsModel: Returns the best model after doing the cross validation gridSearch
  def bestParamsModel(training: Dataset[_]): (CrossValidatorModel) = {

    val assembler = new VectorAssembler()
      .setInputCols(Array("Distance", "TaxiOut", "DepDelay"))
      .setOutputCol("features")
    
  val pipeline = new Pipeline()
      .setStages(Array(assembler, estimator))
  
  // Define CrossValidator
  val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
  
  // Run cross-validation, and choose the best set of parameters.
  val cvModel = cv.fit(training)
  
  cvModel
    
  }
  
  //Method compareModelMetrics: Shows the best model metrics and score
  def showModelMetrics(cvModel: CrossValidatorModel, cvModel2: CrossValidatorModel): Unit = {
    
    val avgMetrics = cvModel.avgMetrics
    val avgMetrics2 = cvModel.avgMetrics

    cvModel.getEstimatorParamMaps.zip(avgMetrics).zipWithIndex.foreach {
      case ((params, metric), index) =>
        print(s"Model ${index + 1}:\n$params -> value = $metric")
    }
}

}