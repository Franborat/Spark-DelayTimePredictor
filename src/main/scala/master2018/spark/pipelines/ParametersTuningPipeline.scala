package master2018.spark.pipelines

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Dataset


abstract class ParametersTuningPipeline {
  
  protected def getEstimatorAndParams: (PipelineStage, Array[ParamMap])
  
  val (estimator, paramGrid) = getEstimatorAndParams
  
  
  
}