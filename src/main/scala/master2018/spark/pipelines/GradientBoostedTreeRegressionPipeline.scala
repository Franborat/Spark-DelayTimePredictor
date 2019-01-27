package master2018.spark.pipelines

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.ParamGridBuilder

class GradientBoostedTreeRegressionPipeline extends ParametersTuningPipeline {
  override def getEstimatorAndParams: (GBTRegressor, Array[ParamMap]) = {

    val gbt = new GBTRegressor()
      .setMaxIter(10)
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxDepth, Array(5,10))
      .build()

    (gbt, paramGrid)
  }
}
