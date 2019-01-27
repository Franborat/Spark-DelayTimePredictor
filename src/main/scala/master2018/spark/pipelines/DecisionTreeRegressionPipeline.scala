package master2018.spark.pipelines

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.tuning.ParamGridBuilder

class DecisionTreeRegressionPipeline extends ParametersTuningPipeline {
  override def getEstimatorAndParams: (DecisionTreeRegressor, Array[ParamMap]) = {

    val dt = new DecisionTreeRegressor()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxDepth, Array(5, 10))
      .build()

    (dt, paramGrid)
  }
}
