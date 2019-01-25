package master2018.spark.pipelines

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.ParamGridBuilder

class RandomForestPipeline extends ParametersTuningPipeline {

  override def getEstimatorAndParams: (RandomForestRegressor, Array[ParamMap]) = {

    val rf = new RandomForestRegressor()
      .setLabelCol("ArrDelay")
      .setPredictionCol("Prediction")

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxDepth, Array(5, 8, 10))
      .addGrid(rf.numTrees, Array(24, 34))
      .build()

    (rf, paramGrid)
  }

}
