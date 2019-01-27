package master2018.spark.pipelines

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder

class GeneralizedLinearRegressionPipeline extends ParametersTuningPipeline {
  override def getEstimatorAndParams: (GeneralizedLinearRegression, Array[ParamMap]) = {

    val glr = new GeneralizedLinearRegression()
      .setFamily("Gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")

    val paramGrid = new ParamGridBuilder()
      .addGrid(glr.regParam, Array(0.5, 0.1, 0.01))
      .build()

    (glr, paramGrid)
  }

}
