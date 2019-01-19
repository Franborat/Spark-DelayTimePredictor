package master2018.spark.pipelines

// A pipelineStage can rather by a transformer or an estimator
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder


class LinearRegressionPipeline extends ParametersTuningPipeline {
  
    override def getEstimatorAndParams: (LinearRegression, Array[ParamMap]) = {

    val lr = new LinearRegression()
      .setLabelCol("valor")
      .setPredictionCol("valor")
      .setMaxIter(10)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.5, 0.1, 0.01))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    (lr, paramGrid)
}
  
}