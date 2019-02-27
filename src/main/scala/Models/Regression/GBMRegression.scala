package Models.Regression

//Import Project classes

//Import Spark packages
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.PipelineModel
import com.semantix.aijusProd.VariablesYAML._
// Constructs an object to apply gradient boosting model for a regression problem
class GBMRegression extends Regression {

  override def train(): PipelineModel = {
    val model = new GBTRegressor()
      .setLabelCol(independentScaledCol)
      .setFeaturesCol(featureCol)
      .setPredictionCol(predictCol)
      .setMaxDepth(max_depth)
      .setSeed(seed)
      .setMaxBins(maxBins)
      .setMaxIter(maxIter)
      .setMinInfoGain(minInfoGain)
      .setStepSize(learn_rate)

    initialPipeline(model)
  }
}
