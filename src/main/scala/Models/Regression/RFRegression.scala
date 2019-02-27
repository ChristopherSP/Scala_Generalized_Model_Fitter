package Models.Regression

//Import Project classes
import aijusProd.Variables._

//Import Spark packages
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.PipelineModel

// Constructs an object to apply random forest model for a regression problem
class RFRegression extends Regression{

  override def train(): PipelineModel = {

    val model = new RandomForestRegressor()
      .setLabelCol(independentVariable)
      .setFeaturesCol(featureCol)
      .setPredictionCol(predictCol)
      .setNumTrees(ntrees)
      .setMaxDepth(max_depth)
      .setSeed(seed)
      .setMaxBins(maxBins)
      .setMinInfoGain(minInfoGain)

    initialPipeline(model)
  }
}
