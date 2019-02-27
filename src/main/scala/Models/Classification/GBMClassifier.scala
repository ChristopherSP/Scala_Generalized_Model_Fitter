package Models.Classification

//Import Project classes

//Import Spark packages
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.PipelineModel
import com.semantix.aijusProd.VariablesYAML._
// Constructs an object to apply gradient boosting model for a classification problem
class GBMClassifier extends Classification {

  override def train(): PipelineModel = {
    val model = new GBTClassifier()
        .setLabelCol(labelIndexerCol)
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
