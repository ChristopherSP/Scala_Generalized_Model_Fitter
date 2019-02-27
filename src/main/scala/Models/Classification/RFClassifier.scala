package Models.Classification

//Import Project classes
import aijusProd.Variables._

//Import Spark packages
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.PipelineModel

// Constructs an object to apply random forest model for a classification problem
class RFClassifier extends Classification {

  override def train(): PipelineModel = {
    val model = new RandomForestClassifier()
        .setLabelCol(labelIndexerCol)
        .setFeaturesCol(featureCol)
        .setRawPredictionCol(rawPredictionCol)
        .setProbabilityCol(probabilityVecCol)
        .setPredictionCol(predictCol)
        .setNumTrees(ntrees)
        .setMaxDepth(max_depth)
        .setSeed(seed)
        .setMaxBins(maxBins)
        .setMinInfoGain(minInfoGain)

    initialPipeline(model)
  }
}
