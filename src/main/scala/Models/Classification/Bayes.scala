package Models.Classification

//Import Project classes
import com.autoML.VariablesYAML._
//Import Spark packages
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.PipelineModel

// Constructs an object to apply naive bayes model for a classification problem
class Bayes extends Classification {

  override def train(): PipelineModel = {
    val model = new NaiveBayes()
      .setLabelCol(labelIndexerCol)
      .setFeaturesCol(featureCol)
      .setRawPredictionCol(rawPredictionCol)
      .setProbabilityCol(probabilityVecCol)
      .setPredictionCol(predictCol)

    initialPipeline(model)
  }
}
