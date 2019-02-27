package Models.Classification

//Import Project classes
import aijusProd.Variables._

//Import Spark packages
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.PipelineModel

// Constructs an object to apply logistic regression model for a classification problem
class Logistic extends Classification {

  override def train(): PipelineModel = {
    val model = new LogisticRegression()
      .setLabelCol(labelIndexerCol)
      .setFeaturesCol(featureCol)
      .setRawPredictionCol(rawPredictionCol)
      .setProbabilityCol(probabilityVecCol)
      .setPredictionCol(predictCol)
      .setFitIntercept(true)

    initialPipeline(model)
  }

/*
  override def performance(model: PipelineModel, df: DataFrame): Unit = {
//    val rfModel = model.stages(2).asInstanceOf[RandomForestRegressionModel]
    println("Coef Matrix \n")
    model.stages(2).asInstanceOf[LogisticRegressionModel].coefficientMatrix.toArray.foreach(println(_))
    println("Intercept \n")
    model.stages(2).asInstanceOf[LogisticRegressionModel].interceptVector.toArray.foreach(println(_))
  }*/
}
