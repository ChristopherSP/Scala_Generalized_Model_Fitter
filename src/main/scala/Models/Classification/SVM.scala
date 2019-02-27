package Models.Classification

//Import Project classes

//Import Spark packages
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.PipelineModel
import com.semantix.aijusProd.VariablesYAML._
// Constructs an object to apply support vector machine model for a classification problem
class SVM extends Classification {

  override def train(): PipelineModel = {
    val model = new LinearSVC()
      .setLabelCol(labelIndexerCol)
      .setFeaturesCol(featureCol)
      .setRawPredictionCol(rawPredictionCol)
      .setPredictionCol(predictCol)
      .setFitIntercept(true)

    initialPipeline(model)
  }

/*
  override def performance(model: PipelineModel, df: DataFrame): Unit = {
    //    val rfModel = model.stages(2).asInstanceOf[RandomForestRegressionModel]
    println("Coef Matrix \n")
    model.stages(2).asInstanceOf[LinearSVCModel].coefficients.toArray.foreach(println(_))
    println("Intercept \n")
    println(model.stages(2).asInstanceOf[LinearSVCModel].intercept)
  }*/
}
