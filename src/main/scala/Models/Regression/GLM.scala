package Models.Regression

//Import Project classes
import aijusProd.Variables._

//Import Spark packages
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.PipelineModel

// Constructs an object to apply generalized linear model for a regression problem
class GLM extends Regression {

  override def train(): PipelineModel = {

    val model = new GeneralizedLinearRegression()
        .setLabelCol(independentScaledCol)
        .setFeaturesCol(featureCol)
        .setPredictionCol(predictCol)
        .setFamily(family)
        .setLink(link)
        .setMaxIter(maxIter)

    initialPipeline(model)
  }
/*
  override def performance(model: PipelineModel, df: DataFrame): Unit = {
    val regressionMeasures = List("mae", "mse", "rmse", "r2")

    val regressionValues = regressionMeasures.map(measure =>
      (measure, new RegressionEvaluator()
        .setLabelCol("labelIndexed")
        .setPredictionCol("predictedIndexed")
        .setMetricName(measure)
        .evaluate(df)
      )
    ).toDF("name","value")

    regressionValues.show()

    val correlation = df.stat.corr("labelIndexed", "predictedIndexed")

    println("Pearson's Correlation: " + correlation)

    val glmModel = model.stages(2).asInstanceOf[GeneralizedLinearRegressionModel]

    val summary = glmModel.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals().show()
  }*/
}
