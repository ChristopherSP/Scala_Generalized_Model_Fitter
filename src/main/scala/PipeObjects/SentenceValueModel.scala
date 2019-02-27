package PipeObjects

//Import Project classes
import Models.ModelGenerator
import aijusProd.Variables._

//Import Java packages
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

//Import Spark packages
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.io.Directory

//This Class construct and train a model that determines how much a law suit will cost. It also applies the fitted
// model transforming a dataset and evaluates its performance.
class SentenceValueModel(override val uid: String) extends Transformer  with MLWritable with
  DefaultParamsReadable[SentenceValueModel]{

  var trainedModel: PipelineModel = _
  var aditionalVariables: List[String] = _

  def this() = this(Identifiable.randomUID(idModel5))

  def copy(extra: ParamMap): SentenceValueModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  def setAddtionalVariables(variables: List[String]): SentenceValueModel = {
    this.aditionalVariables = variables
    this
  }

  override def transform(df: Dataset[_]): DataFrame = {
    //    Model Constructor
    val model = ModelGenerator.getMethod(method = methodMod5)
      .setDF(df.toDF())
      .setCategoricalVariables(categoricalVariables)
      .setNumericalVariables(numericalVariables ++ aditionalVariables)
      .setIndependentVariable(independentVariableMod5 + inputedSuffix + scaledSuffix)
      .setPredictionColumn(outputMod5)
      .setConstants()
      .setSets(independentVariableMod3, labelsRegression)
      .setTransformers()

    //    Fitting the above model
    trainedModel = model.train()

    //    Appling model to the complete dataset and also to training and test sets
    val output = trainedModel.transform(model.data)
    val outputTraingSet = trainedModel.transform(model.trainingSet)
    val outputTestSet = trainedModel.transform(model.testSet)

    //    Create a JSON like string to export the needed information for the automated report made in R.
    val initialJson = "{\"id\": \"" + idModel5 + "\" ," +
      "\"type\": \"Regression\"" + "," +
      "\"method\": \"" + methodMod5 + "\" ,"

    //    Adds training and test set to the JSON string. Metrics, presented as comment lines, were also calculated but not
    // used, once its better to do it direct in R.
    val finalJson = model.performance(model,outputTraingSet,outputTestSet, initialJson)

    //    Gets the time that the code runned and generates a folder with the date in order to save a historic log of
    // models and performances.
    val date = Calendar.getInstance().getTime
    val dateFormat = new SimpleDateFormat("YYYY-MM-dd")
    val dateFormated = dateFormat.format(date)

    Directory(outputPerformanceDir + dateFormated).createDirectory()

    new PrintWriter(outputPerformanceDir + dateFormated + "/" + idModel5.replace(" ","_") + "_" + methodMod5 + "" +
      ".json") {
      write(finalJson)
      close()
    }

    output
  }

  override def write: MLWriter = trainedModel.write

}
