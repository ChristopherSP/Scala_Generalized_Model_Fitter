package PipeObjects

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import Models.Classification.Classification
import Models.ModelGenerator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import scala.reflect.io.Directory
import com.semantix.aijusProd.VariablesYAML._
class JudgmentPropensity(override val uid: String) extends Transformer with MLWritable {
  var trainedModel: PipelineModel = _

  def this() = this(Identifiable.randomUID("idModel1"))

  def copy(extra: ParamMap): JudgmentPropensity = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def transform(df: Dataset[_]): DataFrame = {
    //    Model Constructor
    val model = ModelGenerator.getMethod(methodMod1)
      .setDF(df.toDF())
      .setCategoricalVariables(categoricalVariables)
      .setNumericalVariables(numericalVariables)
      .setIndependentVariable(independentVariableMod1 + inputedSuffix + indexSuffix)
      .setOutputColumn(outputMod1)
      .setProbabilityColumn(probMod1)
      .setConstants()
      .setSets(independentVariableMod1,labelsJudgmentModel)
      .asInstanceOf[Classification]
      .setProbLabel(probLabelJudgmentModel)
      .setTransformers()

    //    Fitting the above model
    trainedModel = model.train()

    //    Appling model to the complete dataset and also to training and test sets
    val output = trainedModel.transform(model.data)
    val outputTraingSet = trainedModel.transform(model.trainingSet)
    val outputTestSet = trainedModel.transform(model.testSet)

    //    Create a JSON like string to export the needed information for the automated report made in R.
    val initialJson = "{\"id\": \"" + idModel1 + "\" ," +
      "\"type\": \"Classification\"" + "," +
      "\"method\": \"" + methodMod1 + "\" ,"

    //    Adds training and test set to the JSON string. Metrics, presented as comment lines, were also calculated but not
    // used, once its better to do it direct in R.
    val finalJson = model.performance(model, outputTraingSet, outputTestSet, initialJson)

    //    Gets the time that the code runned and generates a folder with the date in order to save a historic log of
    // models and performances.
    val date = Calendar.getInstance().getTime
    val dateFormat = new SimpleDateFormat("YYYY-MM-dd")
    val dateFormated = dateFormat.format(date)

    Directory(outputPerformanceDir + dateFormated).createDirectory()

    new PrintWriter(outputPerformanceDir + dateFormated + "/" + idModel1.replace(" ","_") + "_" + methodMod1 + "" +
      ".json") {
      write(finalJson)
      close()
    }
    output
  }

  override def write: MLWriter = trainedModel.write
}
