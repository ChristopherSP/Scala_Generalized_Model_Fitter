package PipeObjects

//Import Project classes
import Models.Classification.Classification
import Models.ModelGenerator

//Import Spark packages
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

//Import Spark packages
import scala.reflect.io.Directory
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import com.autoML.VariablesYAML._
//This Class construct and train a model that determines how much a person is prone to accept an agreement in a law
// suit. It also applies the fitted model transforming a dataset and evaluates its performance.
class AgreementPropensityModel(override val uid: String) extends Transformer with MLWritable{

  var trainedModel: PipelineModel = _
  var aditionalVariables: List[String] = _

  def this() = this(Identifiable.randomUID(idModel4))

  def copy(extra: ParamMap): AgreementPropensityModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  def setAddtionalVariables(variables: List[String]): AgreementPropensityModel = {
    this.aditionalVariables = variables
    this
  }

  def transform(df: Dataset[_]): DataFrame = {
    println("####################\n\n####################\nInput model 4\n####################\n####################")
//    Model Constructor
    val model = ModelGenerator.getMethod(method = methodMod4)
      .setDF(df.toDF())
      .setCategoricalVariables(categoricalVariables)
      .setNumericalVariables(numericalVariables ++ aditionalVariables)
      .setIndependentVariable(independentVariableMod4 + inputedSuffix + indexSuffix)
      .setOutputColumn(outputMod4)
      .setProbabilityColumn(probMod4)
      .setConstants()
      .setSets(independentVariableMod4,labelsAgreementModel)
      .asInstanceOf[Classification]
      .setProbLabel(probLabelAgreementModel)
      .setTransformers()

//    Fitting the above model
    trainedModel = model.train()

//    Appling model to the complete dataset and also to training and test sets
    val output = trainedModel.transform(model.data)
    val outputTraingSet = trainedModel.transform(model.trainingSet)
    val outputTestSet = trainedModel.transform(model.testSet)

//    Create a JSON like string to export the needed information for the automated report made in R.
    val initialJson = "{\"id\": \"" + idModel4 + "\" ," +
      "\"type\": \"Classification\"" + "," +
      "\"method\": \"" + methodMod4 + "\" ,"

//    Adds training and test set to the JSON string. Metrics, presented as comment lines, were also calculated but not
// used, once its better to do it direct in R.
    val finalJson = model.performance(model,outputTraingSet,outputTestSet, initialJson)

//    Gets the time that the code runned and generates a folder with the date in order to save a historic log of
// models and performances.
    val date = Calendar.getInstance().getTime
    val dateFormat = new SimpleDateFormat("YYYYMMddHHmmss")
    val dateFormated = dateFormat.format(date)

    Directory(outputPerformanceDir + dateFormated).createDirectory()

    new PrintWriter(outputPerformanceDir + dateFormated + "/" + idModel4.replace(" ","_") + "_" + methodMod4 + ".json") {
      write(finalJson)
      close()
    }

//    output.repartition(265)
    output.persist()
    output.count()
    output
  }
  override def write: MLWriter = trainedModel.write
}
