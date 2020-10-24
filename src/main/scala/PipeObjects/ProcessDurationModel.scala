package PipeObjects

//Import Project classes
import Models._

//Import Java packages
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar
import reflect.io._

//Import Spark packages
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import com.autoML.VariablesYAML._
//This Class construct and train a model that determines how long a law suit will last. It also applies the fitted
// model transforming a dataset and evaluates its performance.
class ProcessDurationModel(override val uid: String) extends Transformer with MLWritable with
  DefaultParamsReadable[ProcessDurationModel] {

  var trainedModel: PipelineModel = _

  def this() = this(Identifiable.randomUID(idModel2))

  def copy(extra: ParamMap): ProcessDurationModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def transform(df: Dataset[_]): DataFrame = {
//    Model Constructor

    println("####################\n\n####################\nInput model 2\n####################\n####################")
    df.printSchema()

    val model = ModelGenerator.getMethod(methodMod2)
      .setDF(df.toDF())
      .setCategoricalVariables(categoricalVariables)
      .setNumericalVariables(numericalVariables)
      .setIndependentVariable(independentVariableMod2 + inputedSuffix + scaledSuffix)
      .setPredictionColumn(outputMod2)
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
    val initialJson = "{\"id\": \"" + idModel2 + "\" ," +
      "\"type\": \"Regression\"" + "," +
      "\"method\": \"" + methodMod2 + "\" ,"

//    Adds training and test set to the JSON string. Metrics, presented as comment lines, were also calculated but not
// used, once its better to do it direct in R.
    val finalJson = model.performance(model,outputTraingSet,outputTestSet, initialJson)

//    Gets the time that the code runned and generates a folder with the date in order to save a historic log of
// models and performances.
    val date = Calendar.getInstance().getTime
    val dateFormat = new SimpleDateFormat("YYYYMMddHHmmss")
    val dateFormated = dateFormat.format(date)

    Directory(outputPerformanceDir + dateFormated).createDirectory()

    new PrintWriter(outputPerformanceDir + dateFormated + "/" + idModel2.replace(" ","_") + "_" + methodMod2 + "" +
      ".json") {
      write(finalJson)
      close()
    }

//    output.repartition(280)
    output.persist()
    output.count()
    output
  }

  override def write: MLWriter = trainedModel.write

  override def read: MLReader[ProcessDurationModel] = new MyReader[ProcessDurationModel]

}
