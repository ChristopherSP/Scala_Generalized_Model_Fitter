package Models.Classification

//Import Project classes
import Models.Model
import PipeObjects.{CleanColsModel, GetProb, LabelConverter}
import aijusProd.Variables._

//Import Spark packages
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame

// Below model, this abstraction level contains all setters, getters and variables needed to define a classification
// model
trait Classification extends Model {

//  Variables and steps to be setted
  var labelIndexerCol: String = _
  var probLabel: String = _

  var labelIndexer: StringIndexer = _
  var labelConverter: LabelConverter = _
  var keepCols: CleanColsModel = _
  var getClassProb: GetProb = _

  override def setTransformers(): Model = {
    this.labelIndexerCol = independentVariable
    this
  }

  def setProbLabel(label: String): Classification = {
    this.probLabel = label
    this
  }
  /*def setLabelIndexer(): Classification = {
    this.labelIndexer = new StringIndexer()
      .setInputCol(independentVariable)
      .setOutputCol(labelIndexerCol)
    this
  }*/
//  Convert predicted numerical index to a interpretable string label
  def setLabelConverter(): Classification = {
    this.labelConverter = new LabelConverter()
      .setInputCols(independentVariable.replaceAll(indexSuffix,""),independentVariable, predictCol)
      .setOutputColumn(outputCol + converterSuffix)
    this
  }
//  Extracts the desired label probability from the probability vector by finding its index
  def setGetProb(): Classification = {
    this.getClassProb = new GetProb()
      .setInputCol(probabilityVecCol)
      .setProbLabel(probLabel)
      .setOutputCol(probabilityCol)
    this
  }
//  Sets which column to maintain after apply model and clean the rest
  def setCleanCols(): Classification = {
    this.keepCols = new CleanColsModel()
      .setInputCol(data.columns)
      .setOutputCol(Array(outputCol + converterSuffix, probabilityCol))
    this
  }
//  Initializes Pipelines relevant to models
  override def setIndependentPipeline(): Model = {
    setLabelConverter()
    setGetProb()
    setCleanCols()
    this
  }
//  Define steps to apply in a classification model
  override def setPipelineSteps(method: Object): Array[PipelineStage] = {
    Array(numericalAssembler, categoricalAssembler, assembler, method.asInstanceOf[PipelineStage], labelConverter,
      getClassProb, keepCols)
  }

//  Performance evaluation for a classification problem
  override def performance(model: Model, trainingSet: DataFrame, testSet: DataFrame, json: String): String = {
    // This commented block is been made on R to generate the automated report. It calculates some evaluation metrics
    // implemented in spark

    /*    val sc: SparkContext = SparkContext.getOrCreate()
    val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    val multiClassValues = multiClassMeasures.map(measure =>
      (measure, new MulticlassClassificationEvaluator()
        .setLabelCol(model.getIndependentVariables+indexSuffix)
        .setPredictionCol(predictCol)
        .setMetricName(measure)
        .evaluate(trainingSet)
      )
    ).toDF("name","value")

    val error = 1 - multiClassValues.filter($"name" === "accuracy").select("value").first().getDouble(0)

    val binaryClassValues = binaryClassMeasures.map(measure =>
      (measure, new BinaryClassificationEvaluator()
        .setLabelCol(model.getIndependentVariables+indexSuffix)
        .setRawPredictionCol(predictCol)
        .setMetricName(measure)
        .evaluate(trainingSet)
      )
    ).toDF("name","value")

    val metrics = multiClassValues.union(binaryClassValues)
      .union(List(("error", error)).toDF("name","value"))

    metrics.show()

    val trainingSetCM = trainingSet.stat.crosstab(model.getIndependentVariables, outputCol)
      .orderBy(model.getIndependentVariables + outputCol)

    val testSetCM = testSet.stat.crosstab(model.getIndependentVariables, outputCol)
      .orderBy(model.getIndependentVariables + outputCol)

    trainingSetCM.show()


    import spark.implicits._
*/

    //    Selects independent variable, predicted and probability column of the training set and renames then to
    // normalize the header to use it in the automated report. This normalized dataframe is then cast as string
    // and structured as a json
    val predictedTrainingSet = trainingSet.select(model.getIndependentVariables.replaceAll(indexSuffix,""), outputCol +
      converterSuffix, probabilityCol)
      .withColumnRenamed(model.getIndependentVariables.replaceAll(indexSuffix,""), "target")
      .withColumnRenamed(outputCol + converterSuffix, "predicted")
      .withColumnRenamed(probabilityCol, "probability")
      .toJSON
      .collect()
      .map(_.toString)
      .mkString("[",",","]")

    //    Selects independent variable, predicted and probability column of the test set and renames then to
    // normalize the header to use it in the automated report. This normalized dataframe is then cast as string
    // and structured as a json
    val predictedTestSet = testSet.select(model.getIndependentVariables.replaceAll(indexSuffix,""), outputCol +
      converterSuffix, probabilityCol)
      .withColumnRenamed(model.getIndependentVariables.replaceAll(indexSuffix,""), "target")
      .withColumnRenamed(outputCol + converterSuffix, "predicted")
      .withColumnRenamed(probabilityCol, "probability")
      .toJSON
      .collect()
      .map(_.toString)
      .mkString("[",",","]")

    val finalJson = json +
      "\"trainingSet\": " + predictedTrainingSet + "," +
      "\"testSet\": " + predictedTestSet +
      "}"

    finalJson
  }


}
