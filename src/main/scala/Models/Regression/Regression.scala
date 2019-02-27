package Models.Regression

//Import Project classes
import Models.Model
import PipeObjects.{CleanColsModel, GetFirstElementVector, Unscale}

//Import Spark packages
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import com.semantix.aijusProd.VariablesYAML._
// Below model, this abstraction level contains all setters, getters and variables needed to define a regression model
trait Regression extends Model{

  //  Variables and steps to be setted
  var independentScaledCol: String = _
  var independentScaledVecCol: String = _
  var independentVecCol: String = _

  var independentAssembler: VectorAssembler = _
  var independentScaler: StandardScaler = _
  var independentDisassembler: GetFirstElementVector = _
  var keepCols: CleanColsModel = _
  var unscaleCol: Unscale = _

//  Set independent variable related columns
  override def setTransformers(): Model = {
    this.independentVecCol = independentVariable + "Vec"
    this.independentScaledCol = independentVariable + scaledSuffix
    this.independentScaledVecCol = independentVariable + scaledSuffix + "Vec"
    this
  }

//  Vectorize independent variable
  def setindependentAssembler(): Regression = {
    this.independentAssembler = new VectorAssembler()
      .setInputCols(Array(independentVariable))
      .setOutputCol(independentVecCol)
    this
  }
//  Scale independent variable
  def setIndependentScaler(): Regression = {
    this.independentScaler = new StandardScaler()
      .setInputCol(independentVecCol)
      .setOutputCol(independentScaledVecCol)
      .setWithStd(true)
      .setWithMean(true)
    this
  }
//  Unvectorize independent variable
  def setIndependentDisassembler(): Regression = {
    this.independentDisassembler = new GetFirstElementVector()
      .setInputCol(independentVariable + vectorSuffix)
      .setOutputCol(independentVariable)
    this
  }

  //  Sets which column to maintain after apply model and clean the rest
  def setCleanCols(): Regression = {
    this.keepCols = new CleanColsModel()
      .setInputCol(data.columns)
      .setOutputCol(Array(predictCol))
    this
  }

  //  Transform predicted column to original scale
  def setUnscale(): Regression = {
    this.unscaleCol = new Unscale()
      .setInputCol(independentVariable.replaceAll(scaledSuffix,""))
      .setOutputCol(predictCol)
    this
  }

  //  Initializes Pipelines relevant to models
  override def setIndependentPipeline(): Model = {
    setindependentAssembler()
    setIndependentScaler()
    setIndependentDisassembler()
    setCleanCols()
    setUnscale()
    this
  }

  //  Define steps to apply in a classification model
  override def setPipelineSteps(method: Object): Array[PipelineStage] = {
    Array(numericalAssembler, categoricalAssembler, assembler, independentDisassembler, method
      .asInstanceOf[PipelineStage], keepCols, unscaleCol)
  }

  //  Performance evaluation for a regression problem
  override def performance(model: Model, trainingSet: DataFrame, testSet: DataFrame, json: String): String = {
    // This commented block is been made on R to generate the automated report. It calculates some evaluation metrics
    // implemented in spark

    /*    val evaluator = new RegressionEvaluator()
      .setLabelCol(model.getIndependentVariables.replaceAll(scaledSuffix,""))
      .setPredictionCol(model.getPredictionColumn + unscaledSuffix)
      .setMetricName(regressionPerfMetric)


    val rmseTrainingSet = evaluator.evaluate(trainingSet)
    val rmseTestSet = evaluator.evaluate(testSet)

    val correlationTrain = trainingSet.stat.corr(model.getIndependentVariables,model.getPredictionColumn)
    val correlationTest = testSet.stat.corr(model.getIndependentVariables,model.getPredictionColumn)

    val generalization = Math.abs(rmseTrainingSet - rmseTestSet)
    val generalizationProportion = 100 - Math.round(100*generalization)
*/

    //    Selects independent variable and predicted column of the training set and renames then to normalize the
    // header to use it in the automated report. This normalized dataframe is then cast as string and structured as a
    // json
    val predictedTrainingSet = trainingSet.select(model.getIndependentVariables.replaceAll(scaledSuffix,""),model.getPredictionColumn + unscaledSuffix)
      .withColumnRenamed(model.getIndependentVariables.replaceAll(scaledSuffix,""), "target")
      .withColumnRenamed(model.getPredictionColumn + unscaledSuffix, "predicted")
      .toJSON
      .collect()
      .map(_.toString)
      .mkString("[",",","]")

    //    Selects independent variable and predicted column of the test set and renames then to normalize the
    // header to use it in the automated report. This normalized dataframe is then cast as string and structured as a
    // json
    val predictedTestSet = testSet.select(model.getIndependentVariables.replaceAll(scaledSuffix,""),model.getPredictionColumn + unscaledSuffix)
      .withColumnRenamed(model.getIndependentVariables.replaceAll(scaledSuffix,""), "target")
      .withColumnRenamed(model.getPredictionColumn + unscaledSuffix, "predicted")
      .toJSON
      .collect()
      .map(_.toString)
      .mkString("[",",","]")

    val finalJson = json +
/*      "\"rmse\": {" +
        "\"trainingSet\": " + rmseTrainingSet + "," +
        "\"testSet\": " + rmseTestSet +
      "}," +
      "\"correlation\": {" +
        "\"trainingSet\": " + correlationTrain + "," +
        "\"testSet\": " + correlationTest +
      "}," +
      "\"distinctiveness\": " + generalization + "," +
      "\"generalization\": " + generalizationProportion + "," +*/
      "\"trainingSet\": " + predictedTrainingSet + "," +
      "\"testSet\": " + predictedTestSet +
      "}"

    finalJson
  }
}
