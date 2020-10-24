package Models

//Import Project classes

//Import Spark packages
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.autoML.VariablesYAML._
// The most general level of abstraction for models. It contains all setters, getters and variables needed to apply a
// model
trait Model {

  //Input data
  var data: DataFrame = _
  var trainingSet: DataFrame = _
  var testSet: DataFrame = _

  //Variables for models
  var numericalVariables: List[String] = _
  var categoricalVariables: List[String] = _
  var dependentVariables: List[String] = _
  var independentVariable: String = _
  //Hyperparameters
  var max_depth: Int = config.hyperparameters.getOrElse("maxDepth", null).toInt
  var mtry: Int = config.hyperparameters.getOrElse("mtry", null).toInt
  var ntrees: Int = config.hyperparameters.getOrElse("ntrees", null).toInt
  var maxBins: Int = config.hyperparameters.getOrElse("maxBins", null).toInt
  var maxIter: Int = config.hyperparameters.getOrElse("maxIter", null).toInt
  var minInfoGain: Double = config.hyperparameters.getOrElse("minInfoGain", null).toDouble
  var learn_rate: Double = config.hyperparameters.getOrElse("learnRate", null).toDouble
  var kernel: String = config.hyperparameters.getOrElse("kernel", null)
  var family: String = config.hyperparameters.getOrElse("family", null)
  var link: String = config.hyperparameters.getOrElse("link", null)

  var rawPredictionCol: String = "Votes"
  var probabilityVecCol: String = "probabilityVec"
  var probabilityCol: String = "probability"
  //Column names
  var outputCol = "output"
  var predictCol = "predictedOutput"

  //Pipeline
  var categoricalAssembler: PipelineStage  = _
  var numericalAssembler: PipelineStage = _
  var dependentScaler: PipelineStage = _
  var categoricalIndexer: Array[PipelineStage] = _
  var assembler: PipelineStage = _

  //Set global ratio to slip data and seed for reproducibility
  val trainRatio: Double = 0.8
  val seed: Long = 112358

  //Setters
  def setDF(df: DataFrame): Model = {
    this.data = df
    this
  }
  /*def setClient(Client: String): Model = {
    this.client = Client
    this
  }*/
  def setNumericalVariables(numVar: List[String]): Model = {
    this.numericalVariables = numVar
    this
  }
  def setCategoricalVariables(catVar: List[String]): Model = {
    this.categoricalVariables = catVar
    this
  }
  def setIndependentVariable(yVar: String): Model = {
    this.independentVariable = yVar
    this
  }
  /*def setRawPredCol(columnName: String): Model = {
    this.rawPredictionCol = columnName
    this
  }*/
  /*def setProbVecCol(columnName: String): Model = {
    this.probabilityVecCol = columnName
    this
  }*/
  /*def split(): (DataFrame, DataFrame) = {
    val Array(trainingSet, testSet) = this.data.randomSplit(Array[Double](trainRatio, 1 - trainRatio), seed = seed)
    (trainingSet.toDF, testSet.toDF)
  }*/
//  Set training and test set
  def setSets(labelCol: String, labels: List[String]): Model = {
    val Array(trainingSet, testSet) = this.data.filter(
      col(labelCol).isin(labels:_*)
    ).randomSplit(
      Array[Double](trainRatio, 1 - trainRatio),
      seed = seed)

    this.trainingSet = trainingSet.toDF
    this.testSet = testSet.toDF
    this
  }
  def setNumericalVec(): Model = {
    this.numericalAssembler = new VectorAssembler()
      .setInputCols(this.numericalVariables.toArray)
      .setOutputCol(dependentVecCol)
    this
  }
  def setScaler(): Model = {
    this.dependentScaler = new StandardScaler()
      .setInputCol(dependentVecCol)
      .setOutputCol(dependentScaledVecCol)
      .setWithStd(true)
      .setWithMean(true)
    /*this.dependentScaler = this.numericalVariables.toArray.map(
      column => new StandardScaler()
        .setInputCol(column)
        .setOutputCol(s"${column + scaleSuffix}")
        .setWithStd(true)
        .setWithMean(true)
    )*/
    this
  }
  def setIndexer(): Model = {
    /*this.categoricalIndexer = new StringIndexer()
      .setInputCol("categoricalVec")
      .setOutputCol("categoricalIndexVec")*/
    this.categoricalIndexer = this.categoricalVariables.toArray.map(
      column => new StringIndexer()
        .setInputCol(column)
        .setOutputCol(s"${column + indexSuffix}")
    )
    this
  }
  def setCategoricalVec(): Model = {
//    val categoricalIndexed = categoricalVariables.map(variable => variable + indexSuffix)
    val categoricalIndexed = categoricalVariables
    this.categoricalAssembler = new VectorAssembler()
      .setInputCols(categoricalIndexed.toArray)
      .setOutputCol(categoricalIndexVecCol)
    this
  }
  def setAssembler(): Model = {
    this.assembler = new VectorAssembler()
//      .setInputCols(Array(dependentScaledVecCol,categoricalIndexVecCol))
      .setInputCols(Array(dependentVecCol,categoricalIndexVecCol))
      .setOutputCol(featureCol)
    this
  }
//  Instanciates variables that doesn't need parameters
  def setConstants(): Model = {

    setNumericalVec()
    setScaler()
    setIndexer()
    setCategoricalVec()
    setAssembler()

    this.dependentVariables = numericalVariables ++ categoricalVariables
    this.max_depth = this.dependentVariables.length
    this.mtry = Math.floor(Math.sqrt(this.dependentVariables.length)).toInt

    this
  }
  def setNTree(numTree: Int): Model ={
    this.ntrees = numTree
    this
  }
  def setMaxBins(MaxBins: Int): Model ={
    this.maxBins = MaxBins
    this
  }
  def setMaxIter(MaxIter: Int): Model ={
    this.maxIter = MaxIter
    this
  }
  def setMinInfoGain(MinInfoGain: Double): Model ={
    this.minInfoGain = MinInfoGain
    this
  }
  def setLearnRate(learnRate: Double): Model ={
    this.learn_rate = learnRate
    this
  }
  def setKernel(Kernel: String): Model ={
    this.kernel = Kernel
    this
  }
  def setFamily(Family: String): Model ={
    this.family = Family
    this
  }
  def setLink(Link: String): Model ={
    this.link = Link
    this
  }
  def setOutputColumn(col: String): Model ={
    this.outputCol = col
    this
  }
  def setPredictionColumn(col: String): Model ={
    this.predictCol = col
    this
  }
  def setProbabilityColumn(col: String): Model ={
    this.probabilityCol = col
    this
  }
//  Instanciates steps relevant to model that doesn't need parameters
  def setTransformers(): Model
  def setIndependentPipeline(): Model
  def setPipelineSteps(method: Object): Array[PipelineStage]
//  Initializes Pipelines relevant to models
  def initialPipeline(method: Object): PipelineModel = {

    setIndependentPipeline()

    val steps = setPipelineSteps(method)
    val pipeline: Pipeline = new Pipeline().setStages(steps)

    pipeline.fit(trainingSet)
  }

  //Getters
  def getDF: DataFrame = {
    this.data
  }
  /*def getClient: String = {
    this.client
  }*/
  def getDependentVariables: List[String] = {
    this.dependentVariables
  }
  def getIndependentVariables: String = {
    this.independentVariable
  }
  def getPredictionColumn: String = {
    this.predictCol
  }
  def getOutputColumn: String = {
    this.outputCol
  }
  def getHiperparameters: (Int, Int, Int, Int, Int, Double, Double, String, String, String) = {
    (this.ntrees, this.max_depth, this.mtry, this.maxBins, this.maxIter, this.minInfoGain, this.learn_rate,
      this.kernel, this.family, this.link)
  }

  //Train method
  def train(): PipelineModel

  //Performance evaluation
  def performance(model:Model ,trainingSet: DataFrame, testSet: DataFrame, json: String): String
}
