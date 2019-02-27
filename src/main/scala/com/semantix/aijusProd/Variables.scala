package com.semantix.aijusProd

object Variables {
  //Paths
  val inputFile = "/home/christopher/Downloads/iris.csv"
  val outputPerformanceDir = "/home/christopher/Downloads/Performance/"

  //Models ID
  val idModel2 = "Process Duration Model"
  val idModel3 = "Sentence Model"
  val idModel4 = "Agreement Propensity Model"
  val idModel5 = "Sentence Value Model"

  //Methods
  val rfRegression = "RFRegression"
  val rfClassifier = "RFClassifier"
  val gbmClassifier = "GBMClassifier"
  val gbmRegression = "GBMRegression"
  val glm = "GLM"
  val logistic = "Logistic"
  val svm = "SVM"
  val bayes = "Bayes"

  //Used Methods
  val methodMod2: String = rfRegression
  val methodMod3: String = rfClassifier
  val methodMod4: String = rfClassifier
  val methodMod5: String = rfRegression

  //Output Names
  val outputMod2 = "outputMod2"
  val outputMod3 = "label_mod3"
  val outputMod4 = "label_mod4"
  val outputMod5 = "outputMod5"

  val probMod3 = "prob_mod3"
  val probMod4 = "prob_mod4"

  val featuresName = "features"
  val scaledFeaturesName = "scaledFeatures"
  val categoricalIndexedName = "categoryIndex"

  //Suffix to Rename Columns
  val inputedSuffix = "_inputed"
  val vectorSuffix = "_vec"
  val scaledSuffix = "_scaled"
  val indexSuffix = "_index"
  val unscaledSuffix = "_unscaled"
  val converterSuffix = "_converted"
  val featureCol = "features"
  val dependentVecCol = "dependentVec"
  val dependentScaledVecCol = "dependentScaledVec"
  val categoricalIndexVecCol = "categoricalIndexVec"

  //Independent Variables
  val independentVariableMod2 = "SepalLength"
  val independentVariableMod3 = "Species"
  val independentVariableMod4 = "Species"
  val independentVariableMod5 = "PetalLength"

  //Labels
  val labelsSentenceModel = List("setosa", "virginica")
  val labelsAgreementModel = List("setosa", "versicolor")
  val labelsRegression = List("setosa", "versicolor","virginica")
  val probLabelSentenceModel = "setosa"
  val probLabelAgreementModel = "setosa"

  //Dependent variables
  val numericalOriginal = List("SepalLength", "SepalWidth", "PetalLength", "PetalWidth")
  val categoricalOriginal = List("Categorica")

  val numericalVariables: List[String] = numericalOriginal.map(variable => variable + inputedSuffix + scaledSuffix +
    vectorSuffix)
  val categoricalVariables: List[String] = categoricalOriginal.map(variable => variable + inputedSuffix + indexSuffix)

  //Performance Metrics
  //Regression
  val regressionPerfMetric = "rmse"

  //Classification
  val multiClassMeasures = List("f1", "weightedPrecision", "weightedRecall", "accuracy")
  val binaryClassMeasures = List("areaUnderROC", "areaUnderPR")
}