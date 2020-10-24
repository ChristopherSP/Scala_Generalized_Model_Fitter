package com.autoML

import java.io.FileReader

object VariablesYAML {
  val yamlPath: String = "/home/hadoop/main/confFiles/parameters.yml"
  val reader = new FileReader(yamlPath)
  val mapper = new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.yaml.YAMLFactory())
  val config: YAML = mapper.readValue(reader, classOf[YAML])

  //Paths
  println("PATH")
//  val inputFile: String = config.filesPath.get("input")
  val inputFile: String = config.filesPath.getOrElse("input", null)
  val outputFile: String = config.filesPath.getOrElse("output", null)
  val outputPerformanceDir: String = config.filesPath.getOrElse("outputPerformance", null)

//  Files Info
  println("File Info")
  val fileType: String = config.filesInfo.getOrElse("type", null)
  val fileHeader: String = config.filesInfo.getOrElse("header", null)
  val fileDelimiter: String = config.filesInfo.getOrElse("delimiter", null)
  val fileQuotationMark: String = config.filesInfo.getOrElse("quote", null)
  val fileSchema: String = config.filesInfo.getOrElse("inferSchema", null)

  //Models ID
  println("Model Id")
  val idModel1: String = config.modelsID.getOrElse("JudgmentPropensity", null)
  val idModel2: String = config.modelsID.getOrElse("ProcessDuration", null)
  val idModel3: String = config.modelsID.getOrElse("Sentence", null)
  val idModel4: String = config.modelsID.getOrElse("AgreementPropensity", null)
  val idModel5: String = config.modelsID.getOrElse("SentenceValue", null)
  val idModel6: String = config.modelsID.getOrElse("AgreementValue", null)

  //Methods
  println("Methods")
  val rfRegression = "RFRegression"
  val rfClassifier = "RFClassifier"
  val gbmClassifier = "GBMClassifier"
  val gbmRegression = "GBMRegression"
  val glm = "GLM"
  val logistic = "Logistic"
  val svm = "SVM"
  val bayes = "Bayes"

  //Used Methods
  println("Use Methods")
  val methodMod1: String = config.methods.getOrElse("JudgmentPropensity", null)
  val methodMod2: String = config.methods.getOrElse("ProcessDuration", null)
  val methodMod3: String = config.methods.getOrElse("Sentence", null)
  val methodMod4: String = config.methods.getOrElse("AgreementPropensity", null)
  val methodMod5: String = config.methods.getOrElse("SentenceValue", null)
  val methodMod6: String = config.methods.getOrElse("AgreementValue", null)

  //Output Names
  println("OutNames")
  val outputMod1: String = config.outputColumnNames.getOrElse("JudgmentPropensity", null)
  val outputMod2: String = config.outputColumnNames.getOrElse("ProcessDuration", null)
  val outputMod3: String = config.outputColumnNames.getOrElse("Sentence", null)
  val outputMod4: String = config.outputColumnNames.getOrElse("AgreementPropensity", null)
  val outputMod5: String = config.outputColumnNames.getOrElse("SentenceValue", null)
  val outputMod6: String = config.outputColumnNames.getOrElse("AgreementValue", null)
  val outputMod7: String = config.outputColumnNames.getOrElse("ThesesAssociation", null)
  val probMod1: String = config.outputColumnNames.getOrElse("ProbabilityJudgment", null)
  val probMod3: String = config.outputColumnNames.getOrElse("ProbabilitySentence", null)
  val probMod4: String = config.outputColumnNames.getOrElse("ProbabilityAgreement", null)

  val featuresName: String = config.sparkIternalColumnNames.getOrElse("featuresName", null)
  val scaledFeaturesName: String = config.sparkIternalColumnNames.getOrElse("scaledFeaturesName", null)
  val categoricalIndexedName: String = config.sparkIternalColumnNames.getOrElse("categoricalIndexedName", null)

  //Suffix to Rename Columns
  println("Sufixes")
  val inputedSuffix: String = config.suffixes.getOrElse("inputedSuffix", null)
  val vectorSuffix: String = config.suffixes.getOrElse("vectorSuffix", null)
  val scaledSuffix: String = config.suffixes.getOrElse("scaledSuffix", null)
  val indexSuffix: String = config.suffixes.getOrElse("indexSuffix", null)
  val unscaledSuffix: String = config.suffixes.getOrElse("unscaledSuffix", null)
  val converterSuffix: String = config.suffixes.getOrElse("converterSuffix", null)
  val featureCol: String = config.suffixes.getOrElse("featureCol", null)
  val dependentVecCol: String = config.suffixes.getOrElse("dependentVecCol", null)
  val dependentScaledVecCol: String = config.suffixes.getOrElse("dependentScaledVecCol", null)
  val categoricalIndexVecCol: String = config.suffixes.getOrElse("categoricalIndexVecCol", null)

  //Independent Variables
  println("Independent Variables")
  val independentVariableMod1: String = config.independentVariables.getOrElse("JudgmentPropensity", null)
  val independentVariableMod2: String = config.independentVariables.getOrElse("ProcessDuration", null)
  val independentVariableMod3: String = config.independentVariables.getOrElse("Sentence", null)
  val independentVariableMod4: String = config.independentVariables.getOrElse("AgreementPropensity", null)
  val independentVariableMod5: String = config.independentVariables.getOrElse("SentenceValue", null)
  val independentVariableMod6: String = config.independentVariables.getOrElse("AgreementValue", null)
  val independentVariableMod7: String = config.independentVariables.getOrElse("ThesesAssociation", null)

  //Labels
  println("Labels")
  val labelsJudgmentModel: List[String] = config.filterLabels.getOrElse("JudgmentPropensity", null)
  val labelsSentenceModel: List[String] = config.filterLabels.getOrElse("Sentence", null)
  val labelsAgreementModel: List[String] = config.filterLabels.getOrElse("AgreementPropensity", null)
  val labelsRegression: List[String] = config.filterLabels.getOrElse("Regression", null)
  val probLabelJudgmentModel: String = config.probabilityClass.getOrElse("JudgmentPropensity", null)
  val probLabelSentenceModel: String = config.probabilityClass.getOrElse("Sentence", null)
  val probLabelAgreementModel: String = config.probabilityClass.getOrElse("AgreementPropensity", null)

  //Dependent variables
  println("Dependent")
  val numericalOriginal: List[String] = config.dependentVariables.getOrElse("numerical", null)
  val categoricalOriginal: List[String] = config.dependentVariables.getOrElse("categorical", null)

  val numericalVariables: List[String] = numericalOriginal.map(variable => variable + inputedSuffix + scaledSuffix +
    vectorSuffix)
  val categoricalVariables: List[String] = categoricalOriginal.map(variable => variable + inputedSuffix + indexSuffix)

  //Performance Metrics
  println("Metrics")
  //Regression
  val regressionPerfMetric: List[String] = config.performanceMetrics.getOrElse("regression", null)

  //Classification
  val multiClassMeasures: List[String] = config.performanceMetrics.getOrElse("multiClassification", null)
  val binaryClassMeasures: List[String] = config.performanceMetrics.getOrElse("binaryClassification", null)

}
