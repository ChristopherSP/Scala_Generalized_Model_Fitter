package com.autoML

import java.util.{List => JList, Map => JMap}
import collection.JavaConversions._
//import com.fasterxml.jackson.annotation.JsonProperty
import org.spark_project.guava.base.Preconditions

class YAML(@com.fasterxml.jackson.annotation.JsonProperty("filesPath") _filesPath: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("filesInfo") _filesInfo: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("modelsID") _modelsID: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("methods") _methods: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("hyperparameters") _hyperparameters: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("outputColumnNames") _outputColumnNames: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("sparkIternalColumnNames") _sparkIternalColumnNames: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("suffixes") _suffixes: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("independentVariables") _independentVariables: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("probabilityClass") _probabilityClass: JMap[String, String],
           @com.fasterxml.jackson.annotation.JsonProperty("filterLabels") _filterLabels: JMap[String, JList[String]],
           @com.fasterxml.jackson.annotation.JsonProperty("dependentVariables") _dependentVariables: JMap[String, JList[String]],
           @com.fasterxml.jackson.annotation.JsonProperty("performanceMetrics") _performanceMetrics: JMap[String, JList[String]]) {

  val filesPath: Map[String, String] = Preconditions
    .checkNotNull(_filesPath, "filesPath cannot be null", null)
    .toMap
  val filesInfo: Map[String, String] = Preconditions
    .checkNotNull(_filesInfo, "filesInfo cannot be null", null)
    .toMap
  val modelsID: Map[String, String] = Preconditions
    .checkNotNull(_modelsID, "modelsID cannot be null", null)
    .toMap
  val methods: Map[String, String] = Preconditions
    .checkNotNull(_methods, "methods cannot be null", null)
    .toMap
  val hyperparameters: Map[String, String] = Preconditions
    .checkNotNull(_hyperparameters, "hyperparameters cannot be null", null)
    .toMap
  val outputColumnNames: Map[String, String] = Preconditions
    .checkNotNull(_outputColumnNames, "outputColumnNames cannot be null", null)
    .toMap
  val sparkIternalColumnNames: Map[String, String] = Preconditions
    .checkNotNull(_sparkIternalColumnNames, "sparkIternalColumnNames cannot be null", null)
    .toMap
  val suffixes: Map[String, String] = Preconditions
    .checkNotNull(_suffixes, "suffixes cannot be null", null)
    .toMap
  val independentVariables: Map[String, String] = Preconditions
    .checkNotNull(_independentVariables, "independentVariables cannot be null", null)
    .toMap
  val probabilityClass: Map[String, String] = Preconditions
    .checkNotNull(_probabilityClass, "probabilityClass cannot be null", null)
    .toMap
  val filterLabels: Map[String, List[String]] = Preconditions
    .checkNotNull(_filterLabels, "filterLabels cannot be null", null)
    .toMap
    .map(pair => pair._1 -> pair._2.toList)
  val performanceMetrics: Map[String, List[String]] = Preconditions
    .checkNotNull(_performanceMetrics, "performanceMetrics cannot be null", null)
    .toMap
    .map(pair => pair._1 -> pair._2.toList)
  val dependentVariables: Map[String, List[String]] = Preconditions
    .checkNotNull(_dependentVariables, "dependentVariables cannot be null", null)
    .toMap
    .map(pair => pair._1 -> pair._2.toList)
}
